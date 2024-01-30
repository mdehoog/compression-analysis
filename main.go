package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"reflect"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
)

type estimator func([]byte) float64

func main() {
	blockNum := big.NewInt(-1) // starting block; will iterate backwards from here
	txsToFetch := 1000000      // min # of transactions to include in our sample (negative means all txs)
	minTxSize := 0             // minimum transaction size to include in our sample whether to

	// spanBatchMode will remove signatures from the tx rlp before compression. This simulates the
	// behavior of span batches which segregates the signatures from the more compressible parts of
	// the tx during batch compression.
	spanBatchMode := true

	// remote node URL or local database location:
	// clientLocation := "https://mainnet.base.org"
	// clientLocation := "https://base-mainnet-dev.cbhq.net:8545"
	clientLocation := "/data"

	bootstrapTxs := 100 // min number of txs to use to bootstrap the batch compressor

	fmt.Printf("Starting block: %v, min tx sample size: %v, min tx size: %v, span batch mode: %v\n",
		blockNum, txsToFetch, minTxSize, spanBatchMode)

	var client Client
	var err error
	if strings.HasPrefix(clientLocation, "http://") || strings.HasPrefix(clientLocation, "https://") {
		client, err = ethclient.Dial(clientLocation)
	} else {
		client, err = NewLocalClient(clientLocation)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	offsets := []float64{-15, -17.5, -20, -22.5, -25}

	zlibBestBatchEstimator := newZlibBatchEstimator()
	estimators := []estimator{
		zlibBestBatchEstimator.write, // first estimator value is always used as the "ground truth" against which others are measured
		uncompressedSizeEstimator,
		cheap0Estimator,
		cheap1Estimator,
		cheap2Estimator,
		cheap3Estimator,
		cheap4Estimator,
		cheap5Estimator,
		cheap6Estimator,
		cheap7Estimator,
		cheap8Estimator,
		repeatedByte0Estimator,
		repeatedByte1Estimator,
		repeatedByte3Estimator,
		repeatedOrZeroEstimator,
		cheap2Estimator,
		cheap3Estimator,
		cheap4Estimator,
		zlibBestEstimator,
		fastLZEstimator,
		regressionEstimator,
	}
	for _, offset := range offsets {
		estimators = append(estimators, fastLZEstimatorWithOffset(offset))
	}
	totals := make([]float64, len(estimators))
	scalars := make([]float64, len(estimators))
	diffs := make([]float64, len(estimators))
	diffsSq := make([]float64, len(estimators))
	count := 0

	printInterval := 10 * time.Second
	lastPrint := time.Now().Add(-printInterval)

	for i := 0; i < 2; i++ {
		var nextBlockHash *common.Hash
		count = 0
		zlibBestBatchEstimator.reset()
		if i == 0 {
			fmt.Println("Calculating scalars...")
		} else {
			fmt.Println("Calculating errors...")
		}

		for {
			var block *types.Block
			if nextBlockHash == nil {
				block, err = client.BlockByNumber(context.Background(), blockNum)
			} else {
				block, err = client.BlockByHash(context.Background(), *nextBlockHash)
			}
			if err != nil {
				log.Fatal(err)
			}
			if block == nil {
				break
			}
			p := block.ParentHash()
			nextBlockHash = &p

			if time.Since(lastPrint) > printInterval {
				lastPrint = time.Now()
				fmt.Println("Blocknum:", block.NumberU64())
			}

			for _, tx := range block.Transactions() {
				if tx.Type() == types.DepositTxType {
					continue
				}
				b, err := tx.MarshalBinary()
				if err != nil {
					log.Fatal(err)
				}
				if len(b) < minTxSize {
					continue
				}
				if spanBatchMode {
					// for span batch mode we trim the signature, and assume there is no estimation
					// error on this component were we to just treat it as entirely incompressible.
					b = b[:len(b)-68.]
				}
				count++
				if count <= bootstrapTxs {
					zlibBestBatchEstimator.write(b)
					continue
				}
				var first float64
				for j, e := range estimators {
					estimate := e(b)
					if i == 0 {
						totals[j] += estimate
					} else {
						if j == 0 {
							first = estimate
						}
						diff := estimate*scalars[j] - first
						diffs[j] += math.Abs(diff)
						diffsSq[j] += diff * diff
					}
				}
				count++
			}
			if txsToFetch > 0 && count > txsToFetch {
				break
			}
		}

		for j := range estimators {
			scalars[j] = totals[0] / totals[j]
		}
	}

	prettyPrintStats("scalars", estimators, scalars)

	// compute mean error metrics
	var mass []float64
	for j := range estimators {
		mass = append(mass, diffs[j]/float64(count))
	}
	fmt.Println()
	prettyPrintStats("mean-absolute-error", estimators, mass)

	var rmses []float64
	for j := range estimators {
		rmses = append(rmses, math.Sqrt(diffsSq[j]/float64(count)))
	}
	fmt.Println()
	prettyPrintStats("root-mean-sq-error", estimators, rmses)
}

func prettyPrintStats(prefix string, estimators []estimator, stats []float64) {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)

	for j := range estimators {
		fmt.Fprintf(w, "%v\t%v\t%v\n", prefix, getFuncName(estimators[j]), stats[j])
	}
	w.Flush()
}

func getFuncName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()[5:] // trim off "main." prefix
}

// only count bytes that are non-zero and non-repeated
func repeatedOrZeroEstimator(tx []byte) float64 {
	lastByte := byte(0)
	count := 0
	for _, b := range tx {
		if b != lastByte && b != 0 {
			count += 1
		}
		lastByte = b
	}
	return float64(count)
}

func repeatedByteEstimator(tx []byte, repeatedByteCost, changedByteCost int) float64 {
	lastByte := byte(0)
	count := 0
	for _, b := range tx {
		if b == lastByte {
			count += repeatedByteCost
		} else {
			count += changedByteCost
		}
		lastByte = b
	}
	return float64(count) / float64(repeatedByteCost+changedByteCost)
}

func repeatedByte0Estimator(tx []byte) float64 {
	return repeatedByteEstimator(tx, 0, 16)
}

func repeatedByte1Estimator(tx []byte) float64 {
	return repeatedByteEstimator(tx, 1, 16)
}

func repeatedByte2Estimator(tx []byte) float64 {
	return repeatedByteEstimator(tx, 2, 16)
}

func repeatedByte3Estimator(tx []byte) float64 {
	return repeatedByteEstimator(tx, 3, 16)
}

func repeatedByte4Estimator(tx []byte) float64 {
	return repeatedByteEstimator(tx, 4, 16)
}

func cheapEstimator(tx []byte, zeroByteCost int, nonZeroByteCost int) float64 {
	count := 0
	for _, b := range tx {
		if b == 0 {
			count += zeroByteCost
		} else {
			count += nonZeroByteCost
		}
	}
	return float64(count) / float64(zeroByteCost+nonZeroByteCost)
}

func zlibBestEstimator(tx []byte) float64 {
	var b bytes.Buffer
	w, err := zlib.NewWriterLevel(&b, zlib.BestCompression)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	w.Write(tx)
	w.Flush()                   // flush instead of close to not include the digest
	return float64(b.Len() - 2) // flush writes 2 extra "sync" bytes so don't count those
}

func fastLZEstimator(tx []byte) float64 {
	return float64(flzCompressLen(tx))
}

func fastLZEstimatorWithOffset(offset float64) func(tx []byte) float64 {
	return func(tx []byte) float64 {
		return float64(flzCompressLen(tx)) + offset
	}
}

func regressionEstimator(tx []byte) float64 {
	flz := float64(flzCompressLen(tx))
	l := float64(len(tx))
	return -35.1823 + flz*1.0694 - l*0.1047
}

func uncompressedSizeEstimator(tx []byte) float64 {
	return float64(len(tx))
}

func cheap0Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 0, 16)
}

// simulate if we could a 0.5 score for zero bytes
func cheapP5Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 1, 32)
}

func cheap1Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 1, 16)
}

func cheap2Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 2, 16)
}

func cheap3Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 3, 16)
}

// cheap4Estimator is the actual L1 Data Cost estimator currently in use
func cheap4Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 4, 16)
}

func cheap5Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 5, 16)
}

func cheap6Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 6, 16)
}

func cheap7Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 7, 16)
}

func cheap8Estimator(tx []byte) float64 {
	return cheapEstimator(tx, 8, 16)
}

// zlibBatchEstimator simulates a zlib compressor at max compression that works on (large) tx
// batches.  Should bootstrap it before use by calling it on several samples of representative
// data.
type zlibBatchEstimator struct {
	b [2]bytes.Buffer
	w [2]*zlib.Writer
}

func newZlibBatchEstimator() *zlibBatchEstimator {
	b := &zlibBatchEstimator{}
	var err error
	for i := range b.w {
		b.w[i], err = zlib.NewWriterLevel(&b.b[i], zlib.BestCompression)
		if err != nil {
			log.Fatal(err)
		}
	}
	return b
}

func (w *zlibBatchEstimator) reset() {
	for i := range w.w {
		w.b[i].Reset()
		w.w[i].Reset(&w.b[i])
	}
}

func (w *zlibBatchEstimator) write(p []byte) float64 {
	// targeting:
	//	b[0] == 0-64kb
	//	b[1] == 64kb-128kb
	before := w.b[1].Len()
	_, err := w.w[1].Write(p)
	if err != nil {
		log.Fatal(err)
	}
	err = w.w[1].Flush()
	if err != nil {
		log.Fatal(err)
	}
	after := w.b[1].Len()
	// if b[1] > 64kb, write to b[0]
	if w.b[1].Len() > 64*1024 {
		_, err = w.w[0].Write(p)
		if err != nil {
			log.Fatal(err)
		}
		err = w.w[0].Flush()
		if err != nil {
			log.Fatal(err)
		}
	}
	// if b[1] > 128kb, rotate
	if w.b[1].Len() > 128*1024 {
		w.b[1].Reset()
		w.w[1].Reset(&w.b[1])
		tb := w.b[1]
		tw := w.w[1]
		w.b[1] = w.b[0]
		w.w[1] = w.w[0]
		w.b[0] = tb
		w.w[0] = tw
	}
	return float64(after - before - 2) // flush writes 2 extra "sync" bytes so don't count those
}

type Client interface {
	BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	Close()
}

type LocalClient struct {
	n  *node.Node
	db ethdb.Database
}

func NewLocalClient(dataDir string) (Client, error) {
	nodeCfg := node.DefaultConfig
	nodeCfg.Name = "geth"
	nodeCfg.DataDir = dataDir
	n, err := node.New(&nodeCfg)
	if err != nil {
		return nil, err
	}
	handles := utils.MakeDatabaseHandles(0)
	db, err := n.OpenDatabaseWithFreezer("chaindata", 512, handles, "", "", true)
	if err != nil {
		return nil, err
	}
	return &LocalClient{
		n:  n,
		db: db,
	}, nil
}

func (c *LocalClient) Close() {
	_ = c.db.Close()
	_ = c.n.Close()
}

func (c *LocalClient) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	number := rawdb.ReadHeaderNumber(c.db, hash)
	if number == nil {
		return nil, nil
	}
	return rawdb.ReadBlock(c.db, hash, *number), nil
}

func (c *LocalClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	if number.Int64() < 0 {
		return c.BlockByHash(ctx, rawdb.ReadHeadBlockHash(c.db))
	}
	hash := rawdb.ReadCanonicalHash(c.db, number.Uint64())
	if bytes.Equal(hash.Bytes(), common.Hash{}.Bytes()) {
		return nil, nil
	}
	return rawdb.ReadBlock(c.db, hash, number.Uint64()), nil
}

func flzCompressLen(ib []byte) uint32 {
	n := uint32(0)
	ht := make([]uint32, 8192)
	u24 := func(i uint32) uint32 {
		return uint32(ib[i]) | (uint32(ib[i+1]) << 8) | (uint32(ib[i+2]) << 16)
	}
	cmp := func(p uint32, q uint32, e uint32) uint32 {
		l := uint32(0)
		for e -= q; l < e; l++ {
			if ib[p+l] != ib[q+l] {
				e = 0
			}
		}
		return l
	}
	literals := func(r uint32) {
		n += 0x21 * (r / 0x20)
		r %= 0x20
		if r != 0 {
			n += r + 1
		}
	}
	match := func(l uint32) {
		l--
		n += 3 * (l / 262)
		if l%262 >= 6 {
			n += 3
		} else {
			n += 2
		}
	}
	hash := func(v uint32) uint32 {
		return ((2654435769 * v) >> 19) & 0x1fff
	}
	setNextHash := func(ip uint32) uint32 {
		ht[hash(u24(ip))] = ip
		return ip + 1
	}
	a := uint32(0)
	ipLimit := uint32(len(ib)) - 13
	for ip := a + 2; ip < ipLimit; {
		r := uint32(0)
		d := uint32(0)
		for {
			s := u24(ip)
			h := hash(s)
			r = ht[h]
			ht[h] = ip
			d = ip - r
			if ip >= ipLimit {
				break
			}
			ip++
			if d <= 0x1fff && s == u24(r) {
				break
			}
		}
		if ip >= ipLimit {
			break
		}
		ip--
		if ip > a {
			literals(ip - a)
		}
		l := cmp(r+3, ip+3, ipLimit+9)
		match(l)
		ip = setNextHash(setNextHash(ip + l))
		a = ip
	}
	literals(uint32(len(ib)) - a)
	return n
}
