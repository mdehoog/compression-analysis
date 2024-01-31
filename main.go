package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
)

func main() {
	trimSignature := true
	bootstrapTxs := 1000
	endBlock := uint64(105235064) // OP bedrock block + 1
	//endBlock := uint64(0)
	startBlock := int64(-1) // -1 for latest

	// remote node URL or local database location:
	// clientLocation := "https://mainnet.base.org"
	// clientLocation := "https://base-mainnet-dev.cbhq.net:8545"
	clientLocation := "/data/op-geth"
	output, err := os.Create("/data/fastlz.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()

	var client Client
	if strings.HasPrefix(clientLocation, "http://") || strings.HasPrefix(clientLocation, "https://") {
		client, err = ethclient.Dial(clientLocation)
	} else {
		client, err = NewLocalClient(clientLocation)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	zlibBestBatchEstimator := newZlibBatchEstimator()

	printInterval := 10 * time.Second
	lastPrint := time.Now().Add(-printInterval)

	count := 0
	var nextBlockHash *common.Hash
	for {
		var block *types.Block
		if nextBlockHash == nil {
			block, err = client.BlockByNumber(context.Background(), big.NewInt(startBlock))
		} else {
			block, err = client.BlockByHash(context.Background(), *nextBlockHash)
		}
		if err != nil {
			log.Fatal(err)
		}
		if block == nil || block.NumberU64() <= endBlock {
			break
		}
		p := block.ParentHash()
		nextBlockHash = &p

		if time.Since(lastPrint) > printInterval {
			lastPrint = time.Now()
			fmt.Println("Block:", block.NumberU64())
		}

		for _, tx := range block.Transactions() {
			if tx.Type() == types.DepositTxType {
				continue
			}
			b, err := tx.MarshalBinary()
			if err != nil {
				log.Fatal(err)
			}
			if trimSignature {
				// for span batch mode we trim the signature, and assume there is no estimation
				// error on this component were we to just treat it as entirely incompressible.
				b = b[:len(b)-68.]
			}
			count++
			if count <= bootstrapTxs {
				zlibBestBatchEstimator.write(b)
				continue
			}

			best := zlibBestBatchEstimator.write(b)
			fastlz := FlzCompressLen(b)
			zeroes := uint32(0)
			nonZeroes := uint32(0)
			for _, b := range b {
				if b == 0 {
					zeroes++
				} else {
					nonZeroes++
				}
			}

			if best == 0 {
				// invalid datapoint, ignore
				continue
			}

			// block numbers fit in 32-bits (for now)
			err = binary.Write(output, binary.LittleEndian, uint32(block.NumberU64()))
			if err != nil {
				log.Fatal(err)
			}
			err = binary.Write(output, binary.LittleEndian, best)
			if err != nil {
				log.Fatal(err)
			}
			err = binary.Write(output, binary.LittleEndian, fastlz)
			if err != nil {
				log.Fatal(err)
			}
			err = binary.Write(output, binary.LittleEndian, zeroes)
			if err != nil {
				log.Fatal(err)
			}
			err = binary.Write(output, binary.LittleEndian, nonZeroes)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
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

func (w *zlibBatchEstimator) write(p []byte) uint32 {
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
	if after-before-2 < 0 {
		return 0
	}
	return uint32(after - before - 2) // flush writes 2 extra "sync" bytes so don't count those
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
	handles := utils.MakeDatabaseHandles(1024)
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

func FlzCompressLen(ib []byte) uint32 {
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
	if len(ib) < 13 {
		ipLimit = 0
	}
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
