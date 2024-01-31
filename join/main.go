package main

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

func main() {
	f1, err := os.Open("/Users/michaeldehoog/Downloads/fastlz.bin.incomplete2")
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	f2, err := os.Open("/Users/michaeldehoog/Downloads/fastlz.bin.incomplete3")
	if err != nil {
		panic(err)
	}
	defer f2.Close()
	out, err := os.Create("/Users/michaeldehoog/Downloads/fastlz.bin")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	record := make([]byte, 4*5)
	pivot := uint32(2590000)

	for {
		_, err := io.ReadFull(f1, record)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		block := binary.LittleEndian.Uint32(record)
		if block <= pivot {
			break
		}
		_, err = out.Write(record)
		if err != nil {
			panic(err)
		}
	}

	for {
		_, err := io.ReadFull(f2, record)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		block := binary.LittleEndian.Uint32(record)
		if block > pivot {
			continue
		}
		_, err = out.Write(record)
		if err != nil {
			panic(err)
		}
	}
}
