package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

type VWAP struct {
	TotalPriceVolume float64
	TotalVolume      uint64
}

func main() {
	filePath := "hft_marketdata.bin"
	capacity := uint64(100 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.DefaultPayloadSize)

	log.Printf("Starting Market Data Algo...")

	mapped, file, err := shm.OpenOrCreateMmap(filePath, size)
	if err != nil {
		log.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	rb := ringbuf.Init(mapped, capacity, ringbuf.DefaultPayloadSize)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	payload := make([]byte, ringbuf.DefaultPayloadSize)

	vwapMap := make(map[string]*VWAP)

	log.Println("Algo started. Waiting for ticks...")

	count := 0
	start := time.Now()

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			if rb.Pop(mapped, payload) {
				// Unpack data: [8 bytes Symbol] [8 bytes Price] [4 bytes Volume]
				symBytes := payload[0:8]
				// Trim null bytes or spaces
				sym := string(bytes.TrimRight(symBytes, " \x00"))

				price := math.Float64frombits(binary.LittleEndian.Uint64(payload[8:16]))
				volume := binary.LittleEndian.Uint32(payload[16:20])

				if vwapMap[sym] == nil {
					vwapMap[sym] = &VWAP{}
				}
				v := vwapMap[sym]
				v.TotalPriceVolume += price * float64(volume)
				v.TotalVolume += uint64(volume)

				count++

				if count%10000 == 0 {
					log.Printf("Processed %d ticks. Current %s VWAP: %.2f",
						count, sym, v.TotalPriceVolume/float64(v.TotalVolume))
				}
			} else {
				runtime.Gosched() // spin wait
			}
		}
	}

	log.Printf("Algo stopped. Processed %d ticks in %v", count, time.Since(start))
}
