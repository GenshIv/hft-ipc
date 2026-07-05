package main

import (
	"encoding/binary"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
	filePath := "hft_marketdata.bin"
	capacity := uint64(100 * 1000) // 100k
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)

	log.Printf("Starting Market Data Feed. Mmap size: %.2f MB", float64(size)/1024/1024)

	mapped, file, err := shm.OpenOrCreateMmap(filePath, size)
	if err != nil {
		log.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	rb := ringbuf.Init(mapped, capacity)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	payload := make([]byte, ringbuf.PayloadSize)

	symbols := []string{"BTC/USD ", "ETH/USD ", "SOL/USD ", "DOGE/USD"}
	var symbolBytes [4][8]byte
	for i, s := range symbols {
		copy(symbolBytes[i][:], s)
	}

	prices := []float64{65000.50, 3500.25, 150.10, 0.15}

	log.Println("Feed started. Generating ticks...")

	count := 0
	start := time.Now()

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			symIdx := count % len(symbols)

			// Simulate price movement
			prices[symIdx] += (float64(count%3) - 1.0) * 0.1

			// Pack data: [8 bytes Symbol] [8 bytes Price] [4 bytes Volume]
			copy(payload[0:8], symbolBytes[symIdx][:])
			binary.LittleEndian.PutUint64(payload[8:16], math.Float64bits(prices[symIdx]))
			binary.LittleEndian.PutUint32(payload[16:20], uint32(1+(count%10)))

			// Push to ring buffer (spin wait if full)
			if rb.Push(mapped, payload) {
				count++
			}

			// Throttling just a little bit so we can actually read the output in the consumer
			time.Sleep(10 * time.Microsecond)
		}
	}

	log.Printf("Feed stopped. Sent %d ticks in %v", count, time.Since(start))
}
