package main

import (
	"encoding/binary"
	"fmt"
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
	// Ensure channels directory exists
	os.MkdirAll("channels", 0755)

	filePath := "channels/json_parser.bin"
	capacity := uint64(50 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.DefaultPayloadSize)

	log.Println("Starting JSON Parser...")

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

	// Simulate parsing a stream of JSON price updates
	skus := []string{"MONITOR-27", "KEYBOARD-01", "HEADSET-09", "LAPTOP-01"}
	basePrices := []float64{250.0, 75.0, 120.0, 995.0} // JSON says LAPTOP is 995.0

	log.Println("JSON Parser running. Emitting prices...")

	count := 0
	start := time.Now()

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			idx := count % len(skus)

			// Simulate price fluctuation in the JSON feed
			currentPrice := basePrices[idx] + (float64(count%3) - 1.0)

			// Structure:
			// 0-31: SKU (32 bytes)
			// 32-39: Price (float64)
			// 40: Source ID (0x02 for JSON)

			for i := 0; i < 32; i++ {
				payload[i] = 0
			}
			copy(payload[0:32], skus[idx])

			binary.LittleEndian.PutUint64(payload[32:40], math.Float64bits(currentPrice))
			payload[40] = 0x02 // JSON Source

			if rb.Push(mapped, payload) {
				count++
				if count%100000 == 0 {
					fmt.Printf("[JSON] Pushed %d records. Last: %s -> %.2f\n", count, skus[idx], currentPrice)
				}
			}

			// Slightly different speed than CSV
			time.Sleep(7 * time.Microsecond)
		}
	}

	log.Printf("JSON Parser stopped. Processed %d records in %v", count, time.Since(start))
}
