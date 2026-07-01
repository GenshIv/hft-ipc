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
	
	filePath := "channels/csv_parser.bin"
	capacity := uint64(50 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)

	log.Println("Starting CSV Parser...")

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
	
	// Simulate parsing a large CSV file line by line
	skus := []string{"LAPTOP-01", "PHONE-05", "TV-55", "MOUSE-02"}
	basePrices := []float64{1000.0, 500.0, 750.0, 25.0}

	log.Println("CSV Parser running. Emitting prices...")
	
	count := 0
	start := time.Now()

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			idx := count % len(skus)
			
			// Simulate price fluctuation in the CSV
			currentPrice := basePrices[idx] + (float64(count%5) - 2.0)
			
			// Structure: 
			// 0-31: SKU (32 bytes)
			// 32-39: Price (float64)
			// 40: Source ID (0x01 for CSV)
			
			// Clear SKU area
			for i := 0; i < 32; i++ { payload[i] = 0 }
			copy(payload[0:32], skus[idx])
			
			binary.LittleEndian.PutUint64(payload[32:40], math.Float64bits(currentPrice))
			payload[40] = 0x01 // CSV Source
			
			// Push to ring buffer (spin wait if full)
			if rb.Push(mapped, payload) {
				count++
				if count%100000 == 0 {
					fmt.Printf("[CSV] Pushed %d records. Last: %s -> %.2f\n", count, skus[idx], currentPrice)
				}
			}
			
			// Small sleep to simulate parsing overhead
			time.Sleep(5 * time.Microsecond)
		}
	}
	
	log.Printf("CSV Parser stopped. Processed %d records in %v", count, time.Since(start))
}
