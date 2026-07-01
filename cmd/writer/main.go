package main

import (
	"encoding/binary"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hft-ipc/ringbuf"
	"hft-ipc/shm"
)

func main() {
	filePath := "hft_shared_memory.bin"
	capacity := uint64(1000 * 1000) // 1 million entries
	
	// Size = header size + capacity * payload size
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)
	
	log.Printf("Starting writer. Mmap size: %d bytes (%.2f MB)", size, float64(size)/1024/1024)
	
	mapped, file, err := shm.OpenOrCreateMmap(filePath, size)
	if err != nil {
		log.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	rb := ringbuf.Init(mapped, capacity)

	// Setup graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	payload := make([]byte, ringbuf.PayloadSize)
	var txID uint64 = 1

	log.Println("Writer started. Writing transactions...")
	
	start := time.Now()
	written := 0

loop:
	for {
		select {
		case <-sigs:
			log.Println("Interrupt received, stopping...")
			break loop
		default:
			// Prepare payload
			binary.LittleEndian.PutUint64(payload[0:8], txID)
			binary.LittleEndian.PutUint64(payload[8:16], uint64(time.Now().UnixNano()))
			// Fill the rest with some dummy data
			for i := 16; i < ringbuf.PayloadSize; i++ {
				payload[i] = 0xFF
			}

			// Try to push. If full, spin wait.
			if rb.Push(mapped, payload) {
				txID++
				written++
			}
			
			// Small sleep so we don't completely lock up the OS in this demo.
			// In real HFT, you might use runtime.Gosched() or an exponential backoff.
			if written%100000 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
	
	elapsed := time.Since(start)
	log.Printf("Writer stopped. Wrote %d transactions in %v (%.2f tx/sec)", 
		written, elapsed, float64(written)/elapsed.Seconds())
}
