package main

import (
	"encoding/binary"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
	filePath := "hft_shared_memory.bin"
	capacityBytes := uint64(1024 * 1024 * 10) // 10 MB capacity

	size := int(ringbuf.DataOffset) + int(capacityBytes)

	log.Printf("Starting writer. Mmap size: %d bytes (%.2f MB)", size, float64(size)/1024/1024)

	mapped, file, err := shm.OpenOrCreateMmap(filePath, size)
	if err != nil {
		log.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	rb := ringbuf.Init(mapped, capacityBytes)

	// Setup graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Vary payload size to demonstrate the continuous byte queue
	payload1 := make([]byte, 128)
	payload2 := make([]byte, 512)

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
			// Prepare payload1
			binary.LittleEndian.PutUint64(payload1[0:8], txID)
			binary.LittleEndian.PutUint64(payload1[8:16], uint64(time.Now().UnixNano()))
			for i := 16; i < len(payload1); i++ {
				payload1[i] = 0xAA
			}

			// Try to push. If full, spin wait.
			for !rb.Push(mapped, payload1) {
				// spin wait
			}
			txID++
			written++

			// Prepare payload2
			binary.LittleEndian.PutUint64(payload2[0:8], txID)
			binary.LittleEndian.PutUint64(payload2[8:16], uint64(time.Now().UnixNano()))
			for i := 16; i < len(payload2); i++ {
				payload2[i] = 0xBB
			}

			for !rb.Push(mapped, payload2) {
				// spin wait
			}
			txID++
			written++

			// Small sleep so we don't completely lock up the OS in this demo.
			if written%100000 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}

	elapsed := time.Since(start)
	log.Printf("Writer stopped. Wrote %d transactions in %v (%.2f tx/sec)",
		written, elapsed, float64(written)/elapsed.Seconds())
}
