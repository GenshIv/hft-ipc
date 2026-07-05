package main

import (
	"bytes"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
	filePath := "hft_logger.bin"
	capacity := uint64(500 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)

	log.Printf("Starting Logger Sink...")

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

	outFile, err := os.OpenFile("application.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer outFile.Close()

	log.Println("Sink started. Writing to application.log...")

	count := 0
	batch := make([]byte, 0, 1024*1024) // 1MB batch buffer

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			if rb.Pop(mapped, payload) {
				// Find length of string (null terminated)
				idx := bytes.IndexByte(payload, 0)
				if idx == -1 {
					idx = len(payload)
				}

				batch = append(batch, payload[:idx]...)
				batch = append(batch, '\n')
				count++

				// Flush batch to disk if it gets large enough
				if len(batch) > 500*1024 {
					outFile.Write(batch)
					batch = batch[:0]
				}
			} else {
				// Flush remaining if idle
				if len(batch) > 0 {
					outFile.Write(batch)
					batch = batch[:0]
				}
				runtime.Gosched()
			}
		}
	}

	// Final flush
	if len(batch) > 0 {
		outFile.Write(batch)
	}

	log.Printf("Sink stopped. Wrote %d logs to disk.", count)
}
