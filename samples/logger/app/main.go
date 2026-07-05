package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
	filePath := "hft_logger.bin"
	capacity := uint64(500 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)

	log.Printf("Starting Logger App. Mmap size: %.2f MB", float64(size)/1024/1024)

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

	log.Println("App started. Emitting logs...")

	count := 0
	start := time.Now()

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			count++
			logMessage := fmt.Sprintf(`{"time": "%s", "level": "INFO", "msg": "User login successful", "user_id": %d}`,
				time.Now().Format(time.RFC3339Nano), count)

			// Clear payload (optional but good for clean strings)
			for i := range payload {
				payload[i] = 0
			}

			// Copy string into payload
			copy(payload, logMessage)

			// If ring buffer is full, we could drop the log or wait. Let's spin wait.
			if rb.Push(mapped, payload) {
				// Log emitted
			} else {
				// Spin wait
			}

			time.Sleep(100 * time.Microsecond)
		}
	}

	log.Printf("App stopped. Emitted %d logs in %v", count, time.Since(start))
}
