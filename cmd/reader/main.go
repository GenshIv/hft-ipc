package main

import (
	"encoding/binary"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
	filePath := "hft_shared_memory.bin"
	capacity := uint64(1000 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)

	log.Printf("Starting reader...")

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

	log.Println("Reader started. Waiting for transactions...")

	var lastTxID uint64
	var totalLatency time.Duration
	var count uint64

	start := time.Now()

loop:
	for {
		select {
		case <-sigs:
			log.Println("Interrupt received, stopping...")
			break loop
		default:
			if rb.Pop(mapped, payload) {
				txID := binary.LittleEndian.Uint64(payload[0:8])
				ts := binary.LittleEndian.Uint64(payload[8:16])

				// Calculate latency
				latency := time.Now().UnixNano() - int64(ts)
				if latency > 0 {
					totalLatency += time.Duration(latency)
				}

				count++
				lastTxID = txID

				// Print stats periodically
				if count%500000 == 0 {
					log.Printf("Read %d txs. Last ID: %d, Avg Latency: %v",
						count, lastTxID, totalLatency/time.Duration(count))
					// Reset average
					totalLatency = 0
					count = 0
				}
			} else {
				// Spin wait if empty
				runtime.Gosched()
			}
		}
	}

	elapsed := time.Since(start)
	log.Printf("Reader stopped. Alive for %v", elapsed)
}
