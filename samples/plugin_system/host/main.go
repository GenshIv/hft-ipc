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
	capacity := uint64(10 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.DefaultPayloadSize)

	// 1. Host -> Plugin buffer
	txMap, txFile, err := shm.OpenOrCreateMmap("shm_host_to_plugin.bin", size)
	if err != nil {
		log.Fatalf("Failed to mmap TX: %v", err)
	}
	defer txFile.Close()
	defer txMap.Unmap()
	txRb := ringbuf.Init(txMap, capacity, ringbuf.DefaultPayloadSize)

	// 2. Plugin -> Host buffer
	rxMap, rxFile, err := shm.OpenOrCreateMmap("shm_plugin_to_host.bin", size)
	if err != nil {
		log.Fatalf("Failed to mmap RX: %v", err)
	}
	defer rxFile.Close()
	defer rxMap.Unmap()
	rxRb := ringbuf.Init(rxMap, capacity, ringbuf.DefaultPayloadSize)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	txPayload := make([]byte, ringbuf.DefaultPayloadSize)
	rxPayload := make([]byte, ringbuf.DefaultPayloadSize)

	log.Println("Host started. Sending tasks to plugin...")

	var taskID uint64 = 1
	var received uint64 = 0

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			// Push a task if there's room
			binary.LittleEndian.PutUint64(txPayload[0:8], taskID)
			binary.LittleEndian.PutUint64(txPayload[8:16], uint64(time.Now().UnixNano()))

			if txRb.Push(txMap, txPayload) {
				taskID++
			}

			// Read a result if available
			if rxRb.Pop(rxMap, rxPayload) {
				resultTaskID := binary.LittleEndian.Uint64(rxPayload[0:8])
				resultTime := binary.LittleEndian.Uint64(rxPayload[8:16])

				latency := time.Now().UnixNano() - int64(resultTime)
				received++

				if received%50000 == 0 {
					log.Printf("Received result for task %d. Roundtrip latency: %d ns", resultTaskID, latency)
				}
			} else {
				runtime.Gosched()
			}

			// Optional: sleep to not burn 100% CPU on host
			time.Sleep(10 * time.Microsecond)
		}
	}

	log.Printf("Host stopped. Sent %d tasks, received %d results.", taskID-1, received)
}
