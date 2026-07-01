package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
	capacity := uint64(10 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)

	// 1. Host -> Plugin buffer (Plugin reads from this)
	rxMap, rxFile, err := shm.OpenOrCreateMmap("shm_host_to_plugin.bin", size)
	if err != nil {
		log.Fatalf("Failed to mmap RX: %v", err)
	}
	defer rxFile.Close()
	defer rxMap.Unmap()
	rxRb := ringbuf.Init(rxMap, capacity)

	// 2. Plugin -> Host buffer (Plugin writes to this)
	txMap, txFile, err := shm.OpenOrCreateMmap("shm_plugin_to_host.bin", size)
	if err != nil {
		log.Fatalf("Failed to mmap TX: %v", err)
	}
	defer txFile.Close()
	defer txMap.Unmap()
	txRb := ringbuf.Init(txMap, capacity)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	rxPayload := make([]byte, ringbuf.PayloadSize)

	log.Println("Plugin started. Waiting for tasks...")
	
	processed := 0

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			// Read task
			if rxRb.Pop(rxMap, rxPayload) {
				// Process task (in a real scenario, do some work)
				// For this sample, we just mirror it back to prove roundtrip
				
				// Push result back
				// Spin wait until there's room in the TX buffer
				for !txRb.Push(txMap, rxPayload) {
					runtime.Gosched()
				}
				processed++
			} else {
				runtime.Gosched()
			}
		}
	}
	
	log.Printf("Plugin stopped. Processed %d tasks.", processed)
}
