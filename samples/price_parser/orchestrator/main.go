package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

type ActiveChannel struct {
	Name    string
	File    *os.File
	Map     mmap.MMap
	RingBuf *ringbuf.RingBuffer
}

func main() {
	log.Println("Starting Dynamic Orchestrator...")
	
	// Ensure channels directory exists
	channelsDir := "channels"
	os.MkdirAll(channelsDir, 0755)

	// We use atomic.Value to hold []*ActiveChannel so the fast loop is completely lock-free
	var channels atomic.Value
	channels.Store(make([]*ActiveChannel, 0))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Background routine to discover new channels (Directory Scanner)
	go func() {
		knownFiles := make(map[string]bool)
		
		for {
			files, err := os.ReadDir(channelsDir)
			if err == nil {
				for _, f := range files {
					if f.IsDir() || filepath.Ext(f.Name()) != ".bin" {
						continue
					}
					
					fullPath := filepath.Join(channelsDir, f.Name())
					if !knownFiles[fullPath] {
						// Found a new channel!
						capacity := uint64(50 * 1000)
						size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)
						
						m, file, err := shm.OpenOrCreateMmap(fullPath, size)
						if err != nil {
							log.Printf("Failed to map new channel %s: %v", fullPath, err)
							continue
						}
						
						rb := ringbuf.Init(m, capacity)
						
						ch := &ActiveChannel{
							Name:    f.Name(),
							File:    file,
							Map:     m,
							RingBuf: rb,
						}
						
						// Add to atomic list
						oldSlice := channels.Load().([]*ActiveChannel)
						newSlice := append(oldSlice, ch)
						channels.Store(newSlice)
						
						knownFiles[fullPath] = true
						log.Printf("Orchestrator: Discovered and connected to %s", f.Name())
					}
				}
			}
			time.Sleep(1 * time.Second)
		}
	}()

	payload := make([]byte, ringbuf.PayloadSize)
	
	// Database mockup
	priceDB := make(map[string]float64)
	updatesCount := 0
	totalReceived := 0

	log.Println("Orchestrator fast-loop running. Waiting for channels...")

loop:
	for {
		select {
		case <-sigs:
			break loop
		default:
			processedAny := false
			
			// Load current channels atomically (very fast)
			currentChannels := channels.Load().([]*ActiveChannel)

			// Poll all known channels
			for _, ch := range currentChannels {
				if ch.RingBuf.Peek(ch.Map, payload) {
					// 1. Process the data (e.g. save to DB, analyze)
					processPayload(payload, priceDB, &updatesCount, &totalReceived, ch.Name)
					
					// 2. Acknowledge the message (advance Tail). 
					// If orchestrator crashes before this line, the message is NOT lost!
					ch.RingBuf.Ack()
					
					processedAny = true
				}
			}

			if !processedAny {
				runtime.Gosched() // Spin wait if all are empty
			}
		}
	}
	
	// Cleanup
	currentChannels := channels.Load().([]*ActiveChannel)
	for _, ch := range currentChannels {
		ch.Map.Unmap()
		ch.File.Close()
	}
	
	log.Printf("Orchestrator stopped. Received: %d, DB Updates: %d", totalReceived, updatesCount)
}

func processPayload(payload []byte, db map[string]float64, updates *int, received *int, channelName string) {
	*received++
	
	// Parse SKU (trim null bytes)
	skuBytes := payload[0:32]
	idx := bytes.IndexByte(skuBytes, 0)
	if idx == -1 {
		idx = 32
	}
	sku := string(skuBytes[:idx])
	
	// Parse Price
	price := math.Float64frombits(binary.LittleEndian.Uint64(payload[32:40]))
	
	// Parse Source
	source := "Unknown"
	if payload[40] == 0x01 {
		source = "CSV"
	} else if payload[40] == 0x02 {
		source = "JSON"
	}

	// Check if update is needed
	oldPrice, exists := db[sku]
	if !exists || math.Abs(oldPrice-price) > 0.001 {
		db[sku] = price
		*updates++
		
		if *updates%10000 == 0 {
			log.Printf("[DB UPDATE] %s | Channel: %s | Source: %s | Price changed: %.2f -> %.2f", sku, channelName, source, oldPrice, price)
		}
	}
}
