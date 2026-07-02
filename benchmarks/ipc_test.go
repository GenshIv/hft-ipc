package benchmarks

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
	"github.com/edsrzf/mmap-go"
)

// BenchmarkPack_CSV measures the speed of formatting/packing mock CSV data
func BenchmarkPack_CSV(b *testing.B) {
	payload := make([]byte, ringbuf.DefaultPayloadSize)
	sku := "LAPTOP-01"
	price := 1000.50

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear SKU
		for j := 0; j < 32; j++ {
			payload[j] = 0
		}
		copy(payload[0:32], sku)
		binary.LittleEndian.PutUint64(payload[32:40], math.Float64bits(price))
		payload[40] = 0x01
	}
}

// BenchmarkPack_JSON measures the speed of formatting/packing mock JSON data
// In our architecture, the actual data packing is the same, so speeds should be identical,
// but in a real app, JSON unmarshaling would be measured here.
func BenchmarkPack_JSON(b *testing.B) {
	payload := make([]byte, ringbuf.DefaultPayloadSize)
	sku := "MONITOR-27"
	price := 250.75

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 32; j++ {
			payload[j] = 0
		}
		copy(payload[0:32], sku)
		binary.LittleEndian.PutUint64(payload[32:40], math.Float64bits(price))
		payload[40] = 0x02
	}
}

// BenchmarkDelivery_1to1 measures the end-to-end throughput of a single IPC channel
func BenchmarkDelivery_1to1(b *testing.B) {
	os.MkdirAll("test_channels", 0755)
	path := filepath.Join("test_channels", "bench_1to1.bin")
	os.Remove(path) // Ensure clean

	capacity := uint64(50 * 1000)
	size := int(ringbuf.DataOffset) + int(capacity*ringbuf.DefaultPayloadSize)

	mapped, file, err := shm.OpenOrCreateMmap(path, size)
	if err != nil {
		b.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	rb := ringbuf.Init(mapped, capacity, ringbuf.DefaultPayloadSize)

	payloadIn := make([]byte, ringbuf.DefaultPayloadSize)
	payloadOut := make([]byte, ringbuf.DefaultPayloadSize)

	// Producer
	go func() {
		for i := 0; i < b.N; i++ {
			for !rb.Push(mapped, payloadIn) {
				runtime.Gosched()
			}
		}
	}()

	b.ResetTimer()

	// Consumer
	for i := 0; i < b.N; i++ {
		for !rb.Pop(mapped, payloadOut) {
			runtime.Gosched()
		}
	}
}

// BenchmarkDelivery_3to1 measures the throughput of an orchestrator reading from 3 sources
func BenchmarkDelivery_3to1(b *testing.B) {
	os.MkdirAll("test_channels", 0755)

	setupChannel := func(name string) (mmap.MMap, *ringbuf.RingBuffer, *os.File) {
		path := filepath.Join("test_channels", name)
		os.Remove(path)

		capacity := uint64(50 * 1000)
		size := int(ringbuf.DataOffset) + int(capacity*ringbuf.DefaultPayloadSize)

		mapped, file, err := shm.OpenOrCreateMmap(path, size)
		if err != nil {
			b.Fatalf("Failed to mmap: %v", err)
		}

		rb := ringbuf.Init(mapped, capacity, ringbuf.DefaultPayloadSize)
		return mapped, rb, file
	}

	map1, rb1, file1 := setupChannel("bench_3to1_1.bin")
	defer file1.Close()
	defer map1.Unmap()

	map2, rb2, file2 := setupChannel("bench_3to1_2.bin")
	defer file2.Close()
	defer map2.Unmap()

	map3, rb3, file3 := setupChannel("bench_3to1_3.bin")
	defer file3.Close()
	defer map3.Unmap()

	payloadIn := make([]byte, ringbuf.DefaultPayloadSize)
	payloadOut := make([]byte, ringbuf.DefaultPayloadSize)

	// Producers
	producer := func(rb *ringbuf.RingBuffer, mapped []byte) {
		for i := 0; i < b.N; i++ {
			for !rb.Push(mapped, payloadIn) {
				runtime.Gosched()
			}
		}
	}

	go producer(rb1, map1)
	go producer(rb2, map2)
	go producer(rb3, map3)

	b.ResetTimer()

	// Consumer (Orchestrator)
	total := b.N * 3
	count := 0

	for count < total {
		processedAny := false
		if rb1.Pop(map1, payloadOut) {
			count++
			processedAny = true
		}
		if rb2.Pop(map2, payloadOut) {
			count++
			processedAny = true
		}
		if rb3.Pop(map3, payloadOut) {
			count++
			processedAny = true
		}

		if !processedAny {
			runtime.Gosched()
		}
	}
}
