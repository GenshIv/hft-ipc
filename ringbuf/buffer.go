package ringbuf

import (
	"sync/atomic"
	"unsafe"
)

const (
	// CacheLineSize is typically 64 bytes on x86-64 and modern ARM.
	CacheLineSize = 64
	
	// PayloadSize we will use 512 bytes for the payload to simulate log messages
	PayloadSize = 512
)

// padding to avoid false sharing between producers and consumers.
type padding [CacheLineSize - 8]byte

// RingBuffer structure mapped directly to memory.
// It uses atomic operations to update Head and Tail.
type RingBuffer struct {
	// Head is written by the producer, read by the consumer.
	Head uint64
	_    padding // Prevents False Sharing

	// Tail is written by the consumer, read by the producer.
	Tail uint64
	_    padding // Prevents False Sharing

	// Capacity of the ring buffer
	Capacity uint64
	_        padding

	// The actual data starts immediately after this struct in memory.
}

// DataOffset is where the array of items begins in the mmap.
var DataOffset = unsafe.Sizeof(RingBuffer{})

// Init initializes the RingBuffer metadata in the mapped memory.
func Init(mapped []byte, capacity uint64) *RingBuffer {
	// Cast the beginning of the mapped memory to *RingBuffer
	rb := (*RingBuffer)(unsafe.Pointer(&mapped[0]))
	
	// Only initialize if capacity is not set (first time file is created)
	if atomic.LoadUint64(&rb.Capacity) == 0 {
		atomic.StoreUint64(&rb.Capacity, capacity)
		atomic.StoreUint64(&rb.Head, 0)
		atomic.StoreUint64(&rb.Tail, 0)
	}
	return rb
}

// Push tries to write a payload to the buffer. Returns false if full.
func (rb *RingBuffer) Push(mapped []byte, payload []byte) bool {
	head := atomic.LoadUint64(&rb.Head)
	tail := atomic.LoadUint64(&rb.Tail)
	cap := atomic.LoadUint64(&rb.Capacity)

	// If the buffer is full, we can't write.
	if head-tail >= cap {
		return false
	}

	// Calculate index
	idx := head % cap
	
	// Calculate memory offset
	offset := DataOffset + uintptr(idx*PayloadSize)
	
	// Copy payload directly into mapped memory (Zero-copy from perspective of Go GC!)
	copy(mapped[offset:offset+PayloadSize], payload)
	
	// Atomically advance head (release barrier)
	atomic.AddUint64(&rb.Head, 1)
	
	return true
}

// Pop tries to read a payload from the buffer. Returns false if empty.
func (rb *RingBuffer) Pop(mapped []byte, payloadOut []byte) bool {
	head := atomic.LoadUint64(&rb.Head)
	tail := atomic.LoadUint64(&rb.Tail)
	cap := atomic.LoadUint64(&rb.Capacity)

	// If the buffer is empty, we can't read.
	if head == tail {
		return false
	}

	// Calculate index
	idx := tail % cap
	
	// Calculate memory offset
	offset := DataOffset + uintptr(idx*PayloadSize)
	
	// Copy from mapped memory to output
	copy(payloadOut, mapped[offset:offset+PayloadSize])
	
	// Atomically advance tail
	atomic.AddUint64(&rb.Tail, 1)
	
	return true
}

// Peek tries to read a payload from the buffer without advancing the tail.
// Returns false if empty. Use Ack() to advance the tail after successful processing.
func (rb *RingBuffer) Peek(mapped []byte, payloadOut []byte) bool {
	head := atomic.LoadUint64(&rb.Head)
	tail := atomic.LoadUint64(&rb.Tail)
	cap := atomic.LoadUint64(&rb.Capacity)

	// If the buffer is empty, we can't read.
	if head == tail {
		return false
	}

	// Calculate index
	idx := tail % cap
	
	// Calculate memory offset
	offset := DataOffset + uintptr(idx*PayloadSize)
	
	// Copy from mapped memory to output
	copy(payloadOut, mapped[offset:offset+PayloadSize])
	
	return true
}

// Ack advances the tail, marking the message previously read by Peek as processed.
// This implements At-Least-Once delivery semantics.
func (rb *RingBuffer) Ack() {
	atomic.AddUint64(&rb.Tail, 1)
}
