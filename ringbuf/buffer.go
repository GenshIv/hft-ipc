package ringbuf

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

const (
	CacheLineSize = 64
	// SkipMarker is written as the length when there is not enough space at the end of the buffer.
	// This instructs consumers to wrap around to physical offset 0.
	SkipMarker = 0xFFFFFFFF
)

type padding [CacheLineSize - 8]byte

// RingBuffer implements a tightly packed lock-free byte queue.
// It supports a Single Producer and Multiple Consumers (SPMC).
type RingBuffer struct {
	// WriteOffset is the virtual byte offset where the next write will occur.
	WriteOffset uint64
	_           padding

	// ReadOffset is the virtual byte offset where the next read will occur.
	// Workers compete using CAS on this offset to grab chunks.
	ReadOffset uint64
	_          padding

	// Capacity of the buffer in bytes (excluding header).
	Capacity uint64
	_        padding
}

// DataOffset is where the byte stream begins in the mmap.
var DataOffset = unsafe.Sizeof(RingBuffer{})

// Init initializes the RingBuffer metadata.
// capacityBytes is the physical size of the data region.
func Init(mapped []byte, capacityBytes uint64) *RingBuffer {
	rb := (*RingBuffer)(unsafe.Pointer(&mapped[0]))
	if atomic.LoadUint64(&rb.Capacity) == 0 {
		atomic.StoreUint64(&rb.Capacity, capacityBytes)
		atomic.StoreUint64(&rb.WriteOffset, 0)
		atomic.StoreUint64(&rb.ReadOffset, 0)
	}
	return rb
}

// Push writes a variable-length payload to the byte queue.
// This is for a Single Producer.
func (rb *RingBuffer) Push(mapped []byte, payload []byte) bool {
	L := uint64(len(payload))
	reqSize := 4 + L
	capBytes := atomic.LoadUint64(&rb.Capacity)

	w := atomic.LoadUint64(&rb.WriteOffset)
	r := atomic.LoadUint64(&rb.ReadOffset)

	used := w - r
	if capBytes-used < reqSize {
		return false // Buffer full
	}

	physW := w % capBytes

	// Check if it fits before the wrap boundary
	if physW+reqSize > capBytes {
		// Doesn't fit. We need to skip the rest of the physical buffer.
		skipBytes := capBytes - physW
		if capBytes-used < reqSize+skipBytes {
			return false // Buffer full (including skip space)
		}

		// Write SkipMarker if there is at least 4 bytes left for the marker
		if skipBytes >= 4 {
			offset := DataOffset + uintptr(physW)
			binary.LittleEndian.PutUint32(mapped[offset:offset+4], SkipMarker)
		}

		// Advance w to next multiple of capBytes (virtual wrap)
		w += skipBytes
		physW = 0
	}

	// Write length
	offset := DataOffset + uintptr(physW)
	binary.LittleEndian.PutUint32(mapped[offset:offset+4], uint32(L))

	// Write payload
	copy(mapped[offset+4:offset+4+uintptr(L)], payload)

	// Publish the write atomically
	atomic.StoreUint64(&rb.WriteOffset, w+reqSize)
	return true
}

// GrabChunk attempts to atomically read and claim the next chunk.
// This is lock-free and perfectly safe for Multiple Consumers (Workers) running concurrently.
// It returns a Zero-Copy byte slice pointing to mmap and a boolean indicating success.
// If the queue is empty, returns (nil, false).
func (rb *RingBuffer) GrabChunk(mapped []byte) ([]byte, bool) {
	for {
		w := atomic.LoadUint64(&rb.WriteOffset)
		r := atomic.LoadUint64(&rb.ReadOffset)

		if r >= w {
			return nil, false // Empty
		}

		capBytes := atomic.LoadUint64(&rb.Capacity)
		physR := r % capBytes

		var skipBytes uint64
		if capBytes-physR < 4 {
			skipBytes = capBytes - physR
		} else {
			offset := DataOffset + uintptr(physR)
			length := binary.LittleEndian.Uint32(mapped[offset : offset+4])
			if length == SkipMarker {
				skipBytes = capBytes - physR
			} else {
				// We found a valid chunk!
				nextR := r + 4 + uint64(length)

				// Safety check: chunk shouldn't exceed the published WriteOffset
				if nextR > w {
					return nil, false // Data not fully written yet (or corrupted)
				}

				// Try to claim this chunk with CAS
				if atomic.CompareAndSwapUint64(&rb.ReadOffset, r, nextR) {
					// Claimed successfully! Return zero-copy slice
					return mapped[offset+4 : offset+4+uintptr(length)], true
				}
				// CAS failed (another worker grabbed it). Loop and try again.
				continue
			}
		}

		if skipBytes > 0 {
			// Try to advance ReadOffset past the skip using CAS, so we don't stall.
			// If CAS fails, someone else advanced it, we just continue.
			atomic.CompareAndSwapUint64(&rb.ReadOffset, r, r+skipBytes)
			continue
		}
	}
}
