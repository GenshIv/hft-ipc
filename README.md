# hft-ipc

**hft-ipc** is an ultra-low latency, zero-syscall Inter-Process Communication (IPC) library for Go. It is designed specifically for High-Frequency Trading (HFT) systems, game servers, and other performance-critical applications where microsecond or nanosecond latency is required.

## Features

- **Zero-Syscall Message Passing**: After the initial memory mapping, the OS kernel is completely bypassed. Data is transferred directly through shared physical/virtual memory.
- **Lock-Free Ring Buffer**: Uses pure Go `sync/atomic` operations. No mutexes, no channels, no blocking.
- **Cache-Line Padding**: Core structures are padded to 64 bytes (`CacheLineSize`) to prevent False Sharing across CPU cores.
- **Extreme Throughput**: Capable of processing up to **~18,000,000 transactions per second** on standard hardware (as measured in local benchmarks).

## Architecture

The project is built on two primary components:
1. **`shm` (Shared Memory)**: Utilizes `mmap` to project a single file (e.g., `hft_shared_memory.bin`) into the virtual memory space of multiple independent Go processes.
2. **`ringbuf` (Ring Buffer)**: A lock-free circular buffer mapped directly onto the shared memory region. Processes spin-poll the buffer using atomic `Head` and `Tail` pointers.

## Quick Start

### 1. Initialize the Ring Buffer
Both processes (e.g., Reader and Writer) must map the same file and initialize the buffer.

```go
package main

import (
	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
)

func main() {
    capacity := uint64(1000 * 1000)
    size := int(ringbuf.DataOffset) + int(capacity*ringbuf.PayloadSize)
    
    // Map memory
    mapped, file, _ := shm.OpenOrCreateMmap("hft_shared_memory.bin", size)
    defer file.Close()
    defer mapped.Unmap()

    // Initialize Lock-Free Buffer
    rb := ringbuf.Init(mapped, capacity)
}
```

### 2. Producer (Writer)
Push data into the ring buffer. This operation is non-blocking. If the buffer is full, it returns `false`.

```go
payload := make([]byte, ringbuf.PayloadSize)
// ... fill payload with data (e.g., using binary.LittleEndian)

for {
    if rb.Push(mapped, payload) {
        // Successfully sent to another process!
    } else {
        // Buffer is full, handle backpressure
    }
}
```

### 3. Consumer (Reader)
Pop data from the ring buffer. Uses a spin-lock strategy for minimum latency.

```go
payload := make([]byte, ringbuf.PayloadSize)

for {
    if rb.Pop(mapped, payload) {
        // Successfully received! Process the payload.
    } else {
        // Buffer empty. Yield to scheduler or spin.
        runtime.Gosched() 
    }
}
```

## Running the Examples

The repository includes a basic benchmark/demo via the `cmd` package.

**Terminal 1 (Reader):**
```bash
go run ./cmd/reader/main.go
```

**Terminal 2 (Writer):**
```bash
go run ./cmd/writer/main.go
```

## Advanced Samples

The `samples/` directory contains realistic usage patterns:

1. **Market Data Feed** (`samples/marketdata`): Classic low-latency binary data transfer.
2. **High-Throughput Logger** (`samples/logger`): Offloading I/O operations from critical code paths.
3. **Hot-Swappable Plugin System** (`samples/plugin_system`): Two-way, process-level modularity using dual SPSC buffers.
4. **Dynamic Orchestrator** (`samples/price_parser`): A Multi-Producer, Single-Consumer (MPSC) architecture where multiple independent parsers (`csv_parser`, `json_parser`) write to their own channels, and a central Orchestrator dynamically discovers and polls them without Mutex locks or restarts.

See `samples/README.md` for run instructions.

## Benchmarks

Run the built-in benchmarks with:
```bash
go test -bench . -benchmem ./benchmarks
```

**Results (AMD Ryzen 9 7950X3D):**
- **Data Packing (CSV/JSON):** ~7.2 ns/op
- **Delivery 1-to-1:** ~53.8 ns/op (~18.5 million TPS)
- **Delivery 3-to-1 (Orchestrator):** ~43.0 ns/message (129 ns per 3-source cycle)

*Note: The orchestrator pattern achieves higher efficiency (43ns vs 54ns) because the fast-polling loop multiplexes data sources, practically eliminating CPU spin-wait starvation.*

## Use Cases
- **HFT Trading Engines**: Web or TCP gateway processes handling JSON/FIX protocols can write directly to the core matching engine, decoupling I/O from computation.
- **Gateway/Engine Architecture**: Decouple slow, blocking I/O (WebSockets, HTTP) into a separate process, isolating your core business logic engine from network failures, DDoS, or GC pauses of the web server.
- **Hot-Reloading Modules**: Update components of your system on the fly by spawning a new process and redirecting the IPC ring buffer to it without stopping the main application.
