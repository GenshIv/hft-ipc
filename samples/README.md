# HFT-IPC Samples

This directory contains three distinct use cases demonstrating the capabilities of the `hft-ipc` library for high-frequency, low-latency inter-process communication in Go.

## 1. Market Data Feed (`marketdata`)
Demonstrates classic HFT usage where a feed generator sends structured binary market data (Symbol, Price, Size) to a consumer (Algorithm).

**How to run:**
1. Open two terminals.
2. Terminal 1 (Consumer): `go run ./samples/marketdata/algo`
3. Terminal 2 (Producer): `go run ./samples/marketdata/feed`

## 2. Low-Latency Logger (`logger`)
Demonstrates how to offload I/O operations from a critical application. The application emits string logs into the ring buffer, and a separate "sink" process consumes them and writes them to disk in batches.

**How to run:**
1. Open two terminals.
2. Terminal 1 (Sink): `go run ./samples/logger/sink`
3. Terminal 2 (App): `go run ./samples/logger/app`
4. Check the generated `application.log` file.

## 3. Plugin System (`plugin_system`)
Demonstrates two-way IPC. A host application sends tasks to an external plugin process, and the plugin sends results back. This is achieved by using *two* separate ring buffer shared memory files (`shm_host_to_plugin.bin` and `shm_plugin_to_host.bin`).

**How to run:**
1. Open two terminals.
2. Terminal 1 (Plugin): `go run ./samples/plugin_system/plugin`
3. Terminal 2 (Host): `go run ./samples/plugin_system/host`

## 4. Price Parser (Dynamic Orchestrator)
Demonstrates a real-world scenario where multiple processes parse large datasets (e.g., CSV and JSON) and send them to a central Orchestrator process. Because `hft-ipc` uses an SPSC (Single-Producer Single-Consumer) lock-free ring buffer for maximum speed, this architecture uses a **Directory Discovery** approach. The Orchestrator monitors a `./channels/` directory and dynamically maps any new `*.bin` files created by parsers, adding them to its high-speed polling loop without locking or restarting.

**How to run:**
1. Open three terminals.
2. Terminal 1 (Orchestrator): `go run ./samples/price_parser/orchestrator`
3. Terminal 2 (CSV Parser): `go run ./samples/price_parser/csv_parser`
4. Terminal 3 (JSON Parser): `go run ./samples/price_parser/json_parser`
