# Price Parser (HFT-IPC Plugin System Example)

This example demonstrates the creation of a plugin system based on shared memory and IPC.
The Orchestrator launches parsers (JSON, CSV) and receives normalized data (e.g., prices) from them via ultra-fast queues.

## Potential Use Cases

This architecture can be used not only for web scraping/parsing but also for a variety of other high-load tasks:

*   **Universal Metrics Harvester**: Instant collection and aggregation of metrics from different microservices. Thanks to shared memory, this harvester operates instantly without consuming CPU resources.
*   **Log Processing and Aggregation**: Ultra-fast log collection with minimal latency (zero-copy) for subsequent filtering and writing to storage.

## Example Structure

*   `orchestrator` - manages processes and reads the final aggregated data.
*   `json_parser` - example plugin/worker for processing JSON.
*   `csv_parser` - example plugin/worker for processing CSV.
