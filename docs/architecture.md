# PiUPS Breakout Garden - Architecture

## Overview

PiUPS is a production IoT data pipeline that reads environmental sensors on a Raspberry Pi (2GB RAM) and streams data to Snowflake in real-time using Snowpipe Streaming v2 REST API.

## Design Decisions

### Why Snowpipe Streaming v2 REST API (not SDK)?

| Consideration | REST API | Python SDK |
|--------------|----------|-----------|
| Memory footprint | ~30MB (requests+jwt) | ~200MB+ (Java/Rust core) |
| Pi compatibility | Pure Python, ARM native | Requires Rust compilation |
| Latency | 5-10s ingest-to-query | 3-5s |
| Throughput | Sufficient for IoT (1-10 rows/s) | Designed for 10GB/s |
| Complexity | Simple HTTP calls | Channel management abstraction |

**Decision:** REST API. The 2GB Pi cannot spare 200MB+ for the SDK. IoT workloads produce 1-10 rows/second which is well within REST API limits. The 16MB request payload limit is never approached.

### Why Background Sensor Thread?

Sensors (especially BME680 gas heater and VL53L1X ranging) have variable response times. A dedicated background thread reads sensors continuously at `read_interval_seconds` (5s default) and caches the latest values. The main thread builds batches from the cache at `batch_interval_seconds` (10s default).

This prevents slow sensor reads from blocking Snowflake streaming and ensures the OLED display updates smoothly.

### Why continuation_token (not offset_token alone)?

The Snowpipe Streaming v2 REST API uses a `next_continuation_token` returned from each API call to maintain exactly-once delivery semantics. This token encapsulates both client and row sequencers, ensuring in-order delivery even across reconnections.

### Authentication Flow

```
┌─────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Pi    │     │  Snowflake Auth  │     │  Streaming API  │
│ (client)│     │  /oauth/token    │     │  /v2/streaming  │
└────┬────┘     └────────┬─────────┘     └────────┬────────┘
     │                   │                         │
     │  [PAT mode]       │                         │
     │──── Bearer PAT ───┼─────────────────────────▶
     │                   │                         │
     │  [Key-pair mode]  │                         │
     │── Generate JWT ──▶│                         │
     │◀── OAuth token ──│                         │
     │──── Bearer OAuth ─┼─────────────────────────▶
     │                   │                         │
```

### Memory Budget (2GB Pi)

| Component | Allocation |
|-----------|-----------|
| OS + services | ~800MB |
| Python interpreter | ~50MB |
| Sensor libraries | ~30MB |
| requests/jwt/crypto | ~30MB |
| luma.oled (PIL) | ~40MB |
| Application buffers | ~20MB |
| GC headroom | ~50MB |
| **Total app** | **~220MB** |
| systemd MemoryMax | 256MB |

The `gc.collect()` call after each batch ensures no memory accumulation from JSON serialization buffers.

### Sensor Configuration

Each sensor can be independently enabled/disabled via config:

```json
{
  "sensors": {
    "bh1745_enabled": true,
    "ltr559_enabled": true,
    "vl53l1x_enabled": true,
    "bme680_enabled": true,
    "lsm303d_enabled": true
  }
}
```

Disabled sensors are never imported, saving memory.

### Error Recovery

The `BaseSensor` class implements a graduated error recovery strategy:

1. **Per-read errors:** Logged, return None, increment counter
2. **Max errors (10):** Force re-initialization of sensor
3. **Init failure:** Sensor excluded from read loop
4. **Snowflake connection:** Auto-reconnect on 401, Slack alert on persistent failure

### Data Schema

The app writes to the shared `IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA` table. PiUPS-specific columns are identified by prefix:
- `bh1745_*` — Color sensor
- `ltr559_*` — Light/proximity sensor
- `vl53l1x_*` — Distance sensor
- `lsm303d_*` — Motion sensor

Standard columns (temperature, humidity, pressure, etc.) are shared with other device types.

### Security

- No passwords stored anywhere — PAT or RSA key-pair only
- `.gitignore` excludes all secret files (*.p8, *.pem, *.key, .env, config.json)
- systemd runs with `NoNewPrivileges=true`, `ProtectSystem=strict`
- Config file permissions should be 600 on Pi
- Snowflake role scoped to minimum required (INSERT on table, USAGE on pipe)

### Monitoring

- **Slack alerts:** Connection failures, batch streaming errors
- **Snowflake ALERT:** Data gaps > 60 seconds trigger email notification
- **System metrics:** CPU temp, CPU%, memory%, disk% included in every row
- **OLED display:** Real-time visual confirmation of sensor operation

## Data Flow

1. Sensors read every 5 seconds (background thread)
2. Latest values cached in memory (thread-safe dict)
3. Every 10 seconds: build batch of up to 5 rows from cache
4. OLED display updated with latest values
5. Batch POSTed to Snowpipe Streaming v2 REST API as NDJSON
6. Continuation token tracked for exactly-once delivery
7. Data available for query in Snowflake within ~5 seconds
8. Views provide real-time, hourly, and anomaly perspectives
