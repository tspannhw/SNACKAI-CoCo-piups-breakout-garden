# PiUPS Breakout Garden IoT Sensor Streaming

Production IoT data producer for Raspberry Pi (2GB) with Pimoroni Breakout Garden sensors streaming to Snowflake via Snowpipe Streaming v2 REST API.

## Sensors

| Sensor | I2C Address | Measurements |
|--------|-------------|-------------|
| BH1745 | 0x38 | RGB color (red, green, blue, clear) |
| LTR559 | 0x23 | Ambient light (lux), Proximity |
| VL53L1X | 0x29 | Time-of-flight distance (mm) |
| BME680 | 0x76 | Temperature, Humidity, Pressure, Gas resistance |
| LSM303D | 0x1D | Accelerometer XYZ (g), Magnetometer XYZ |
| SH1106 OLED | 0x3C | 128x128 display output |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Raspberry Pi (2GB RAM)                                          │
│                                                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  BH1745  │  │  LTR559  │  │  VL53L1X │  │  BME680  │       │
│  │  Color   │  │  Light   │  │ Distance │  │ Weather  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
│       │              │              │              │              │
│       └──────────────┴──────────────┴──────────────┘              │
│                          I2C Bus                                   │
│                             │                                      │
│  ┌──────────┐         ┌────┴────────┐         ┌──────────┐       │
│  │  LSM303D │         │  app/main   │         │  SH1106  │       │
│  │  Motion  │────────▶│ Orchestrator│────────▶│   OLED   │       │
│  └──────────┘         └──────┬──────┘         └──────────┘       │
│                              │                                     │
│                    ┌─────────┴─────────┐                          │
│                    │  Snowpipe Stream  │                          │
│                    │  v2 REST Client   │                          │
│                    └─────────┬─────────┘                          │
└──────────────────────────────┼────────────────────────────────────┘
                               │ HTTPS (JWT/PAT)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  Snowflake                                                        │
│                                                                   │
│  IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA                            │
│       │                                                           │
│       ├── V_PIUPS_LATEST (real-time per-device)                  │
│       ├── V_PIUPS_HOURLY (aggregates)                            │
│       ├── V_PIUPS_ANOMALIES (rule-based detection)               │
│       └── PIUPS_DATA_GAP_ALERT (5-min monitoring)                │
│                                                                   │
│  Streamlit Dashboard (IOT_LAB.SENSORS.PIUPS_DASHBOARD)           │
└──────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Clone and Build

```bash
git clone <this-repo>
cd piups
./manage.sh build
```

### 2. Configure

```bash
cp snowflake_config.json.example snowflake_config.json
# Edit with your Snowflake credentials (PAT or key-pair path)
```

### 3. Deploy Snowflake Objects

```bash
# Run setup_snowflake.sql in Snowflake to add columns and create pipe/views
```

### 4. Run

```bash
# With hardware
./manage.sh run

# Simulation mode (no Pi sensors needed)
./manage.sh run-simulate

# Local dashboard
./manage.sh run-dashboard
```

### 5. Deploy as Service (auto-start on boot)

```bash
./scripts/deploy.sh
```

## Authentication

The app supports two methods (configure in `snowflake_config.json`):

| Method | Config Field | Best For |
|--------|-------------|----------|
| PAT | `snowflake.pat` | Development, quick testing |
| RSA Key-pair | `snowflake.private_key_path` | Production (auto-refreshing JWT) |

Environment variables override config file:
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_URL`
- `SNOWFLAKE_PAT` or `SNOWFLAKE_PRIVATE_KEY_PATH`
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` (if key is encrypted)
- `DEVICE_ID`, `SLACK_WEBHOOK`, `LOG_LEVEL`, `SIMULATE`

## Memory Optimization (2GB Pi)

- Batch size capped at 5 rows per cycle
- `gc.collect()` after each batch
- Lazy sensor library imports (only load when sensor enabled)
- systemd MemoryMax=256M hard limit
- No heavy frameworks (pure REST, no Snowflake Python connector)

## Testing

```bash
./manage.sh test     # All tests (simulation mode)
./manage.sh lint     # flake8 checks
```

## Project Structure

```
piups/
├── AGENTS.md                      # CoCo agent instructions
├── README.md                      # This file
├── manage.sh                      # Build/test/run commands
├── requirements.txt               # Python dependencies
├── snowflake_config.json.example  # Config template
├── setup_snowflake.sql            # Snowflake DDL
├── app/
│   ├── __init__.py
│   ├── config.py                  # Dataclass config with env overrides
│   ├── main.py                    # Main orchestrator
│   ├── sensors/
│   │   ├── base.py                # Abstract sensor base class
│   │   ├── color_sensor.py        # BH1745
│   │   ├── light_sensor.py        # LTR559
│   │   ├── distance_sensor.py     # VL53L1X
│   │   ├── weather_sensor.py      # BME680
│   │   └── motion_sensor.py       # LSM303D
│   ├── display/
│   │   └── oled_display.py        # SH1106 OLED renderer
│   ├── streaming/
│   │   ├── jwt_auth.py            # PAT + RSA key-pair auth
│   │   └── snowpipe_client.py     # Snowpipe Streaming v2 REST
│   └── utils/
│       └── system_info.py         # CPU, memory, disk, network
├── dashboard/
│   └── dashboard.py               # Streamlit local dashboard
├── tests/
│   ├── test_sensors.py            # Simulation mode tests
│   └── test_edge_cases.py         # Error handling tests
├── scripts/
│   ├── deploy.sh                  # systemd deployment
│   └── piups-sensor.service       # systemd unit file
└── docs/
    └── architecture.md            # Detailed architecture
```

## Snowflake Objects Created

- `IOT_LAB.SENSORS.BREAKOUT_GARDEN_DATA` — Main table (extended with piups columns)
- `IOT_LAB.SENSORS.BREAKOUT_GARDEN_PIPE` — Streaming pipe
- `IOT_LAB.SENSORS.V_PIUPS_LATEST` — Latest reading per device
- `IOT_LAB.SENSORS.V_PIUPS_HOURLY` — Hourly aggregates
- `IOT_LAB.SENSORS.V_PIUPS_ANOMALIES` — Rule-based anomaly detection
- `IOT_LAB.SENSORS.PIUPS_DATA_GAP_ALERT` — 5-minute gap monitoring

## Cortex Code Activities

Extend this project with:
- `Build a Cortex Agent that answers questions about sensor data using the semantic view`
- `Create a semantic view for BREAKOUT_GARDEN_DATA with synonyms and examples`
- `Add Cortex AI anomaly detection using ML functions on the hourly aggregates`
- `Create alerting stored procedures that send Slack notifications on anomalies`
- `Build a React app that visualizes real-time sensor data from Snowflake`

## References

- [Snowpipe Streaming v2 REST API](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api)
- [Pimoroni Breakout Garden](https://shop.pimoroni.com/collections/breakout-garden)
- [Snowflake Well-Architected Framework](https://www.snowflake.com/en/product/use-cases/well-architected-framework/)
