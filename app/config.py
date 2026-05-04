"""Configuration loader for PiUPS Breakout Garden IoT app.

Loads from snowflake_config.json with environment variable overrides.
Optimized for Raspberry Pi with 2GB RAM.
"""

import json
import os
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class SnowflakeConfig:
    """Snowflake connection and streaming configuration."""
    account: str = ""
    user: str = ""
    url: str = ""
    pat: str = ""
    private_key_path: str = ""
    role: str = "ACCOUNTADMIN"
    database: str = "IOT_LAB"
    schema: str = "SENSORS"
    pipe: str = "BREAKOUT_GARDEN_PIPE"
    channel_name: str = "PIUPS_CHNL"
    warehouse: str = "INGEST"


@dataclass
class SensorConfig:
    """Sensor polling configuration."""
    read_interval_seconds: float = 5.0
    batch_interval_seconds: float = 10.0
    bh1745_enabled: bool = True
    ltr559_enabled: bool = True
    vl53l1x_enabled: bool = True
    bme680_enabled: bool = True
    lsm303d_enabled: bool = True
    simulate: bool = False


@dataclass
class DisplayConfig:
    """OLED display configuration."""
    oled_enabled: bool = True
    oled_address: int = 0x3C
    oled_rotation: int = 2


@dataclass
class AppConfig:
    """Top-level application configuration."""
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig)
    sensors: SensorConfig = field(default_factory=SensorConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)
    device_id: str = ""
    slack_webhook: str = ""
    log_level: str = "INFO"


def load_config(config_path: str = None, simulate: bool = False) -> AppConfig:
    """Load configuration from JSON file with env var overrides.

    Priority: environment variables > config file > defaults.
    """
    config = AppConfig()

    if config_path is None:
        config_path = os.environ.get(
            "PIUPS_CONFIG_PATH",
            str(Path(__file__).parent.parent / "snowflake_config.json")
        )

    if os.path.isfile(config_path):
        with open(config_path, "r") as f:
            data = json.load(f)

        # Snowflake settings
        sf = data.get("snowflake", data)
        config.snowflake.account = sf.get("account", config.snowflake.account)
        config.snowflake.user = sf.get("user", config.snowflake.user)
        config.snowflake.url = sf.get("url", config.snowflake.url)
        config.snowflake.pat = sf.get("pat", config.snowflake.pat)
        config.snowflake.private_key_path = sf.get("private_key_path", config.snowflake.private_key_path)
        config.snowflake.role = sf.get("role", config.snowflake.role)
        config.snowflake.database = sf.get("database", config.snowflake.database)
        config.snowflake.schema = sf.get("schema", config.snowflake.schema)
        config.snowflake.pipe = sf.get("pipe", config.snowflake.pipe)
        config.snowflake.channel_name = sf.get("channel_name", config.snowflake.channel_name)
        config.snowflake.warehouse = sf.get("warehouse", config.snowflake.warehouse)

        # Sensor settings
        sensors = data.get("sensors", {})
        config.sensors.read_interval_seconds = sensors.get("read_interval_seconds", config.sensors.read_interval_seconds)
        config.sensors.batch_interval_seconds = sensors.get("batch_interval_seconds", config.sensors.batch_interval_seconds)
        config.sensors.bh1745_enabled = sensors.get("bh1745_enabled", config.sensors.bh1745_enabled)
        config.sensors.ltr559_enabled = sensors.get("ltr559_enabled", config.sensors.ltr559_enabled)
        config.sensors.vl53l1x_enabled = sensors.get("vl53l1x_enabled", config.sensors.vl53l1x_enabled)
        config.sensors.bme680_enabled = sensors.get("bme680_enabled", config.sensors.bme680_enabled)
        config.sensors.lsm303d_enabled = sensors.get("lsm303d_enabled", config.sensors.lsm303d_enabled)

        # Display settings
        display = data.get("display", {})
        config.display.oled_enabled = display.get("oled_enabled", config.display.oled_enabled)
        oled_addr = display.get("oled_address", None)
        if oled_addr:
            config.display.oled_address = int(oled_addr, 16) if isinstance(oled_addr, str) else oled_addr
        config.display.oled_rotation = display.get("oled_rotation", config.display.oled_rotation)

        # App settings
        config.device_id = data.get("device_id", config.device_id)
        config.slack_webhook = data.get("slack_webhook", config.slack_webhook)
        config.log_level = data.get("log_level", config.log_level)

    # Environment variable overrides (highest priority)
    config.snowflake.account = os.environ.get("SNOWFLAKE_ACCOUNT", config.snowflake.account)
    config.snowflake.user = os.environ.get("SNOWFLAKE_USER", config.snowflake.user)
    config.snowflake.url = os.environ.get("SNOWFLAKE_URL", config.snowflake.url)
    config.snowflake.pat = os.environ.get("SNOWFLAKE_PAT", config.snowflake.pat)
    config.snowflake.private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", config.snowflake.private_key_path)
    config.snowflake.role = os.environ.get("SNOWFLAKE_ROLE", config.snowflake.role)
    config.device_id = os.environ.get("DEVICE_ID", config.device_id)
    config.slack_webhook = os.environ.get("SLACK_WEBHOOK", config.slack_webhook)
    config.log_level = os.environ.get("LOG_LEVEL", config.log_level)

    # Simulation mode
    config.sensors.simulate = simulate or os.environ.get("SIMULATE", "").lower() in ("1", "true", "yes")

    return config
