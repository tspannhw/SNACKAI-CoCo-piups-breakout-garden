"""Main entry point for PiUPS Breakout Garden IoT Sensor Streaming App.

Reads 5 I2C sensors (BH1745, LTR559, VL53L1X, BME680, LSM303D) in a
background thread, displays on OLED, batches readings, and streams to
Snowflake via Snowpipe Streaming v2 REST API.

Usage:
    python -m app.main                    # Normal mode (requires hardware)
    python -m app.main --simulate         # Simulation mode (no hardware)
    python -m app.main --config path.json # Custom config file
"""

import argparse
import gc
import json
import logging
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import requests

from app.config import load_config, AppConfig
from app.display.oled_display import OLEDDisplay
from app.sensors.color_sensor import ColorSensor
from app.sensors.distance_sensor import DistanceSensor
from app.sensors.light_sensor import LightSensor
from app.sensors.motion_sensor import MotionSensor
from app.sensors.weather_sensor import WeatherSensor
from app.streaming.jwt_auth import SnowflakeAuth
from app.streaming.snowpipe_client import SnowpipeStreamingClient
from app.utils.system_info import get_device_info

logger = logging.getLogger("piups")

# Memory-conscious batch size for 2GB Pi
MAX_BATCH_SIZE = 5


class PiUPSApp:
    """Main application orchestrating sensor reads and Snowflake streaming."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._running = False
        self._sensor_cache = {}
        self._cache_lock = threading.Lock()
        self._sensors = []
        self._display = None

    def _init_sensors(self):
        """Initialize enabled sensors."""
        sim = self.config.sensors.simulate

        if self.config.sensors.bh1745_enabled:
            sensor = ColorSensor(simulate=sim)
            if sensor.initialize():
                self._sensors.append(sensor)
            else:
                logger.warning("BH1745 color sensor failed to initialize")

        if self.config.sensors.ltr559_enabled:
            sensor = LightSensor(simulate=sim)
            if sensor.initialize():
                self._sensors.append(sensor)
            else:
                logger.warning("LTR559 light sensor failed to initialize")

        if self.config.sensors.vl53l1x_enabled:
            sensor = DistanceSensor(simulate=sim)
            if sensor.initialize():
                self._sensors.append(sensor)
            else:
                logger.warning("VL53L1X distance sensor failed to initialize")

        if self.config.sensors.bme680_enabled:
            sensor = WeatherSensor(simulate=sim)
            if sensor.initialize():
                self._sensors.append(sensor)
            else:
                logger.warning("BME680 weather sensor failed to initialize")

        if self.config.sensors.lsm303d_enabled:
            sensor = MotionSensor(simulate=sim)
            if sensor.initialize():
                self._sensors.append(sensor)
            else:
                logger.warning("LSM303D motion sensor failed to initialize")

        logger.info("Initialized %d/%d sensors (simulate=%s)",
                    len(self._sensors), 5, sim)

    def _init_display(self):
        """Initialize the OLED display."""
        self._display = OLEDDisplay(
            address=self.config.display.oled_address,
            rotation=self.config.display.oled_rotation,
            enabled=self.config.display.oled_enabled,
        )
        if not self._display.initialize():
            logger.warning("OLED display failed to initialize, continuing without display")

    def _sensor_read_loop(self):
        """Background thread: continuously read sensors into cache."""
        while self._running:
            readings = {}
            for sensor in self._sensors:
                data = sensor.safe_read()
                if data:
                    readings.update(data)

            with self._cache_lock:
                self._sensor_cache = readings.copy()

            time.sleep(self.config.sensors.read_interval_seconds)

    def _build_row(self) -> dict:
        """Build a single data row from cached sensor readings + system info."""
        with self._cache_lock:
            sensor_data = self._sensor_cache.copy()

        device_info = get_device_info()

        row = {
            "uuid": str(uuid.uuid4()),
            "device_id": self.config.device_id or device_info["hostname"],
            "hostname": device_info["hostname"],
            "ip_address": device_info["ip_address"],
            "mac_address": device_info["mac_address"],
            "reading_ts": datetime.now(timezone.utc).isoformat(),
            "cpu_temp_c": device_info["cpu_temp_c"],
            "cpu_percent": device_info["cpu_percent"],
            "memory_percent": device_info["memory_percent"],
            "disk_usage_percent": device_info["disk_usage_percent"],
            **sensor_data,
        }

        row["raw_data"] = json.dumps(row)
        return row

    def _update_display(self, sensor_data: dict, device_info: dict):
        """Update OLED display with latest readings."""
        if self._display:
            self._display.update(sensor_data, device_info)

    def _send_slack_alert(self, message: str):
        """Send alert to Slack edge-alerts channel."""
        if not self.config.slack_webhook:
            return
        try:
            payload = {"text": f":rotating_light: *PiUPS Alert*\n{message}"}
            requests.post(self.config.slack_webhook, json=payload, timeout=5)
        except Exception as e:
            logger.error("Slack alert failed: %s", e)

    def run(self):
        """Main execution loop."""
        logger.info("Starting PiUPS Breakout Garden IoT Streaming App...")
        logger.info("Device: %s | Simulate: %s | Batch interval: %ss",
                    self.config.device_id, self.config.sensors.simulate,
                    self.config.sensors.batch_interval_seconds)

        # Initialize sensors
        self._init_sensors()
        if not self._sensors:
            logger.error("No sensors initialized. Exiting.")
            sys.exit(1)

        # Initialize display
        self._init_display()

        # Initialize Snowpipe Streaming client
        sf = self.config.snowflake
        auth_method = "PAT" if sf.pat else f"Key-pair ({sf.private_key_path})"
        logger.info("Snowflake: account=%s user=%s url=%s auth=%s",
                    sf.account, sf.user, sf.url, auth_method)
        auth = SnowflakeAuth(
            account=sf.account,
            user=sf.user,
            url=sf.url,
            pat=sf.pat,
            private_key_path=sf.private_key_path,
            role=sf.role,
        )

        client = SnowpipeStreamingClient(
            auth=auth,
            database=sf.database,
            schema=sf.schema,
            pipe=sf.pipe,
            channel_name=sf.channel_name,
        )

        # Start sensor reading thread
        self._running = True
        sensor_thread = threading.Thread(target=self._sensor_read_loop, daemon=True)
        sensor_thread.start()

        # Wait for first sensor readings
        time.sleep(self.config.sensors.read_interval_seconds + 1)

        # Connect to Snowflake streaming
        try:
            client.connect()
            logger.info("Connected to Snowpipe Streaming v2")
        except Exception as e:
            logger.error("Failed to connect to Snowflake: %s", e)
            self._send_slack_alert(f"Failed to connect: {e}")
            sys.exit(1)

        # Main batch loop
        batch_count = 0
        row_count = 0
        try:
            while self._running:
                batch = []
                readings_per_batch = min(MAX_BATCH_SIZE, max(1, int(
                    self.config.sensors.batch_interval_seconds
                    / self.config.sensors.read_interval_seconds
                )))

                for _ in range(readings_per_batch):
                    if not self._running:
                        break
                    row = self._build_row()
                    batch.append(row)

                    # Update OLED with latest data
                    with self._cache_lock:
                        display_data = self._sensor_cache.copy()
                    device_info = get_device_info()
                    self._update_display(display_data, device_info)

                    time.sleep(self.config.sensors.read_interval_seconds)

                if batch and self._running:
                    success = client.append_rows(batch)
                    if success:
                        batch_count += 1
                        row_count += len(batch)
                        if batch_count % 10 == 0:
                            logger.info("Streamed %d batches (%d rows total)", batch_count, row_count)
                    else:
                        logger.error("Failed to stream batch %d", batch_count + 1)
                        self._send_slack_alert(
                            f"Batch {batch_count + 1} failed ({len(batch)} rows lost)"
                        )

                # Memory management for 2GB Pi
                gc.collect()

        except KeyboardInterrupt:
            logger.info("Shutdown requested (Ctrl+C)")
        finally:
            self._running = False
            logger.info("Shutting down... (streamed %d batches, %d rows)", batch_count, row_count)
            client.close()
            for sensor in self._sensors:
                sensor.cleanup()
            if self._display:
                self._display.cleanup()
            logger.info("Goodbye.")


def setup_logging(level: str):
    """Configure logging format and level."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    """Parse arguments and run the app."""
    parser = argparse.ArgumentParser(
        description="PiUPS Breakout Garden IoT Sensor -> Snowflake Streaming"
    )
    parser.add_argument("--config", type=str, default=None,
                        help="Path to snowflake_config.json")
    parser.add_argument("--simulate", action="store_true",
                        help="Run in simulation mode (no hardware required)")
    parser.add_argument("--log-level", type=str, default=None,
                        help="Log level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    config = load_config(config_path=args.config, simulate=args.simulate)
    if args.log_level:
        config.log_level = args.log_level

    setup_logging(config.log_level)

    app = PiUPSApp(config)

    def signal_handler(signum, frame):
        logger.info("Signal %s received, stopping...", signum)
        app._running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    app.run()


if __name__ == "__main__":
    main()
