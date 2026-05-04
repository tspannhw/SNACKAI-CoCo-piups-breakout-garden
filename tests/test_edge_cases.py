"""Edge case and error handling tests for PiUPS IoT app."""

import json
import os
import tempfile
from unittest.mock import patch, MagicMock

import requests as req

from app.config import load_config
from app.sensors.color_sensor import ColorSensor
from app.sensors.light_sensor import LightSensor
from app.sensors.distance_sensor import DistanceSensor
from app.sensors.weather_sensor import WeatherSensor
from app.sensors.motion_sensor import MotionSensor
from app.streaming.snowpipe_client import SnowpipeStreamingClient
from app.streaming.jwt_auth import SnowflakeAuth
from app.utils.system_info import get_cpu_temperature, get_ip_address


# =============================================================================
# Sensor Error Handling Edge Cases
# =============================================================================

class TestSensorErrorHandling:
    """Test sensor behavior under error conditions."""

    def test_uninitialized_sensor_safe_read_attempts_init(self):
        sensor = ColorSensor(simulate=True)
        reading = sensor.safe_read()
        assert reading is not None

    def test_error_counter_resets_on_success(self):
        sensor = WeatherSensor(simulate=False)
        sensor._initialized = True
        sensor._error_count = 5
        sensor._sensor = MagicMock()
        sensor._sensor.get_sensor_data.return_value = True
        sensor._sensor.data.temperature = 22.0
        sensor._sensor.data.humidity = 45.0
        sensor._sensor.data.pressure = 1013.0
        sensor._sensor.data.heat_stable = False
        sensor.safe_read()
        assert sensor._error_count == 0

    def test_max_errors_triggers_reinit(self):
        sensor = MotionSensor(simulate=False)
        sensor._initialized = True
        sensor._error_count = 9
        sensor._sensor = MagicMock()
        sensor._sensor.accelerometer.side_effect = RuntimeError("I2C bus error")

        reading = sensor.safe_read()
        assert reading is None
        assert sensor._initialized is False
        assert sensor._error_count == 0

    def test_cleanup_sets_initialized_false(self):
        sensor = WeatherSensor(simulate=True)
        sensor.initialize()
        assert sensor._initialized is True
        sensor.cleanup()
        assert sensor._initialized is False

    def test_multiple_consecutive_reads_stable(self):
        sensor = LightSensor(simulate=True)
        sensor.initialize()
        results = [sensor.safe_read() for _ in range(100)]
        assert all(r is not None for r in results)
        assert sensor._error_count == 0


class TestWeatherEdgeCases:
    """Test BME680 weather sensor edge cases."""

    def test_gas_not_ready_returns_none(self):
        sensor = WeatherSensor(simulate=False)
        sensor._initialized = True
        sensor._sensor = MagicMock()
        sensor._sensor.get_sensor_data.return_value = True
        sensor._sensor.data.temperature = 22.5
        sensor._sensor.data.humidity = 45.0
        sensor._sensor.data.pressure = 1013.25
        sensor._sensor.data.heat_stable = False
        reading = sensor.read()
        assert reading["gas_resistance_ohms"] is None
        assert reading["temperature_c"] == 22.5

    def test_sensor_data_not_ready(self):
        sensor = WeatherSensor(simulate=False)
        sensor._initialized = True
        sensor._sensor = MagicMock()
        sensor._sensor.get_sensor_data.return_value = False
        reading = sensor.read()
        assert reading is None


class TestDistanceEdgeCases:
    """Test VL53L1X distance sensor edge cases."""

    def test_cleanup_stops_ranging(self):
        sensor = DistanceSensor(simulate=False)
        sensor._sensor = MagicMock()
        sensor.cleanup()
        sensor._sensor.stop_ranging.assert_called_once()


# =============================================================================
# Streaming Client Edge Cases
# =============================================================================

class TestStreamingEdgeCases:
    """Test Snowpipe Streaming client edge cases."""

    def test_close_without_connect(self):
        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        client = SnowpipeStreamingClient(
            auth=auth, database="DB", schema="SC", pipe="P", channel_name="C"
        )
        client.close()

    def test_context_manager(self):
        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        auth.get_auth_headers.return_value = {"Authorization": "Bearer x"}

        with patch("app.streaming.snowpipe_client.requests.Session") as mock_session:
            session = MagicMock()
            mock_session.return_value = session
            session.get.return_value = MagicMock(
                json=MagicMock(return_value={"hostname": "host"}),
                raise_for_status=MagicMock()
            )
            session.put.return_value = MagicMock(
                json=MagicMock(return_value={"next_continuation_token": "t1"}),
                raise_for_status=MagicMock()
            )
            session.delete.return_value = MagicMock()

            client = SnowpipeStreamingClient(
                auth=auth, database="DB", schema="SC", pipe="P", channel_name="C"
            )
            with client:
                assert client._channel_id is not None
            assert client._channel_id is None

    @patch("app.streaming.snowpipe_client.requests.Session")
    def test_append_rows_network_error(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.side_effect = req.exceptions.ConnectionError("Network down")

        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        auth.get_auth_headers.return_value = {"Authorization": "Bearer x"}

        client = SnowpipeStreamingClient(
            auth=auth, database="DB", schema="SC", pipe="P", channel_name="C"
        )
        client._ingest_host = "host"
        client._channel_id = "CH_123"
        client._continuation_token = "tok"

        result = client.append_rows([{"foo": "bar"}])
        assert result is False

    def test_offset_token_increments(self):
        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        auth.get_auth_headers.return_value = {"Authorization": "Bearer x"}

        with patch("app.streaming.snowpipe_client.requests.Session") as mock_session_cls:
            session = MagicMock()
            mock_session_cls.return_value = session
            resp = MagicMock(
                status_code=200,
                raise_for_status=MagicMock(),
                json=MagicMock(return_value={"next_continuation_token": "t2"})
            )
            session.post.return_value = resp

            client = SnowpipeStreamingClient(
                auth=auth, database="DB", schema="SC", pipe="P", channel_name="C"
            )
            client._ingest_host = "host"
            client._channel_id = "CH_123"
            client._continuation_token = "tok"
            client._offset_token = 0

            client.append_rows([{"a": 1}, {"b": 2}, {"c": 3}])
            assert client._offset_token == 3


# =============================================================================
# Config Edge Cases
# =============================================================================

class TestConfigEdgeCases:
    """Test configuration loading edge cases."""

    def test_config_from_valid_json_file(self):
        config_data = {
            "snowflake": {
                "account": "MYACCOUNT",
                "user": "MYUSER",
                "url": "https://myaccount.snowflakecomputing.com",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
            },
            "sensors": {
                "read_interval_seconds": 2.0,
                "bh1745_enabled": False,
            },
            "device_id": "piups-test",
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            path = f.name

        try:
            config = load_config(config_path=path)
            assert config.snowflake.account == "MYACCOUNT"
            assert config.snowflake.database == "TEST_DB"
            assert config.sensors.read_interval_seconds == 2.0
            assert config.sensors.bh1745_enabled is False
            assert config.device_id == "piups-test"
        finally:
            os.unlink(path)

    @patch.dict("os.environ", {"SIMULATE": "true"})
    def test_simulate_from_env_var(self):
        config = load_config(config_path="/nonexistent")
        assert config.sensors.simulate is True

    @patch.dict("os.environ", {"SIMULATE": "0"})
    def test_simulate_env_var_false(self):
        config = load_config(config_path="/nonexistent")
        assert config.sensors.simulate is False


# =============================================================================
# System Info Edge Cases
# =============================================================================

class TestSystemInfoEdgeCases:
    """Test system info utilities on non-RPi systems."""

    def test_cpu_temp_returns_none_on_non_rpi(self):
        temp = get_cpu_temperature()
        if not os.path.exists("/sys/devices/virtual/thermal/thermal_zone0/temp"):
            assert temp is None

    def test_ip_address_is_valid(self):
        ip = get_ip_address()
        parts = ip.split(".")
        assert len(parts) == 4
        for part in parts:
            assert 0 <= int(part) <= 255
