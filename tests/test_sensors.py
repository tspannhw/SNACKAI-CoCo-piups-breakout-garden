"""Unit tests for PiUPS Breakout Garden IoT sensor modules and streaming client."""

import json
from unittest.mock import patch, MagicMock

from app.config import load_config
from app.sensors.color_sensor import ColorSensor
from app.sensors.light_sensor import LightSensor
from app.sensors.distance_sensor import DistanceSensor
from app.sensors.weather_sensor import WeatherSensor
from app.sensors.motion_sensor import MotionSensor
from app.utils.system_info import get_hostname, get_mac_address, get_device_info


# =============================================================================
# Sensor Simulation Tests
# =============================================================================

class TestColorSensorSimulation:
    """Test BH1745 color sensor in simulation mode."""

    def test_initialize(self):
        sensor = ColorSensor(simulate=True)
        assert sensor.initialize() is True

    def test_safe_read_returns_dict(self):
        sensor = ColorSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert reading is not None
        assert "bh1745_red" in reading
        assert "bh1745_green" in reading
        assert "bh1745_blue" in reading
        assert "bh1745_clear" in reading

    def test_values_are_positive(self):
        sensor = ColorSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert reading["bh1745_red"] >= 0
        assert reading["bh1745_green"] >= 0
        assert reading["bh1745_blue"] >= 0
        assert reading["bh1745_clear"] >= 0


class TestLightSensorSimulation:
    """Test LTR559 light/proximity sensor in simulation mode."""

    def test_initialize(self):
        sensor = LightSensor(simulate=True)
        assert sensor.initialize() is True

    def test_safe_read_returns_dict(self):
        sensor = LightSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert reading is not None
        assert "ltr559_lux" in reading
        assert "ltr559_proximity" in reading

    def test_lux_range(self):
        sensor = LightSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert 0 <= reading["ltr559_lux"] <= 1000

    def test_proximity_range(self):
        sensor = LightSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert 0 <= reading["ltr559_proximity"] <= 65535


class TestDistanceSensorSimulation:
    """Test VL53L1X distance sensor in simulation mode."""

    def test_initialize(self):
        sensor = DistanceSensor(simulate=True)
        assert sensor.initialize() is True

    def test_safe_read_returns_dict(self):
        sensor = DistanceSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert reading is not None
        assert "vl53l1x_distance_mm" in reading

    def test_distance_range(self):
        sensor = DistanceSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert 0 <= reading["vl53l1x_distance_mm"] <= 800


class TestWeatherSensorSimulation:
    """Test BME680 weather sensor in simulation mode."""

    def test_initialize(self):
        sensor = WeatherSensor(simulate=True)
        assert sensor.initialize() is True

    def test_safe_read_returns_dict(self):
        sensor = WeatherSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert reading is not None
        assert "temperature_c" in reading
        assert "humidity_percent" in reading
        assert "pressure_hpa" in reading
        assert "gas_resistance_ohms" in reading

    def test_temperature_range(self):
        sensor = WeatherSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert 15 < reading["temperature_c"] < 35

    def test_pressure_range(self):
        sensor = WeatherSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert 900 < reading["pressure_hpa"] < 1100


class TestMotionSensorSimulation:
    """Test LSM303D accelerometer/magnetometer in simulation mode."""

    def test_initialize(self):
        sensor = MotionSensor(simulate=True)
        assert sensor.initialize() is True

    def test_safe_read_returns_dict(self):
        sensor = MotionSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert reading is not None
        assert "lsm303d_accel_x" in reading
        assert "lsm303d_accel_y" in reading
        assert "lsm303d_accel_z" in reading
        assert "lsm303d_mag_x" in reading
        assert "lsm303d_mag_y" in reading
        assert "lsm303d_mag_z" in reading

    def test_accel_z_near_1g(self):
        """Z axis should be near 1g when stationary (simulated)."""
        sensor = MotionSensor(simulate=True)
        sensor.initialize()
        reading = sensor.safe_read()
        assert 0.8 < reading["lsm303d_accel_z"] < 1.2


# =============================================================================
# System Info Tests
# =============================================================================

class TestSystemInfo:
    """Test system information utilities."""

    def test_hostname_not_empty(self):
        assert len(get_hostname()) > 0

    def test_mac_address_format(self):
        mac = get_mac_address()
        assert len(mac) == 17
        assert mac.count(":") == 5

    def test_device_info_keys(self):
        info = get_device_info()
        assert "hostname" in info
        assert "ip_address" in info
        assert "mac_address" in info
        assert "cpu_percent" in info
        assert "memory_percent" in info


# =============================================================================
# Config Tests
# =============================================================================

class TestConfig:
    """Test configuration loading."""

    def test_load_default_config(self):
        config = load_config(config_path="/nonexistent/path.json")
        assert config.snowflake.database == "IOT_LAB"
        assert config.snowflake.schema == "SENSORS"
        assert config.sensors.read_interval_seconds == 5.0

    def test_simulate_flag(self):
        config = load_config(config_path="/nonexistent/path.json", simulate=True)
        assert config.sensors.simulate is True

    @patch.dict("os.environ", {"SNOWFLAKE_ACCOUNT": "test_account"})
    def test_env_override(self):
        config = load_config(config_path="/nonexistent/path.json")
        assert config.snowflake.account == "test_account"

    @patch.dict("os.environ", {"DEVICE_ID": "my-piups"})
    def test_device_id_override(self):
        config = load_config(config_path="/nonexistent/path.json")
        assert config.device_id == "my-piups"

    def test_all_sensors_enabled_by_default(self):
        config = load_config(config_path="/nonexistent/path.json")
        assert config.sensors.bh1745_enabled is True
        assert config.sensors.ltr559_enabled is True
        assert config.sensors.vl53l1x_enabled is True
        assert config.sensors.bme680_enabled is True
        assert config.sensors.lsm303d_enabled is True


# =============================================================================
# Streaming Client Tests (mocked HTTP)
# =============================================================================

class TestSnowpipeStreamingClient:
    """Test Snowpipe Streaming client with mocked HTTP."""

    @patch("app.streaming.snowpipe_client.requests.Session")
    def test_connect_discovers_host(self, mock_session_cls):
        from app.streaming.snowpipe_client import SnowpipeStreamingClient
        from app.streaming.jwt_auth import SnowflakeAuth

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        discover_resp = MagicMock()
        discover_resp.json.return_value = {"hostname": "ingest.snowflake.com"}
        discover_resp.raise_for_status = MagicMock()

        channel_resp = MagicMock()
        channel_resp.json.return_value = {"next_continuation_token": "tok123"}
        channel_resp.raise_for_status = MagicMock()

        mock_session.get.return_value = discover_resp
        mock_session.put.return_value = channel_resp

        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        auth.get_auth_headers.return_value = {"Authorization": "Bearer test"}

        client = SnowpipeStreamingClient(
            auth=auth, database="IOT_LAB", schema="SENSORS",
            pipe="BREAKOUT_GARDEN_PIPE", channel_name="TEST",
        )
        client.connect()

        assert client._ingest_host == "ingest.snowflake.com"
        assert client._channel_id is not None
        assert client._continuation_token == "tok123"

    @patch("app.streaming.snowpipe_client.requests.Session")
    def test_append_rows_success(self, mock_session_cls):
        from app.streaming.snowpipe_client import SnowpipeStreamingClient
        from app.streaming.jwt_auth import SnowflakeAuth

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.raise_for_status = MagicMock()
        post_resp.json.return_value = {"next_continuation_token": "tok456"}
        mock_session.post.return_value = post_resp

        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        auth.get_auth_headers.return_value = {"Authorization": "Bearer test"}

        client = SnowpipeStreamingClient(
            auth=auth, database="IOT_LAB", schema="SENSORS",
            pipe="BREAKOUT_GARDEN_PIPE", channel_name="TEST",
        )
        client._ingest_host = "ingest.snowflake.com"
        client._channel_id = "TEST_20240101"
        client._continuation_token = "tok123"

        rows = [{"uuid": "test-1", "temperature_c": 22.5}]
        result = client.append_rows(rows)
        assert result is True
        assert client._continuation_token == "tok456"

    def test_append_empty_rows(self):
        from app.streaming.snowpipe_client import SnowpipeStreamingClient
        from app.streaming.jwt_auth import SnowflakeAuth

        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"

        client = SnowpipeStreamingClient(
            auth=auth, database="IOT_LAB", schema="SENSORS",
            pipe="BREAKOUT_GARDEN_PIPE", channel_name="TEST",
        )
        result = client.append_rows([])
        assert result is True

    def test_full_pipe_name_uppercase(self):
        from app.streaming.snowpipe_client import SnowpipeStreamingClient
        from app.streaming.jwt_auth import SnowflakeAuth

        auth = MagicMock(spec=SnowflakeAuth)
        auth.url = "https://account.snowflakecomputing.com"
        client = SnowpipeStreamingClient(
            auth=auth, database="iot_lab", schema="sensors",
            pipe="my_pipe", channel_name="ch"
        )
        assert client._full_pipe_name == "IOT_LAB.SENSORS.MY_PIPE"


# =============================================================================
# Integration: full sensor -> row building
# =============================================================================

class TestEndToEnd:
    """Test the full pipeline in simulation mode."""

    def test_build_row_with_all_sensors(self):
        from app.main import PiUPSApp

        config = load_config(config_path="/nonexistent", simulate=True)
        config.device_id = "test-piups"
        app = PiUPSApp(config)
        app._init_sensors()

        for sensor in app._sensors:
            data = sensor.safe_read()
            if data:
                app._sensor_cache.update(data)

        row = app._build_row()

        assert row["device_id"] == "test-piups"
        assert "uuid" in row
        assert "reading_ts" in row
        assert "bh1745_red" in row
        assert "ltr559_lux" in row
        assert "vl53l1x_distance_mm" in row
        assert "temperature_c" in row
        assert "lsm303d_accel_x" in row
        assert "raw_data" in row

        raw = json.loads(row["raw_data"])
        assert raw["device_id"] == "test-piups"

    def test_row_numeric_types(self):
        """All numeric fields should be int/float, not strings."""
        from app.main import PiUPSApp

        config = load_config(config_path="/nonexistent", simulate=True)
        app = PiUPSApp(config)
        app._init_sensors()

        for sensor in app._sensors:
            data = sensor.safe_read()
            if data:
                app._sensor_cache.update(data)

        row = app._build_row()

        numeric_fields = [
            "bh1745_red", "bh1745_green", "bh1745_blue", "bh1745_clear",
            "ltr559_lux", "ltr559_proximity",
            "vl53l1x_distance_mm",
            "temperature_c", "humidity_percent", "pressure_hpa",
            "lsm303d_accel_x", "lsm303d_accel_y", "lsm303d_accel_z",
            "lsm303d_mag_x", "lsm303d_mag_y", "lsm303d_mag_z",
        ]
        for field in numeric_fields:
            val = row.get(field)
            if val is not None:
                assert isinstance(val, (int, float)), \
                    f"{field}={val} is {type(val)}, expected int/float"
