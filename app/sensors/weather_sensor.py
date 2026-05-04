"""BME680 Weather / Environment sensor module.

Reads temperature, humidity, pressure, and gas resistance (air quality proxy)
from the Pimoroni BME680 breakout.
"""

import random
from typing import Optional

from app.sensors.base import BaseSensor


class WeatherSensor(BaseSensor):
    """BME680 environmental sensor via I2C (address 0x76)."""

    def __init__(self, simulate: bool = False):
        super().__init__(name="bme680_weather", i2c_address=0x76, simulate=simulate)
        self._sensor = None

    def initialize(self) -> bool:
        """Initialize BME680 with optimal oversampling and heater configuration."""
        if self.simulate:
            self._initialized = True
            return True

        try:
            import bme680

            try:
                self._sensor = bme680.BME680(bme680.I2C_ADDR_PRIMARY)
            except IOError:
                self._sensor = bme680.BME680(bme680.I2C_ADDR_SECONDARY)

            self._sensor.set_humidity_oversample(bme680.OS_2X)
            self._sensor.set_pressure_oversample(bme680.OS_4X)
            self._sensor.set_temperature_oversample(bme680.OS_8X)
            self._sensor.set_filter(bme680.FILTER_SIZE_3)
            self._sensor.set_gas_status(bme680.ENABLE_GAS_MEAS)
            self._sensor.set_gas_heater_temperature(320)
            self._sensor.set_gas_heater_duration(150)
            self._sensor.select_gas_heater_profile(0)

            self._initialized = True
            self.logger.info("BME680 initialized (I2C 0x76) - gas heater 320C/150ms")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize BME680: %s", e)
            return False

    def read(self) -> Optional[dict]:
        """Read temperature, humidity, pressure, and gas resistance."""
        if not self._sensor:
            return None

        if not self._sensor.get_sensor_data():
            self.logger.debug("BME680 sensor data not ready")
            return None

        data = self._sensor.data
        result = {
            "temperature_c": round(data.temperature, 2),
            "humidity_percent": round(data.humidity, 2),
            "pressure_hpa": round(data.pressure, 2),
            "gas_resistance_ohms": None,
        }

        if data.heat_stable:
            result["gas_resistance_ohms"] = int(data.gas_resistance)

        return result

    def _simulate_reading(self) -> dict:
        """Simulated weather data (indoor office environment)."""
        return {
            "temperature_c": round(random.uniform(20.0, 26.0), 2),
            "humidity_percent": round(random.uniform(30.0, 60.0), 2),
            "pressure_hpa": round(random.uniform(1010.0, 1025.0), 2),
            "gas_resistance_ohms": random.randint(100000, 500000),
        }
