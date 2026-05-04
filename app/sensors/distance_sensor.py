"""VL53L1X Time-of-Flight distance sensor module.

Reads distance in millimeters using IR laser ranging
from the Pimoroni VL53L1X breakout.
"""

import random
from typing import Optional

from app.sensors.base import BaseSensor

MAX_DISTANCE_MM = 800


class DistanceSensor(BaseSensor):
    """VL53L1X ToF distance sensor via I2C (address 0x29)."""

    def __init__(self, simulate: bool = False):
        super().__init__(name="vl53l1x_distance", i2c_address=0x29, simulate=simulate)
        self._sensor = None

    def initialize(self) -> bool:
        """Initialize the VL53L1X sensor."""
        if self.simulate:
            self._initialized = True
            return True

        try:
            import VL53L1X

            self._sensor = VL53L1X.VL53L1X(i2c_bus=1, i2c_address=0x29)
            self._initialized = True
            self.logger.info("VL53L1X initialized (I2C 0x29)")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize VL53L1X: %s", e)
            return False

    def read(self) -> Optional[dict]:
        """Read distance in millimeters (medium range mode)."""
        if not self._sensor:
            return None

        self._sensor.open()
        self._sensor.start_ranging(2)  # 2 = Medium Range
        distance_mm = self._sensor.get_distance()
        self._sensor.stop_ranging()

        distance_mm = min(MAX_DISTANCE_MM, distance_mm)

        return {
            "vl53l1x_distance_mm": int(distance_mm),
        }

    def _simulate_reading(self) -> dict:
        """Simulated distance data (object at various distances)."""
        return {
            "vl53l1x_distance_mm": random.randint(20, MAX_DISTANCE_MM),
        }

    def cleanup(self):
        """Stop ranging on shutdown."""
        if self._sensor:
            try:
                self._sensor.stop_ranging()
            except Exception:
                pass
        super().cleanup()
