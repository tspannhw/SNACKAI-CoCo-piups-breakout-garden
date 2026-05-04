"""LTR559 Light and Proximity sensor module.

Reads ambient light (lux) and proximity from the Pimoroni LTR559 breakout.
"""

import random
from typing import Optional

from app.sensors.base import BaseSensor


class LightSensor(BaseSensor):
    """LTR559 light/proximity sensor via I2C (address 0x23)."""

    def __init__(self, simulate: bool = False):
        super().__init__(name="ltr559_light", i2c_address=0x23, simulate=simulate)
        self._sensor = None

    def initialize(self) -> bool:
        """Initialize the LTR559 sensor."""
        if self.simulate:
            self._initialized = True
            return True

        try:
            from ltr559 import LTR559

            self._sensor = LTR559()
            self._initialized = True
            self.logger.info("LTR559 initialized (I2C 0x23)")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize LTR559: %s", e)
            return False

    def read(self) -> Optional[dict]:
        """Read ambient light (lux) and proximity."""
        if not self._sensor:
            return None

        self._sensor.update_sensor()
        lux = self._sensor.get_lux()
        prox = self._sensor.get_proximity()

        return {
            "ltr559_lux": round(float(lux), 2),
            "ltr559_proximity": int(prox),
        }

    def _simulate_reading(self) -> dict:
        """Simulated light/proximity data (indoor environment)."""
        return {
            "ltr559_lux": round(random.uniform(50.0, 500.0), 2),
            "ltr559_proximity": random.randint(0, 100),
        }
