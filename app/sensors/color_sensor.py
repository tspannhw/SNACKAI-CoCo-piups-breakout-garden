"""BH1745 RGB Color sensor module.

Reads red, green, blue, and clear light intensity values
from the Pimoroni BH1745 breakout.
"""

import random
from typing import Optional

from app.sensors.base import BaseSensor


class ColorSensor(BaseSensor):
    """BH1745 luminance/color sensor via I2C (address 0x38)."""

    def __init__(self, simulate: bool = False):
        super().__init__(name="bh1745_color", i2c_address=0x38, simulate=simulate)
        self._sensor = None

    def initialize(self) -> bool:
        """Initialize the BH1745 color sensor."""
        if self.simulate:
            self._initialized = True
            return True

        try:
            from bh1745 import BH1745

            self._sensor = BH1745()
            self._sensor.setup()
            self._initialized = True
            self.logger.info("BH1745 initialized (I2C 0x38)")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize BH1745: %s", e)
            return False

    def read(self) -> Optional[dict]:
        """Read RGBC raw color values with brief LED flash."""
        if not self._sensor:
            return None

        self._sensor.set_leds(1)
        r, g, b, c = self._sensor.get_rgbc_raw()
        self._sensor.set_leds(0)

        return {
            "bh1745_red": round(float(r), 1),
            "bh1745_green": round(float(g), 1),
            "bh1745_blue": round(float(b), 1),
            "bh1745_clear": round(float(c), 1),
        }

    def _simulate_reading(self) -> dict:
        """Simulated color sensor data (typical indoor lighting)."""
        return {
            "bh1745_red": round(random.uniform(50.0, 300.0), 1),
            "bh1745_green": round(random.uniform(80.0, 400.0), 1),
            "bh1745_blue": round(random.uniform(30.0, 200.0), 1),
            "bh1745_clear": round(random.uniform(200.0, 1000.0), 1),
        }
