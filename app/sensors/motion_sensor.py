"""LSM303D Accelerometer and Magnetometer sensor module.

Reads 3-axis acceleration (g) and 3-axis magnetic field
from the Pimoroni LSM303D breakout.
"""

import random
from typing import Optional

from app.sensors.base import BaseSensor


class MotionSensor(BaseSensor):
    """LSM303D accelerometer/magnetometer via I2C (address 0x1D)."""

    def __init__(self, simulate: bool = False):
        super().__init__(name="lsm303d_motion", i2c_address=0x1D, simulate=simulate)
        self._sensor = None

    def initialize(self) -> bool:
        """Initialize the LSM303D sensor."""
        if self.simulate:
            self._initialized = True
            return True

        try:
            from lsm303d import LSM303D

            self._sensor = LSM303D(0x1D)
            self._initialized = True
            self.logger.info("LSM303D initialized (I2C 0x1D)")
            return True
        except Exception as e:
            self.logger.error("Failed to initialize LSM303D: %s", e)
            return False

    def read(self) -> Optional[dict]:
        """Read accelerometer (g) and magnetometer (gauss) XYZ values."""
        if not self._sensor:
            return None

        accel = self._sensor.accelerometer()
        mag = self._sensor.magnetometer()

        return {
            "lsm303d_accel_x": round(accel[0], 4),
            "lsm303d_accel_y": round(accel[1], 4),
            "lsm303d_accel_z": round(accel[2], 4),
            "lsm303d_mag_x": round(mag[0], 2),
            "lsm303d_mag_y": round(mag[1], 2),
            "lsm303d_mag_z": round(mag[2], 2),
        }

    def _simulate_reading(self) -> dict:
        """Simulated accelerometer/magnetometer data (stationary on desk)."""
        return {
            "lsm303d_accel_x": round(random.uniform(-0.05, 0.05), 4),
            "lsm303d_accel_y": round(random.uniform(-0.05, 0.05), 4),
            "lsm303d_accel_z": round(random.uniform(0.95, 1.05), 4),
            "lsm303d_mag_x": round(random.uniform(-0.5, 0.5), 2),
            "lsm303d_mag_y": round(random.uniform(-0.5, 0.5), 2),
            "lsm303d_mag_z": round(random.uniform(-0.5, 0.5), 2),
        }
