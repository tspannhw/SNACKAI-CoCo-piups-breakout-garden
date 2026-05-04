"""Base sensor abstract class for Breakout Garden I2C sensors."""

import logging
from abc import ABC, abstractmethod
from typing import Optional


class BaseSensor(ABC):
    """Abstract base class for all Breakout Garden I2C sensors.

    All sensors follow the same lifecycle:
      1. initialize() - setup I2C, configure the sensor
      2. read() - return current readings as a dict
      3. cleanup() - graceful shutdown
    """

    def __init__(self, name: str, i2c_address: int, simulate: bool = False):
        self.name = name
        self.i2c_address = i2c_address
        self.simulate = simulate
        self.logger = logging.getLogger(f"sensor.{name}")
        self._initialized = False
        self._error_count = 0
        self._max_errors = 10

    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the sensor hardware.

        Returns:
            True if initialization succeeded, False otherwise.
        """

    @abstractmethod
    def read(self) -> Optional[dict]:
        """Read current sensor values.

        Returns:
            Dictionary of sensor readings, or None on failure.
        """

    @abstractmethod
    def _simulate_reading(self) -> dict:
        """Generate simulated sensor data for testing without hardware."""

    def safe_read(self) -> Optional[dict]:
        """Read with error handling and simulation fallback."""
        if self.simulate:
            return self._simulate_reading()

        if not self._initialized:
            self.logger.warning("Sensor %s not initialized, attempting init...", self.name)
            if not self.initialize():
                return None

        try:
            reading = self.read()
            self._error_count = 0
            return reading
        except Exception as e:
            self._error_count += 1
            self.logger.error(
                "Sensor %s read error (%d/%d): %s",
                self.name, self._error_count, self._max_errors, e
            )
            if self._error_count >= self._max_errors:
                self.logger.critical("Sensor %s exceeded max errors, reinitializing...", self.name)
                self._initialized = False
                self._error_count = 0
            return None

    def cleanup(self):
        """Gracefully shut down the sensor. Override if needed."""
        self._initialized = False
        self.logger.info("Sensor %s cleaned up", self.name)
