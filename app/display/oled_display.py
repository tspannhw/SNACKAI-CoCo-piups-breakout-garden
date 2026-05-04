"""SH1106 128x128 OLED display module.

Renders sensor readings on the Pimoroni 1.12" OLED breakout.
Uses luma.oled for rendering with minimal memory footprint.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class OLEDDisplay:
    """SH1106 OLED display (128x128) via I2C."""

    def __init__(self, address: int = 0x3C, rotation: int = 2, enabled: bool = True):
        self.address = address
        self.rotation = rotation
        self.enabled = enabled
        self._device = None
        self._initialized = False

    def initialize(self) -> bool:
        """Initialize the OLED display."""
        if not self.enabled:
            logger.info("OLED display disabled in config")
            return True

        try:
            from luma.core.interface.serial import i2c
            from luma.oled.device import sh1106

            serial = i2c(port=1, address=self.address)
            self._device = sh1106(serial, rotate=self.rotation, height=128, width=128)
            self._device.cleanup = lambda obj: None  # Prevent cleanup on GC
            self._initialized = True
            logger.info("OLED SH1106 initialized (I2C 0x%02X, rotate=%d)", self.address, self.rotation)
            return True
        except Exception as e:
            logger.error("Failed to initialize OLED: %s", e)
            return False

    def update(self, sensor_data: dict, device_info: dict) -> None:
        """Render sensor readings on the OLED display."""
        if not self._initialized or not self._device:
            return

        try:
            from luma.core.render import canvas

            ip = device_info.get("ip_address", "no-ip")
            temp = sensor_data.get("temperature_c", "--")
            humidity = sensor_data.get("humidity_percent", "--")
            pressure = sensor_data.get("pressure_hpa", "--")
            distance = sensor_data.get("vl53l1x_distance_mm", "--")
            lux = sensor_data.get("ltr559_lux", "--")
            cpu_temp = device_info.get("cpu_temp_c", "--")
            accel_x = sensor_data.get("lsm303d_accel_x", "--")
            accel_y = sensor_data.get("lsm303d_accel_y", "--")
            accel_z = sensor_data.get("lsm303d_accel_z", "--")
            mem = device_info.get("memory_percent", "--")

            with canvas(self._device) as draw:
                draw.rectangle(self._device.bounding_box, outline="white", fill="black")
                draw.text((2, 0), "PiUPS Sensor", fill="white")
                draw.text((2, 12), f"IP: {ip}", fill="white")
                draw.text((2, 24), f"Temp: {temp} C", fill="white")
                draw.text((2, 36), f"Hum:  {humidity} %", fill="white")
                draw.text((2, 48), f"Press:{pressure} hPa", fill="white")
                draw.text((2, 60), f"Dist: {distance} mm", fill="white")
                draw.text((2, 72), f"Lux:  {lux}", fill="white")
                draw.text((2, 84), f"CPU:  {cpu_temp} C", fill="white")
                draw.text((2, 96), f"A: {accel_x}/{accel_y}/{accel_z}", fill="white")
                draw.text((2, 108), f"Mem:  {mem} %", fill="white")

        except Exception as e:
            logger.error("OLED render error: %s", e)

    def clear(self) -> None:
        """Clear the display on shutdown."""
        if self._device:
            try:
                from luma.core.render import canvas
                with canvas(self._device) as draw:
                    draw.rectangle(self._device.bounding_box, outline="black", fill="black")
            except Exception:
                pass

    def cleanup(self) -> None:
        """Clean up display resources."""
        self.clear()
        self._initialized = False
        logger.info("OLED display cleaned up")
