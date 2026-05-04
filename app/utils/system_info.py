"""System information utility for Raspberry Pi.

Gathers CPU temperature, CPU usage, memory, disk, network info.
Optimized for 2GB RAM Pi - uses lazy imports to reduce baseline memory.
"""

import logging
import socket
import uuid as uuid_lib
from typing import Optional

logger = logging.getLogger(__name__)


def get_cpu_temperature() -> Optional[float]:
    """Read Raspberry Pi CPU temperature in Celsius."""
    try:
        with open("/sys/devices/virtual/thermal/thermal_zone0/temp", "r") as f:
            temp_millideg = int(f.read().strip())
            return round(temp_millideg / 1000.0, 1)
    except (FileNotFoundError, ValueError):
        return None


def get_system_metrics() -> dict:
    """Get CPU, memory, and disk usage percentages."""
    try:
        import psutil
        cpu_percent = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        return {
            "cpu_percent": round(cpu_percent, 1),
            "memory_percent": round(memory.percent, 1),
            "disk_usage_percent": round(disk.percent, 1),
        }
    except ImportError:
        return {
            "cpu_percent": None,
            "memory_percent": None,
            "disk_usage_percent": None,
        }


def get_hostname() -> str:
    """Get the system hostname."""
    return socket.gethostname()


def get_ip_address() -> str:
    """Get the primary IP address of the device."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_mac_address() -> str:
    """Get the MAC address of the primary network interface."""
    mac = uuid_lib.getnode()
    return ":".join(f"{(mac >> (8 * i)) & 0xFF:02x}" for i in reversed(range(6)))


def get_device_info() -> dict:
    """Get all device identification and system metrics."""
    cpu_temp = get_cpu_temperature()
    metrics = get_system_metrics()
    return {
        "hostname": get_hostname(),
        "ip_address": get_ip_address(),
        "mac_address": get_mac_address(),
        "cpu_temp_c": cpu_temp,
        **metrics,
    }
