"""
Safety and error handling utilities for disk benchmarking tool.
"""

import logging
import signal
import sys
import psutil
import time
from typing import Dict, Any, Optional, List
from pathlib import Path

from utils import get_memory_info, run_command, CommandExecutionError


logger = logging.getLogger(__name__)


class SafetyManager:
    """Manages safety checks and error handling for disk benchmarking."""

    def __init__(self, config=None):
        self.safety_checks: Dict[str, bool] = {}
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.config = config

    def check_disk_space(
        self, mount_point: str, required_mb: Optional[int] = None
    ) -> bool:
        """Check if there's enough disk space for benchmarking."""
        # Use config value if available, otherwise use provided value or default
        if required_mb is None:
            required_mb = self.config.min_free_space_mb if self.config else 2048

        try:
            statvfs = psutil.disk_usage(mount_point)
            free_mb = statvfs.free / (1024 * 1024)

            if free_mb < required_mb:
                self.errors.append(
                    f"Insufficient disk space: {free_mb:.0f} MB available, {required_mb} MB required"
                )
                return False

            self.safety_checks["disk_space"] = True
            logger.info(f"Disk space check passed: {free_mb:.0f} MB available")
            return True

        except Exception as e:
            self.errors.append(f"Could not check disk space: {e}")
            return False

    def check_memory_usage(self, ramdisk_size_mb: int = 0) -> bool:
        """Check if there's enough memory for benchmarking."""
        try:
            memory_info = get_memory_info()

            if "MemAvailable" not in memory_info:
                self.warnings.append("Could not determine available memory")
                return True  # Continue with warning

            available_mb = memory_info["MemAvailable"] / 1024

            # Check if we have enough memory for RAM disk
            if ramdisk_size_mb > 0:
                if available_mb < ramdisk_size_mb * 1.5:  # 50% safety margin
                    self.errors.append(
                        f"Insufficient memory for RAM disk: {available_mb:.0f} MB available, {ramdisk_size_mb} MB required"
                    )
                    return False

                logger.info(
                    f"Memory check passed for RAM disk: {available_mb:.0f} MB available"
                )

            # Check general memory pressure
            if available_mb < 512:  # Less than 512 MB
                self.warnings.append(
                    f"Low memory warning: {available_mb:.0f} MB available"
                )

            self.safety_checks["memory_usage"] = True
            return True

        except Exception as e:
            self.errors.append(f"Could not check memory usage: {e}")
            return False

    def check_cpu_usage(self, threshold_percent: Optional[float] = None) -> bool:
        """Check if CPU usage is reasonable for benchmarking."""
        # Use config value if available, otherwise use provided value or default
        if threshold_percent is None:
            threshold_percent = self.config.max_cpu_threshold if self.config else 80.0

        try:
            # Get CPU usage over 1 second
            cpu_percent = psutil.cpu_percent(interval=1)

            if cpu_percent > threshold_percent:
                self.warnings.append(
                    f"High CPU usage: {cpu_percent:.1f}% (may affect benchmark results)"
                )

            self.safety_checks["cpu_usage"] = True
            logger.info(f"CPU usage check: {cpu_percent:.1f}%")
            return True

        except Exception as e:
            self.warnings.append(f"Could not check CPU usage: {e}")
            return True  # Continue with warning

    def check_write_permissions(self, mount_point: str) -> bool:
        """Check if we have write permissions to the mount point."""
        try:
            test_file = Path(mount_point) / ".benchmark_write_test"

            # Try to create a test file
            test_file.write_text("test")
            test_file.unlink()

            self.safety_checks["write_permissions"] = True
            logger.info(f"Write permissions check passed for {mount_point}")
            return True

        except Exception as e:
            self.errors.append(f"No write permissions to {mount_point}: {e}")
            return False

    def check_system_load(self, threshold: Optional[float] = None) -> bool:
        """Check system load average."""
        # Use config value if available, otherwise use provided value or default
        if threshold is None:
            threshold = self.config.max_load_threshold if self.config else 2.0

        try:
            load_avg = psutil.getloadavg()[0]  # 1-minute load average

            if load_avg > threshold:
                self.warnings.append(
                    f"High system load: {load_avg:.2f} (may affect benchmark results)"
                )

            self.safety_checks["system_load"] = True
            logger.info(f"System load check: {load_avg:.2f}")
            return True

        except Exception as e:
            self.warnings.append(f"Could not check system load: {e}")
            return True  # Continue with warning

    def check_benchmark_dependencies(self, required_tools: List[str]) -> bool:
        """Check if required benchmark tools are available."""
        missing_tools = []

        for tool in required_tools:
            try:
                run_command(["which", tool], timeout=5)
            except CommandExecutionError:
                missing_tools.append(tool)

        if missing_tools:
            self.errors.append(f"Missing required tools: {', '.join(missing_tools)}")
            return False

        self.safety_checks["benchmark_dependencies"] = True
        logger.info(f"Benchmark dependencies check passed: {', '.join(required_tools)}")
        return True

    def perform_all_checks(
        self,
        mount_point: str,
        ramdisk_size_mb: int = 0,
        required_tools: Optional[List[str]] = None,
    ) -> bool:
        """Perform all safety checks."""
        logger.info("Performing safety checks...")

        checks = [
            self.check_disk_space(mount_point),
            self.check_memory_usage(ramdisk_size_mb),
            self.check_cpu_usage(),
            self.check_write_permissions(mount_point),
            self.check_system_load(),
        ]

        if required_tools:
            checks.append(self.check_benchmark_dependencies(required_tools))

        all_passed = all(checks)

        # Log results
        if self.warnings:
            for warning in self.warnings:
                logger.warning(warning)

        if self.errors:
            for error in self.errors:
                logger.error(error)

        if all_passed:
            logger.info("All safety checks passed")
        else:
            logger.error("Some safety checks failed")

        return all_passed

    def get_safety_report(self) -> Dict[str, Any]:
        """Get a safety report with all checks and issues."""
        return {
            "checks_passed": self.safety_checks,
            "warnings": self.warnings,
            "errors": self.errors,
            "all_checks_passed": len(self.errors) == 0,
        }


class InterruptHandler:
    """Handles graceful shutdown on interrupts."""

    def __init__(self):
        self.interrupted = False
        self.cleanup_functions = []

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle interrupt signals."""
        logger.warning(f"Received signal {signum}, initiating graceful shutdown...")
        self.interrupted = True

        # Call cleanup functions
        for cleanup_func in self.cleanup_functions:
            try:
                cleanup_func()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

        sys.exit(1)

    def register_cleanup(self, cleanup_func):
        """Register a cleanup function to be called on interrupt."""
        self.cleanup_functions.append(cleanup_func)

    def is_interrupted(self) -> bool:
        """Check if the process has been interrupted."""
        return self.interrupted


class ResourceMonitor:
    """Monitors system resources during benchmarking."""

    def __init__(self, monitor_interval: float = 1.0):
        self.monitor_interval = monitor_interval
        self.monitoring = False
        self.resource_data = []

    def start_monitoring(self):
        """Start resource monitoring."""
        self.monitoring = True
        self.resource_data = []
        logger.info("Started resource monitoring")

    def stop_monitoring(self):
        """Stop resource monitoring."""
        self.monitoring = False
        logger.info("Stopped resource monitoring")

    def collect_sample(self) -> Dict[str, Any]:
        """Collect a single resource sample."""
        try:
            sample = {
                "timestamp": time.time(),
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_io": (
                    psutil.disk_io_counters()._asdict()
                    if psutil.disk_io_counters()
                    else {}
                ),
                "load_avg": (
                    psutil.getloadavg()[0] if hasattr(psutil, "getloadavg") else 0
                ),
            }

            if self.monitoring:
                self.resource_data.append(sample)

            return sample

        except Exception as e:
            logger.warning(f"Could not collect resource sample: {e}")
            return {}

    def get_monitoring_report(self) -> Dict[str, Any]:
        """Get a report of monitored resources."""
        if not self.resource_data:
            return {"error": "No monitoring data available"}

        # Calculate averages
        avg_cpu = sum(
            sample.get("cpu_percent", 0) for sample in self.resource_data
        ) / len(self.resource_data)
        avg_memory = sum(
            sample.get("memory_percent", 0) for sample in self.resource_data
        ) / len(self.resource_data)
        avg_load = sum(
            sample.get("load_avg", 0) for sample in self.resource_data
        ) / len(self.resource_data)

        return {
            "duration_seconds": self.resource_data[-1]["timestamp"]
            - self.resource_data[0]["timestamp"],
            "samples_collected": len(self.resource_data),
            "average_cpu_percent": avg_cpu,
            "average_memory_percent": avg_memory,
            "average_load": avg_load,
            "peak_cpu_percent": max(
                sample.get("cpu_percent", 0) for sample in self.resource_data
            ),
            "peak_memory_percent": max(
                sample.get("memory_percent", 0) for sample in self.resource_data
            ),
        }


def validate_device_path(device_path: str) -> bool:
    """Validate that a device path is safe to use."""
    import os
    import stat

    # Check if path exists
    if not os.path.exists(device_path):
        logger.error(f"Device path does not exist: {device_path}")
        return False

    # Check if it's a block device
    try:
        st = os.stat(device_path)
        if not stat.S_ISBLK(st.st_mode):
            logger.error(f"Path is not a block device: {device_path}")
            return False
    except OSError as e:
        logger.error(f"Cannot stat device path {device_path}: {e}")
        return False

    # Check if it's not a system critical device
    critical_devices = ["/dev/sda", "/dev/nvme0n1", "/dev/mmcblk0"]
    for critical in critical_devices:
        if device_path.startswith(critical) and len(device_path) == len(critical):
            logger.error(f"Cannot benchmark entire system disk: {device_path}")
            return False

    # Check if device is mounted and warn about it
    try:
        mount_output, _, _ = run_command(["mount"], timeout=10)
        if device_path in mount_output:
            logger.warning(f"Device {device_path} is currently mounted")
    except Exception:
        pass

    return True
