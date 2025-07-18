"""
ioping benchmark module for disk latency measurement.
"""

import re
import time
from typing import Dict, Any

from utils import run_command, CommandExecutionError, check_command_exists
from .base import BenchmarkModule, BenchmarkResult


class IopingBenchmark(BenchmarkModule):
    """ioping benchmark for disk latency measurement."""

    def __init__(self):
        super().__init__("ioping")

    def is_available(self) -> bool:
        """Check if ioping is available."""
        return check_command_exists("ioping")

    def run(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run ioping latency benchmark."""
        self.logger.info(f"Running ioping latency benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Get configuration values
            count = self.config.ioping_count if self.config else 100
            size = self.config.ioping_size if self.config else "4k"
            timeout = int(self.config.ioping_deadline) if self.config else 120

            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            # Build ioping command, exclude -D flag for RAM disks
            ioping_command = [
                "ioping",
                "-c",
                str(count),
                "-s",
                size,
                "-q",  # Quiet mode (less verbose)
                mount_point,
            ]

            # Add direct I/O flag only for non-RAM disks
            if not is_ramdisk:
                ioping_command.insert(-2, "-D")  # Insert before mount_point
            else:
                self.logger.info(
                    "Skipping direct I/O flag for RAM disk (tmpfs doesn't support it)"
                )

            stdout, stderr, return_code = run_command(ioping_command, timeout=timeout)

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"ioping command failed with return code {return_code}",
                )

            # Parse ioping output
            metrics = self._parse_ioping_output(stdout)

            self.logger.info(f"ioping benchmark completed in {duration:.2f}s")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=True,
                duration_seconds=duration,
                raw_output=stdout,
                metrics=metrics,
            )

        except CommandExecutionError as e:
            duration = time.time() - start_time
            self.logger.error(f"ioping benchmark failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def _parse_ioping_output(self, output: str) -> Dict[str, Any]:
        """Parse ioping output to extract latency metrics."""
        metrics = {}

        # Parse the completed requests line for IOPS and throughput
        # Example: "99 requests completed in 794.9 us, 396 KiB read, 124.5 k iops, 486.5 MiB/s"
        completed_pattern = r"(\d+)\s+requests\s+completed\s+in\s+([\d.]+)\s*(\w+),\s+(\d+)\s*(\w+)\s+read,\s+([\d.]+)\s*(\w*)\s+iops,\s+([\d.]+)\s*(\w+)/s"
        completed_match = re.search(completed_pattern, output)

        if completed_match:
            requests_completed = int(completed_match.group(1))
            completion_time = float(completed_match.group(2))
            time_unit = completed_match.group(3)
            data_amount = int(completed_match.group(4))
            data_unit = completed_match.group(5)
            iops_value = float(completed_match.group(6))
            iops_multiplier = completed_match.group(7)  # k, M, etc.
            throughput_value = float(completed_match.group(8))
            throughput_unit = completed_match.group(9)  # MiB, etc.

            metrics["requests_completed"] = requests_completed
            metrics["completion_time"] = completion_time
            metrics["completion_time_unit"] = time_unit
            metrics["data_read"] = data_amount
            metrics["data_read_unit"] = data_unit

            # Convert IOPS to actual number
            if iops_multiplier == "k":
                iops_actual = iops_value * 1000
            elif iops_multiplier == "M":
                iops_actual = iops_value * 1000000
            else:
                iops_actual = iops_value
            metrics["iops"] = iops_actual

            # Convert throughput to MB/s for consistency
            if throughput_unit == "MiB":
                throughput_mb_s = throughput_value * 1.048576  # Convert MiB/s to MB/s
            elif throughput_unit == "KiB":
                throughput_mb_s = (
                    throughput_value * 1.048576 / 1024
                )  # Convert KiB/s to MB/s
            elif throughput_unit == "GiB":
                throughput_mb_s = (
                    throughput_value * 1.048576 * 1024
                )  # Convert GiB/s to MB/s
            else:
                throughput_mb_s = throughput_value  # Assume MB/s
            metrics["throughput_mbps"] = throughput_mb_s
            metrics["throughput_original"] = throughput_value
            metrics["throughput_unit"] = throughput_unit

        # Parse latency statistics
        # Example: "min/avg/max/mdev = 1.89 us / 8.03 us / 13.8 us / 2.91 us"
        latency_pattern = r"min/avg/max/mdev\s*=\s*([\d.]+)\s*(\w+)\s*/\s*([\d.]+)\s*(\w+)\s*/\s*([\d.]+)\s*(\w+)\s*/\s*([\d.]+)\s*(\w+)"
        latency_match = re.search(latency_pattern, output)

        if latency_match:
            min_val, min_unit = float(latency_match.group(1)), latency_match.group(2)
            avg_val, avg_unit = float(latency_match.group(3)), latency_match.group(4)
            max_val, max_unit = float(latency_match.group(5)), latency_match.group(6)
            mdev_val, mdev_unit = float(latency_match.group(7)), latency_match.group(8)

            # Convert all to microseconds for consistency
            metrics["latency_min_us"] = self._convert_to_microseconds(min_val, min_unit)
            metrics["latency_avg_us"] = self._convert_to_microseconds(avg_val, avg_unit)
            metrics["latency_max_us"] = self._convert_to_microseconds(max_val, max_unit)
            metrics["latency_mdev_us"] = self._convert_to_microseconds(
                mdev_val, mdev_unit
            )

        return metrics

    def _convert_to_microseconds(self, value: float, unit: str) -> float:
        """Convert time value to microseconds."""
        if unit == "us":
            return value
        elif unit == "ms":
            return value * 1000
        elif unit == "s":
            return value * 1000000
        elif unit == "ns":
            return value / 1000
        else:
            # Default to microseconds if unknown unit
            return value

    def run_sequential_latency(
        self, device_path: str, mount_point: str
    ) -> BenchmarkResult:
        """Run sequential latency benchmark."""
        self.logger.info(
            f"Running ioping sequential latency benchmark on {mount_point}"
        )

        start_time = time.time()

        try:
            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            # Run ioping in sequential mode
            ioping_command = [
                "ioping",
                "-c",
                "100",  # 100 requests
                "-s",
                "4k",  # 4KB block size
                "-q",  # Quiet mode
                "-S",  # Sequential mode
                mount_point,
            ]

            # Add direct I/O flag only for non-RAM disks
            if not is_ramdisk:
                ioping_command.insert(-2, "-D")  # Insert before mount_point

            stdout, stderr, return_code = run_command(ioping_command, timeout=120)

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"ioping sequential latency failed with return code {return_code}",
                )

            metrics = self._parse_ioping_output(stdout)
            metrics["test_type"] = "sequential_latency"

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=True,
                duration_seconds=duration,
                raw_output=stdout,
                metrics=metrics,
            )

        except CommandExecutionError as e:
            duration = time.time() - start_time
            self.logger.error(f"ioping sequential latency failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def run_random_latency(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run random latency benchmark."""
        self.logger.info(f"Running ioping random latency benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            # Run ioping in random mode (default)
            ioping_command = [
                "ioping",
                "-c",
                "100",  # 100 requests
                "-s",
                "4k",  # 4KB block size
                "-q",  # Quiet mode
                "-R",  # Random mode (explicit)
                mount_point,
            ]

            # Add direct I/O flag only for non-RAM disks
            if not is_ramdisk:
                ioping_command.insert(-2, "-D")  # Insert before mount_point

            stdout, stderr, return_code = run_command(ioping_command, timeout=120)

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"ioping random latency failed with return code {return_code}",
                )

            metrics = self._parse_ioping_output(stdout)
            metrics["test_type"] = "random_latency"

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=True,
                duration_seconds=duration,
                raw_output=stdout,
                metrics=metrics,
            )

        except CommandExecutionError as e:
            duration = time.time() - start_time
            self.logger.error(f"ioping random latency failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def run_cached_latency(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run cached latency benchmark."""
        self.logger.info(f"Running ioping cached latency benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            # Run ioping in cached mode
            ioping_command = [
                "ioping",
                "-c",
                "100",  # 100 requests
                "-s",
                "4k",  # 4KB block size
                "-q",  # Quiet mode
                "-C",  # Cached mode
                mount_point,
            ]

            # Add direct I/O flag only for non-RAM disks
            if not is_ramdisk:
                ioping_command.insert(-2, "-D")  # Insert before mount_point

            stdout, stderr, return_code = run_command(ioping_command, timeout=120)

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"ioping cached latency failed with return code {return_code}",
                )

            metrics = self._parse_ioping_output(stdout)
            metrics["test_type"] = "cached_latency"

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=True,
                duration_seconds=duration,
                raw_output=stdout,
                metrics=metrics,
            )

        except CommandExecutionError as e:
            duration = time.time() - start_time
            self.logger.error(f"ioping cached latency failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def _is_tmpfs_mount(self, mount_point: str) -> bool:
        """Check if mount point is a tmpfs filesystem."""
        try:
            # Check /proc/mounts to see if this mount point is tmpfs
            with open("/proc/mounts", "r") as f:
                for line in f:
                    parts = line.split()
                    if (
                        len(parts) >= 3
                        and parts[1] == mount_point
                        and parts[2] == "tmpfs"
                    ):
                        return True
            return False
        except Exception:
            return False
