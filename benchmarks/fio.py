"""
fio benchmark module for flexible I/O testing.
"""

import json
import time
from typing import Dict, Any

from utils import run_command, CommandExecutionError, check_command_exists
from .base import BenchmarkModule, BenchmarkResult


class FioBenchmark(BenchmarkModule):
    """fio benchmark for flexible I/O testing."""

    def __init__(self):
        super().__init__("fio")

    def is_available(self) -> bool:
        """Check if fio is available."""
        return check_command_exists("fio")

    def run(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run fio benchmark."""
        self.logger.info(f"Running fio benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Get configuration values
            size = self.config.fio_size if self.config else "256M"
            numjobs = self.config.fio_numjobs if self.config else 4
            runtime = self.config.fio_runtime if self.config else 60
            block_size = self.config.fio_block_size if self.config else "4k"
            iodepth = self.config.fio_iodepth if self.config else 32
            rwmixread = self.config.fio_rwmixread if self.config else 70
            timeout = int(self.config.max_test_duration) if self.config else 300

            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            # Adjust direct I/O setting for RAM disks
            direct_io = "0" if is_ramdisk else "1"
            if is_ramdisk:
                self.logger.info(
                    "Using buffered I/O for RAM disk (tmpfs doesn't support direct I/O)"
                )

            # Run fio with comprehensive I/O patterns
            fio_command = [
                "fio",
                "--name=benchmark",
                f"--directory={mount_point}",
                "--ioengine=libaio",
                f"--direct={direct_io}",
                f"--size={size}",
                f"--numjobs={numjobs}",
                f"--runtime={runtime}",
                "--time_based",
                "--group_reporting",
                "--output-format=json",
                "--rw=randrw",  # Mixed random read/write
                f"--bs={block_size}",
                f"--iodepth={iodepth}",
                f"--rwmixread={rwmixread}",
            ]

            stdout, stderr, return_code = run_command(fio_command, timeout=timeout)

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"fio command failed with return code {return_code}",
                )

            # Parse fio JSON output
            metrics = self._parse_fio_output(stdout)

            self.logger.info(f"fio benchmark completed in {duration:.2f}s")

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
            self.logger.error(f"fio benchmark failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def _parse_fio_output(self, output: str) -> Dict[str, Any]:
        """Parse fio JSON output to extract metrics."""
        metrics = {}

        try:
            fio_data = json.loads(output)

            # Extract global options
            if "global options" in fio_data:
                metrics["global_options"] = fio_data["global options"]

            # Extract job results
            if "jobs" in fio_data and len(fio_data["jobs"]) > 0:
                job = fio_data["jobs"][0]  # Take first job

                # Read statistics
                if "read" in job:
                    read_stats = job["read"]
                    metrics["read_iops"] = read_stats.get("iops", 0)
                    metrics["read_bandwidth_kbs"] = read_stats.get("bw", 0)
                    metrics["read_bandwidth_mbs"] = read_stats.get("bw", 0) / 1024

                    if "lat_ns" in read_stats:
                        metrics["read_lat_mean_ns"] = read_stats["lat_ns"].get(
                            "mean", 0
                        )
                        metrics["read_lat_stddev_ns"] = read_stats["lat_ns"].get(
                            "stddev", 0
                        )

                # Write statistics
                if "write" in job:
                    write_stats = job["write"]
                    metrics["write_iops"] = write_stats.get("iops", 0)
                    metrics["write_bandwidth_kbs"] = write_stats.get("bw", 0)
                    metrics["write_bandwidth_mbs"] = write_stats.get("bw", 0) / 1024

                    if "lat_ns" in write_stats:
                        metrics["write_lat_mean_ns"] = write_stats["lat_ns"].get(
                            "mean", 0
                        )
                        metrics["write_lat_stddev_ns"] = write_stats["lat_ns"].get(
                            "stddev", 0
                        )

                # Overall statistics
                if "job_runtime" in job:
                    metrics["job_runtime_ms"] = job["job_runtime"]

                # CPU usage
                if "usr_cpu" in job:
                    metrics["cpu_user_percent"] = job["usr_cpu"]
                if "sys_cpu" in job:
                    metrics["cpu_system_percent"] = job["sys_cpu"]

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.warning(f"Could not parse fio JSON output: {e}")
            # Fall back to text parsing if JSON fails
            metrics = self._parse_fio_text_fallback(output)

        return metrics

    def _parse_fio_text_fallback(self, output: str) -> Dict[str, Any]:
        """Fallback text parsing for fio output."""
        metrics = {}

        # Simple regex patterns for key metrics
        import re

        # Look for IOPS patterns
        iops_pattern = r"IOPS=(\d+)"
        iops_matches = re.findall(iops_pattern, output)
        if iops_matches:
            metrics["total_iops"] = int(iops_matches[0])

        # Look for bandwidth patterns
        bw_pattern = r"BW=(\d+)(\w+)"
        bw_matches = re.findall(bw_pattern, output)
        if bw_matches:
            bw_value, bw_unit = bw_matches[0]
            metrics["total_bandwidth"] = int(bw_value)
            metrics["bandwidth_unit"] = bw_unit

        return metrics

    def run_sequential_read(
        self, device_path: str, mount_point: str
    ) -> BenchmarkResult:
        """Run sequential read benchmark."""
        self.logger.info(f"Running fio sequential read benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            direct_io = "0" if is_ramdisk else "1"

            fio_command = [
                "fio",
                "--name=seq_read",
                f"--directory={mount_point}",
                "--ioengine=libaio",
                f"--direct={direct_io}",
                "--size=512M",
                "--numjobs=1",
                "--runtime=60",
                "--time_based",
                "--output-format=json",
                "--rw=read",
                "--bs=1M",
                "--iodepth=1",
            ]

            stdout, stderr, return_code = run_command(fio_command, timeout=300)
            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"fio sequential read failed with return code {return_code}",
                )

            metrics = self._parse_fio_output(stdout)
            metrics["test_type"] = "sequential_read"

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
            self.logger.error(f"fio sequential read failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def run_random_write(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run random write benchmark."""
        self.logger.info(f"Running fio random write benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            direct_io = "0" if is_ramdisk else "1"

            fio_command = [
                "fio",
                "--name=rand_write",
                f"--directory={mount_point}",
                "--ioengine=libaio",
                f"--direct={direct_io}",
                "--size=256M",
                "--numjobs=4",
                "--runtime=60",
                "--time_based",
                "--output-format=json",
                "--rw=randwrite",
                "--bs=4k",
                "--iodepth=32",
            ]

            stdout, stderr, return_code = run_command(fio_command, timeout=300)
            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"fio random write failed with return code {return_code}",
                )

            metrics = self._parse_fio_output(stdout)
            metrics["test_type"] = "random_write"

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
            self.logger.error(f"fio random write failed: {e}")

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
