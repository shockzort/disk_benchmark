"""
dd benchmark module for direct I/O write performance testing.
"""

import re
import time
import os
from typing import Dict, Any

from utils import run_command, CommandExecutionError, check_command_exists
from .base import BenchmarkModule, BenchmarkResult


class DdBenchmark(BenchmarkModule):
    """dd benchmark for direct I/O write performance."""

    def __init__(self):
        super().__init__("dd")

    def is_available(self) -> bool:
        """Check if dd is available."""
        return check_command_exists("dd")

    def run(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run dd benchmark."""
        self.logger.info(f"Running dd benchmark on {mount_point}")

        start_time = time.time()
        test_file = os.path.join(mount_point, "dd_test_file")

        try:
            # Get configuration values
            block_size = self.config.dd_block_size if self.config else "1M"
            count = self.config.dd_count if self.config else 100
            flags = self.config.dd_flags if self.config else "direct,fsync"
            timeout = int(self.config.max_test_duration) if self.config else 300

            # Check if this is a RAM disk (tmpfs doesn't support direct I/O)
            is_ramdisk = (
                device_path == "ramdisk"
                or mount_point.startswith("/mnt/ramdisk_")
                or self._is_tmpfs_mount(mount_point)
            )

            # Parse flags and exclude 'direct' for RAM disks
            oflag_parts = []
            conv_parts = []
            for flag in flags.split(","):
                flag = flag.strip()
                if flag in ["direct", "sync", "dsync"]:
                    # Skip 'direct' flag for RAM disks as tmpfs doesn't support it
                    if flag == "direct" and is_ramdisk:
                        self.logger.info(
                            "Skipping 'direct' flag for RAM disk (tmpfs doesn't support direct I/O)"
                        )
                        continue
                    oflag_parts.append(flag)
                elif flag in ["fsync", "fdatasync"]:
                    conv_parts.append(flag)

            # Build dd command
            dd_cmd = [
                "dd",
                "if=/dev/zero",
                f"of={test_file}",
                f"bs={block_size}",
                f"count={count}",
            ]

            if oflag_parts:
                dd_cmd.append(f"oflag={','.join(oflag_parts)}")
            if conv_parts:
                dd_cmd.append(f"conv={','.join(conv_parts)}")

            # Run dd write test with direct I/O
            stdout, stderr, return_code = run_command(dd_cmd, timeout=timeout)

            duration = time.time() - start_time

            # Clean up test file
            try:
                if os.path.exists(test_file):
                    os.remove(test_file)
            except OSError:
                pass

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"dd command failed with return code {return_code}",
                )

            # Parse dd output
            metrics = self._parse_dd_output(stderr)  # dd writes to stderr

            self.logger.info(f"dd benchmark completed in {duration:.2f}s")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=True,
                duration_seconds=duration,
                raw_output=stderr,
                metrics=metrics,
            )

        except CommandExecutionError as e:
            duration = time.time() - start_time
            self.logger.error(f"dd benchmark failed: {e}")

            # Clean up test file on error
            try:
                if os.path.exists(test_file):
                    os.remove(test_file)
            except OSError:
                pass

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def _parse_dd_output(self, output: str) -> Dict[str, Any]:
        """Parse dd output to extract metrics."""
        metrics = {}

        # Parse transfer statistics
        # Examples:
        # "104857600 bytes (105 MB, 100 MiB) copied, 2.12345 s, 49.4 MB/s" (English)
        # "104857600 bytes (105 MB, 100 MiB) copied, 9,44847 s, 11,1 MB/s" (European)
        transfer_match = re.search(
            r"(\d+)\s+bytes.*copied,\s+([\d.,]+)\s+s,\s+([\d.,]+)\s+([KMGT]?B/s)",
            output,
        )
        if transfer_match:
            metrics["bytes_transferred"] = int(transfer_match.group(1))

            # Handle both comma and period decimal separators
            time_str = transfer_match.group(2).replace(",", ".")
            rate_str = transfer_match.group(3).replace(",", ".")

            try:
                metrics["transfer_time_sec"] = float(time_str)
                metrics["transfer_rate"] = float(rate_str)
                metrics["transfer_rate_unit"] = transfer_match.group(4)
            except ValueError:
                self.logger.warning(
                    f"Could not parse dd numeric values: time='{transfer_match.group(2)}', rate='{transfer_match.group(3)}'"
                )
                return metrics

        # Convert to MB/s if needed
        if "transfer_rate" in metrics and "transfer_rate_unit" in metrics:
            rate = metrics["transfer_rate"]
            unit = metrics["transfer_rate_unit"]

            if unit == "GB/s":
                rate *= 1024
            elif unit == "KB/s":
                rate /= 1024
            elif unit == "B/s":
                rate /= 1024 * 1024

            metrics["transfer_rate_mbps"] = rate

        return metrics

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
