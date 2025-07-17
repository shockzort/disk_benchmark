"""
hdparm benchmark module for disk timing tests.
"""

import re
import time
from typing import Dict, Any

from utils import run_command, CommandExecutionError, check_command_exists
from .base import BenchmarkModule, BenchmarkResult


class HdparmBenchmark(BenchmarkModule):
    """hdparm benchmark for disk timing."""

    def __init__(self):
        super().__init__("hdparm")

    def is_available(self) -> bool:
        """Check if hdparm is available."""
        return check_command_exists("hdparm")

    def run(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run hdparm benchmark."""
        self.logger.info(f"Running hdparm benchmark on {device_path}")

        start_time = time.time()

        try:
            # Run hdparm timing test
            stdout, stderr, return_code = run_command(
                ["hdparm", "-Tt", device_path], timeout=120
            )

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stdout,
                    error_message=stderr,
                )

            # Parse hdparm output
            metrics = self._parse_hdparm_output(stdout)

            self.logger.info(f"hdparm benchmark completed in {duration:.2f}s")

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
            self.logger.error(f"hdparm benchmark failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def _parse_hdparm_output(self, output: str) -> Dict[str, Any]:
        """Parse hdparm output to extract metrics."""
        metrics = {}

        # Parse cached reads
        cached_match = re.search(
            r"Timing cached reads:\s+(\d+)\s+MB in\s+([\d.]+)\s+seconds\s+=\s+([\d.]+)\s+MB/sec",
            output,
        )
        if cached_match:
            metrics["cached_reads_mb"] = int(cached_match.group(1))
            metrics["cached_reads_time_sec"] = float(cached_match.group(2))
            metrics["cached_reads_speed_mb_per_sec"] = float(cached_match.group(3))

        # Parse buffered disk reads
        buffered_match = re.search(
            r"Timing buffered disk reads:\s+(\d+)\s+MB in\s+([\d.]+)\s+seconds\s+=\s+([\d.]+)\s+MB/sec",
            output,
        )
        if buffered_match:
            metrics["buffered_reads_mb"] = int(buffered_match.group(1))
            metrics["buffered_reads_time_sec"] = float(buffered_match.group(2))
            metrics["buffered_reads_speed_mb_per_sec"] = float(buffered_match.group(3))

        return metrics

    def get_device_info(self, device_path: str) -> Dict[str, Any]:
        """Get device information using hdparm."""
        try:
            stdout, stderr, return_code = run_command(
                ["hdparm", "-I", device_path], timeout=30
            )

            if return_code == 0:
                return {"hdparm_info": stdout, "error": None}
            else:
                return {"hdparm_info": None, "error": stderr}

        except CommandExecutionError as e:
            return {"hdparm_info": None, "error": str(e)}
