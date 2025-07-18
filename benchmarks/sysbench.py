"""
sysbench benchmark module for fileio performance testing.
"""

import re
import time
import os
from typing import Dict, Any

from utils import run_command, CommandExecutionError, check_command_exists
from .base import BenchmarkModule, BenchmarkResult


class SysbenchBenchmark(BenchmarkModule):
    """sysbench benchmark for fileio performance."""

    def __init__(self):
        super().__init__("sysbench")

    def is_available(self) -> bool:
        """Check if sysbench is available."""
        return check_command_exists("sysbench")

    def run(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run sysbench fileio benchmark."""
        self.logger.info(f"Running sysbench fileio benchmark on {mount_point}")

        start_time = time.time()

        try:
            # Change to mount point directory
            original_dir = os.getcwd()
            os.chdir(mount_point)

            # Get configuration values
            file_total_size = (
                self.config.sysbench_file_total_size if self.config else "1G"
            )
            file_num = self.config.sysbench_file_num if self.config else 16
            file_block_size = (
                self.config.sysbench_file_block_size if self.config else 16384
            )
            threads = self.config.sysbench_threads if self.config else 4
            max_time = self.config.sysbench_max_time if self.config else 60
            timeout = int(self.config.max_test_duration) if self.config else 120

            # Prepare phase
            prepare_cmd = [
                "sysbench",
                "fileio",
                f"--file-total-size={file_total_size}",
                "--file-test-mode=rndrw",
                f"--file-num={file_num}",
                f"--file-block-size={file_block_size}",
                "prepare",
            ]

            stdout, stderr, return_code = run_command(prepare_cmd, timeout=timeout)

            if return_code != 0:
                os.chdir(original_dir)
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=time.time() - start_time,
                    raw_output=stderr,
                    error_message=f"sysbench prepare failed with return code {return_code}",
                )

            # Run phase
            run_cmd = [
                "sysbench",
                "fileio",
                f"--file-total-size={file_total_size}",
                "--file-test-mode=rndrw",
                f"--file-num={file_num}",
                f"--file-block-size={file_block_size}",
                f"--time={max_time}",
                f"--threads={threads}",
                "run",
            ]

            stdout, stderr, return_code = run_command(run_cmd, timeout=timeout * 2)

            if return_code != 0:
                # Try to cleanup before returning
                self._cleanup_sysbench_files(mount_point)
                os.chdir(original_dir)
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=time.time() - start_time,
                    raw_output=stderr,
                    error_message=f"sysbench run failed with return code {return_code}",
                )

            # Cleanup phase
            self._cleanup_sysbench_files(mount_point)
            os.chdir(original_dir)

            duration = time.time() - start_time

            # Parse sysbench output
            metrics = self._parse_sysbench_output(stdout)

            self.logger.info(f"sysbench benchmark completed in {duration:.2f}s")

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
            self.logger.error(f"sysbench benchmark failed: {e}")

            # Try to cleanup and restore directory
            try:
                self._cleanup_sysbench_files(mount_point)
                os.chdir(original_dir)
            except Exception as e:
                self.logger.error(f"Error cleaning up sysbench files: {e}")
                pass

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )
        except Exception as e:
            # Restore directory on any error
            try:
                os.chdir(original_dir)
            except Exception as e:
                self.logger.error(f"Error restoring directory: {e}")
                pass

            duration = time.time() - start_time
            self.logger.error(f"sysbench benchmark crashed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )

    def _cleanup_sysbench_files(self, mount_point: str):
        """Clean up sysbench test files."""
        try:
            file_total_size = (
                self.config.sysbench_file_total_size if self.config else "1G"
            )
            file_num = self.config.sysbench_file_num if self.config else 16
            file_block_size = (
                self.config.sysbench_file_block_size if self.config else 16384
            )

            cleanup_cmd = [
                "sysbench",
                "fileio",
                f"--file-total-size={file_total_size}",
                "--file-test-mode=rndrw",
                f"--file-num={file_num}",
                f"--file-block-size={file_block_size}",
                "cleanup",
            ]

            # Don't check return code for cleanup - just try
            run_command(cleanup_cmd, timeout=30, check_return_code=False)

        except Exception as e:
            self.logger.warning(f"Failed to cleanup sysbench files: {e}")

    def _parse_sysbench_output(self, output: str) -> Dict[str, Any]:
        """Parse sysbench output to extract metrics."""
        metrics = {}

        # Parse file operations per second (new format)
        reads_per_sec_pattern = r"reads/s:\s*([\d.]+)"
        reads_match = re.search(reads_per_sec_pattern, output)
        if reads_match:
            metrics["reads_per_sec"] = float(reads_match.group(1))

        writes_per_sec_pattern = r"writes/s:\s*([\d.]+)"
        writes_match = re.search(writes_per_sec_pattern, output)
        if writes_match:
            metrics["writes_per_sec"] = float(writes_match.group(1))

        fsyncs_per_sec_pattern = r"fsyncs/s:\s*([\d.]+)"
        fsyncs_match = re.search(fsyncs_per_sec_pattern, output)
        if fsyncs_match:
            metrics["fsyncs_per_sec"] = float(fsyncs_match.group(1))

        # Parse read throughput (MiB/s)
        read_throughput_pattern = r"read, MiB/s:\s*([\d.]+)"
        read_throughput_match = re.search(read_throughput_pattern, output)
        if read_throughput_match:
            metrics["read_throughput_mib_per_sec"] = float(
                read_throughput_match.group(1)
            )
            # Convert to MB/s for consistency (1 MiB = 1.048576 MB)
            metrics["read_throughput_mbps"] = (
                float(read_throughput_match.group(1)) * 1.048576
            )

        # Parse write throughput (MiB/s)
        write_throughput_pattern = r"written, MiB/s:\s*([\d.]+)"
        write_throughput_match = re.search(write_throughput_pattern, output)
        if write_throughput_match:
            metrics["write_throughput_mib_per_sec"] = float(
                write_throughput_match.group(1)
            )
            # Convert to MB/s for consistency (1 MiB = 1.048576 MB)
            metrics["write_throughput_mbps"] = (
                float(write_throughput_match.group(1)) * 1.048576
            )

        # Calculate total file operations per second
        total_ops = 0
        if "reads_per_sec" in metrics:
            total_ops += metrics["reads_per_sec"]
        if "writes_per_sec" in metrics:
            total_ops += metrics["writes_per_sec"]
        if "fsyncs_per_sec" in metrics:
            total_ops += metrics["fsyncs_per_sec"]
        if total_ops > 0:
            metrics["file_operations_per_sec"] = total_ops

        # Parse total number of events
        total_events_pattern = r"total number of events:\s*([\d.]+)"
        total_events_match = re.search(total_events_pattern, output)
        if total_events_match:
            metrics["total_events"] = int(float(total_events_match.group(1)))

        # Parse latency (both patterns for compatibility)
        latency_pattern = r"Latency \(ms\):\s*min:\s*([\d.]+)\s*avg:\s*([\d.]+)\s*max:\s*([\d.]+)\s*95th percentile:\s*([\d.]+)"
        latency_match = re.search(latency_pattern, output)
        if latency_match:
            metrics["latency_min_ms"] = float(latency_match.group(1))
            metrics["latency_avg_ms"] = float(latency_match.group(2))
            metrics["latency_max_ms"] = float(latency_match.group(3))
            metrics["latency_95th_percentile_ms"] = float(latency_match.group(4))

        # Parse events (thread fairness)
        events_pattern = r"events \(avg/stddev\):\s*([\d.]+)/([\d.]+)"
        events_match = re.search(events_pattern, output)
        if events_match:
            metrics["events_avg"] = float(events_match.group(1))
            metrics["events_stddev"] = float(events_match.group(2))

        # Parse execution time
        exec_time_pattern = r"execution time \(avg/stddev\):\s*([\d.]+)/([\d.]+)"
        exec_time_match = re.search(exec_time_pattern, output)
        if exec_time_match:
            metrics["execution_time_avg"] = float(exec_time_match.group(1))
            metrics["execution_time_stddev"] = float(exec_time_match.group(2))

        # Parse total time
        total_time_pattern = r"total time:\s*([\d.]+)s"
        total_time_match = re.search(total_time_pattern, output)
        if total_time_match:
            metrics["total_time_sec"] = float(total_time_match.group(1))

        return metrics

    def run_sequential_read(
        self, device_path: str, mount_point: str
    ) -> BenchmarkResult:
        """Run sequential read benchmark."""
        self.logger.info(f"Running sysbench sequential read benchmark on {mount_point}")

        return self._run_specific_test(
            device_path, mount_point, "seqrd", "sequential_read"
        )

    def run_sequential_write(
        self, device_path: str, mount_point: str
    ) -> BenchmarkResult:
        """Run sequential write benchmark."""
        self.logger.info(
            f"Running sysbench sequential write benchmark on {mount_point}"
        )

        return self._run_specific_test(
            device_path, mount_point, "seqwr", "sequential_write"
        )

    def run_random_read(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run random read benchmark."""
        self.logger.info(f"Running sysbench random read benchmark on {mount_point}")

        return self._run_specific_test(device_path, mount_point, "rndrd", "random_read")

    def run_random_write(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run random write benchmark."""
        self.logger.info(f"Running sysbench random write benchmark on {mount_point}")

        return self._run_specific_test(
            device_path, mount_point, "rndwr", "random_write"
        )

    def _run_specific_test(
        self, device_path: str, mount_point: str, test_mode: str, test_name: str
    ) -> BenchmarkResult:
        """Run a specific sysbench test mode."""
        start_time = time.time()

        try:
            # Change to mount point directory
            original_dir = os.getcwd()
            os.chdir(mount_point)

            # Prepare phase
            prepare_cmd = [
                "sysbench",
                "fileio",
                "--file-total-size=512M",
                f"--file-test-mode={test_mode}",
                "--file-num=8",
                "--file-block-size=16384",
                "prepare",
            ]

            run_command(prepare_cmd, timeout=120)

            # Run phase
            run_cmd = [
                "sysbench",
                "fileio",
                "--file-total-size=512M",
                f"--file-test-mode={test_mode}",
                "--file-num=8",
                "--file-block-size=16384",
                "--time=60",
                "--threads=4",
                "run",
            ]

            stdout, stderr, return_code = run_command(run_cmd, timeout=180)

            # Cleanup
            self._cleanup_sysbench_files(mount_point)
            os.chdir(original_dir)

            duration = time.time() - start_time

            if return_code != 0:
                return self._create_result(
                    device_path=device_path,
                    mount_point=mount_point,
                    success=False,
                    duration_seconds=duration,
                    raw_output=stderr,
                    error_message=f"sysbench {test_name} failed with return code {return_code}",
                )

            # Parse output
            metrics = self._parse_sysbench_output(stdout)
            metrics["test_mode"] = test_mode
            metrics["test_name"] = test_name

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=True,
                duration_seconds=duration,
                raw_output=stdout,
                metrics=metrics,
            )

        except Exception as e:
            # Restore directory on any error
            try:
                os.chdir(original_dir)
            except Exception as ex:
                self.logger.error(f"sysbench {test_name} failed: {ex}")
                pass

            duration = time.time() - start_time
            self.logger.error(f"sysbench {test_name} failed: {e}")

            return self._create_result(
                device_path=device_path,
                mount_point=mount_point,
                success=False,
                duration_seconds=duration,
                raw_output="",
                error_message=str(e),
            )
