"""
Benchmark orchestrator for coordinating multiple benchmark tools.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from .base import BenchmarkModule, BenchmarkResult
from .hdparm import HdparmBenchmark
from .dd import DdBenchmark
from .fio import FioBenchmark
from .sysbench import SysbenchBenchmark
from .ioping import IopingBenchmark


logger = logging.getLogger(__name__)


class BenchmarkOrchestrator:
    """Orchestrates multiple benchmark tools."""

    def __init__(self, config=None, is_ramdisk=False):
        self.benchmarks: List[BenchmarkModule] = []
        self.results: List[BenchmarkResult] = []
        self.config = config
        self.is_ramdisk = is_ramdisk

        # Initialize available benchmarks
        self._initialize_benchmarks()

    def _initialize_benchmarks(self):
        """Initialize available benchmark modules."""
        # Core benchmarks - hdparm only for physical devices
        core_benchmarks = []
        if not self.is_ramdisk:
            core_benchmarks.append(HdparmBenchmark())
        core_benchmarks.append(DdBenchmark())

        # Advanced benchmarks (may not be available)
        advanced_benchmarks = [
            FioBenchmark(),
            SysbenchBenchmark(),
            IopingBenchmark(),
        ]

        # Add all available benchmarks
        all_benchmarks = core_benchmarks + advanced_benchmarks

        for benchmark in all_benchmarks:
            if benchmark.is_available():
                # Set configuration for the benchmark
                if self.config:
                    benchmark.set_config(self.config)
                self.benchmarks.append(benchmark)
                logger.info(f"Added {benchmark.name} benchmark")
            else:
                logger.warning(f"Skipping {benchmark.name} benchmark - not available")

        # Log if hdparm was skipped for RAM disk
        if self.is_ramdisk:
            logger.info("hdparm benchmark skipped - not applicable for RAM disks")

    def get_available_benchmarks(self) -> List[str]:
        """Get list of available benchmark names."""
        return [bench.name for bench in self.benchmarks]

    def run_all_benchmarks(
        self, device_path: str, mount_point: str
    ) -> List[BenchmarkResult]:
        """Run all available benchmarks."""
        logger.info(f"Running {len(self.benchmarks)} benchmarks on {device_path}")

        print(f"\n{'='*60}")
        print(f"RUNNING {len(self.benchmarks)} BENCHMARK TESTS")
        print(f"{'='*60}")
        print(f"Target: {device_path}")
        print(f"Mount Point: {mount_point}")
        print(f"Tests: {', '.join([b.name for b in self.benchmarks])}")
        print()

        self.results = []

        for i, benchmark in enumerate(self.benchmarks, 1):
            print(f"[{i}/{len(self.benchmarks)}] Running {benchmark.name} benchmark...")
            logger.info(
                f"Starting {benchmark.name} benchmark ({i}/{len(self.benchmarks)})..."
            )

            try:
                result = benchmark.run(device_path, mount_point)
                self.results.append(result)

                if result.success:
                    logger.info(f"✅ {benchmark.name} completed successfully")
                    # Print quick result summary
                    self._print_quick_result(result)
                else:
                    logger.error(f"💥 {benchmark.name} failed: {result.error_message}")
                    print(f"  💥 Failed: {result.error_message}")

            except Exception as e:
                logger.error(f"💥 {benchmark.name} crashed: {e}")
                print(f"  💥 Crashed: {str(e)}")

                # Create error result
                error_result = BenchmarkResult(
                    tool_name=benchmark.name,
                    device_path=device_path,
                    mount_point=mount_point,
                    timestamp=datetime.now().isoformat(),
                    success=False,
                    duration_seconds=0.0,
                    raw_output="",
                    error_message=str(e),
                )
                self.results.append(error_result)

            print()  # Add spacing between tests

        logger.info(f"Completed {len(self.results)} benchmarks")

        # Print completion summary
        successful = sum(1 for r in self.results if r.success)
        failed = len(self.results) - successful
        total_time = sum(r.duration_seconds for r in self.results)

        print(f"{'='*60}")
        print("BENCHMARK EXECUTION COMPLETED")
        print(f"{'='*60}")
        print(f"Total tests: {len(self.results)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"{'='*60}")

        return self.results

    def run_specific_benchmark(
        self, benchmark_name: str, device_path: str, mount_point: str
    ) -> Optional[BenchmarkResult]:
        """Run a specific benchmark by name."""
        for benchmark in self.benchmarks:
            if benchmark.name == benchmark_name:
                logger.info(f"Running {benchmark_name} benchmark...")
                result = benchmark.run(device_path, mount_point)
                self.results.append(result)
                return result

        logger.error(f"Benchmark {benchmark_name} not found or not available")
        return None

    def get_results(self) -> List[BenchmarkResult]:
        """Get all benchmark results."""
        return self.results

    def _print_quick_result(self, result: BenchmarkResult):
        """Print a quick summary of benchmark result."""
        if not result.success or not result.metrics:
            print(f"  ✅ Completed in {result.duration_seconds:.2f}s")
            return

        metrics_summary = []

        # Extract key metrics for different tools
        if result.tool_name == "hdparm":
            if "buffered_reads_speed_mb_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Read: {result.metrics['buffered_reads_speed_mb_per_sec']:.1f} MB/s"
                )
            if "cached_reads_speed_mb_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Cached: {result.metrics['cached_reads_speed_mb_per_sec']:.1f} MB/s"
                )

        elif result.tool_name == "dd":
            if "transfer_rate_mb_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Write: {result.metrics['transfer_rate_mb_per_sec']:.1f} MB/s"
                )

        elif result.tool_name == "fio":
            if "read_bandwidth_mbs" in result.metrics:
                metrics_summary.append(
                    f"Read: {result.metrics['read_bandwidth_mbs']:.1f} MB/s"
                )
            if "write_bandwidth_mbs" in result.metrics:
                metrics_summary.append(
                    f"Write: {result.metrics['write_bandwidth_mbs']:.1f} MB/s"
                )
            if "read_iops" in result.metrics:
                metrics_summary.append(f"R-IOPS: {result.metrics['read_iops']:.0f}")
            if "write_iops" in result.metrics:
                metrics_summary.append(f"W-IOPS: {result.metrics['write_iops']:.0f}")

        elif result.tool_name == "sysbench":
            if "read_throughput_mb_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Read: {result.metrics['read_throughput_mb_per_sec']:.1f} MB/s"
                )
            if "write_throughput_mb_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Write: {result.metrics['write_throughput_mb_per_sec']:.1f} MB/s"
                )
            if "total_throughput_mb_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Total: {result.metrics['total_throughput_mb_per_sec']:.1f} MB/s"
                )
            elif "file_operations_per_sec" in result.metrics:
                metrics_summary.append(
                    f"Ops: {result.metrics['file_operations_per_sec']:.0f}/s"
                )

        elif result.tool_name == "ioping":
            if "latency_avg_us" in result.metrics:
                metrics_summary.append(
                    f"Latency: {result.metrics['latency_avg_us']:.1f} μs"
                )
            if "iops" in result.metrics:
                metrics_summary.append(f"IOPS: {result.metrics['iops']:.0f}")
            if "throughput_mb_per_sec" in result.metrics:
                metrics_summary.append(f"Throughput: {result.metrics['throughput_mb_per_sec']:.1f} MB/s")

        summary_text = (
            ", ".join(metrics_summary) if metrics_summary else "No key metrics"
        )
        print(f"  ✅ Completed in {result.duration_seconds:.2f}s - {summary_text}")

    def get_results_summary(self) -> Dict[str, Any]:
        """Get summary of benchmark results."""
        total_benchmarks = len(self.results)
        successful_benchmarks = sum(1 for result in self.results if result.success)
        failed_benchmarks = total_benchmarks - successful_benchmarks

        total_duration = sum(result.duration_seconds for result in self.results)

        return {
            "total_benchmarks": total_benchmarks,
            "successful_benchmarks": successful_benchmarks,
            "failed_benchmarks": failed_benchmarks,
            "total_duration_seconds": total_duration,
            "benchmark_names": [result.tool_name for result in self.results],
        }
