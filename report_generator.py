"""
Report generator for disk benchmark results.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from benchmarks.base import BenchmarkResult

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate comprehensive benchmark reports."""

    def __init__(self):
        self.report_dir = Path("reports")
        self.report_dir.mkdir(exist_ok=True)

    def generate_report(
        self,
        results: List[BenchmarkResult],
        device_info: Dict[str, Any],
        sys_info: Dict[str, Any],
        config=None,
    ) -> str:
        """Generate a comprehensive benchmark report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"benchmark_report_{timestamp}.txt"
        report_path = self.report_dir / report_filename

        # Generate report content
        report_content = self._generate_text_report(
            results, device_info, sys_info, config
        )

        # Write report to file
        with open(report_path, "w") as f:
            f.write(report_content)

        logger.info(f"Report generated: {report_path}")
        return str(report_path)

    def generate_json_report(
        self,
        results: List[BenchmarkResult],
        device_info: Dict[str, Any],
        sys_info: Dict[str, Any],
        config=None,
    ) -> str:
        """Generate a JSON benchmark report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"benchmark_report_{timestamp}.json"
        report_path = self.report_dir / report_filename

        # Prepare data for JSON
        json_data = {
            "report_info": {
                "timestamp": timestamp,
                "generated_at": datetime.now().isoformat(),
            },
            "system_info": sys_info,
            "device_info": device_info,
            "configuration": config.to_dict() if config else None,
            "results": [result.to_dict() for result in results],
            "summary": self._generate_summary(results),
        }

        # Write JSON report
        with open(report_path, "w") as f:
            json.dump(json_data, f, indent=2)

        logger.info(f"JSON report generated: {report_path}")
        return str(report_path)

    def _generate_text_report(
        self,
        results: List[BenchmarkResult],
        device_info: Dict[str, Any],
        sys_info: Dict[str, Any],
        config=None,
    ) -> str:
        """Generate text format report."""
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append("DISK BENCHMARK REPORT")
        lines.append("=" * 80)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # System Information
        lines.append("SYSTEM INFORMATION")
        lines.append("-" * 40)
        for key, value in sys_info.items():
            lines.append(f"{key.capitalize()}: {value}")
        lines.append("")

        # Device Information
        lines.append("DEVICE INFORMATION")
        lines.append("-" * 40)
        lines.extend(self._format_device_info(device_info))
        lines.append("")

        # Configuration
        if config:
            lines.append("BENCHMARK CONFIGURATION")
            lines.append("-" * 40)
            lines.append(config.to_human_readable())
            lines.append("")

        # Benchmark Results Summary
        summary = self._generate_summary(results)
        lines.append("BENCHMARK SUMMARY")
        lines.append("-" * 40)
        lines.append(f"Total benchmarks: {summary['total_benchmarks']}")
        lines.append(f"Successful: {summary['successful_benchmarks']}")
        lines.append(f"Failed: {summary['failed_benchmarks']}")
        lines.append(f"Total duration: {summary['total_duration_seconds']:.2f} seconds")
        lines.append("")

        # Individual Results
        lines.append("DETAILED RESULTS")
        lines.append("-" * 40)

        for result in results:
            lines.append(f"{result.tool_name.upper()} BENCHMARK")
            lines.append(f"{'=' * (len(result.tool_name) + 10)}")
            lines.append(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
            lines.append(f"Duration: {result.duration_seconds:.2f} seconds")
            lines.append(f"Timestamp: {result.timestamp}")

            if result.success and result.metrics:
                lines.append("Metrics:")
                if result.tool_name == "fio":
                    lines.extend(self._format_fio_metrics(result.metrics))
                else:
                    for key, value in result.metrics.items():
                        lines.append(f"  {key}: {value}")

            if result.raw_output:
                lines.append("Raw Output:")
                for line in result.raw_output.split("\n"):
                    if line.strip():
                        lines.append(f"  {line}")

            if not result.success and result.error_message:
                lines.append(f"Error: {result.error_message}")

            lines.append("")

        return "\n".join(lines)

    def _generate_summary(self, results: List[BenchmarkResult]) -> Dict[str, Any]:
        """Generate summary statistics."""
        total_benchmarks = len(results)
        successful_benchmarks = sum(1 for result in results if result.success)
        failed_benchmarks = total_benchmarks - successful_benchmarks
        total_duration = sum(result.duration_seconds for result in results)

        # Extract key performance metrics
        performance_metrics = {}

        for result in results:
            if result.success and result.metrics:
                if result.tool_name == "hdparm":
                    if "buffered_reads_speed_mbps" in result.metrics:
                        performance_metrics["hdparm_buffered_read_speed"] = (
                            result.metrics["buffered_reads_speed_mbps"]
                        )
                elif result.tool_name == "dd":
                    if "transfer_rate_mbps" in result.metrics:
                        performance_metrics["dd_write_speed"] = result.metrics[
                            "transfer_rate_mbps"
                        ]
                elif result.tool_name == "fio":
                    # Handle write test results
                    if result.metrics["test_type"] == "write_test":
                        if "write_throughput_mbps" in result.metrics:
                            performance_metrics["fio_write_throughput"] = (
                                result.metrics["write_throughput_mbps"]
                            )
                        if "write_iops" in result.metrics:
                            performance_metrics["fio_write_iops"] = result.metrics[
                                "write_iops"
                            ]
                        if "disk_util_percent" in result.metrics:
                            performance_metrics["fio_write_disk_util"] = result.metrics[
                                "disk_util_percent"
                            ]

                    # Handle random read-write test results
                    if result.metrics["test_type"] == "randrw_test":
                        if "read_throughput_mbps" in result.metrics:
                            performance_metrics["fio_randrw_read_throughput"] = (
                                result.metrics["read_throughput_mbps"]
                            )
                        if "write_throughput_mbps" in result.metrics:
                            performance_metrics["fio_randrw_write_throughput"] = (
                                result.metrics["write_throughput_mbps"]
                            )
                        if "read_iops" in result.metrics:
                            performance_metrics["fio_randrw_read_iops"] = (
                                result.metrics["read_iops"]
                            )
                        if "write_iops" in result.metrics:
                            performance_metrics["fio_randrw_write_iops"] = (
                                result.metrics["write_iops"]
                            )
                        if "disk_util_percent" in result.metrics:
                            performance_metrics["fio_randrw_disk_util"] = (
                                result.metrics["disk_util_percent"]
                            )
                elif result.tool_name == "sysbench":
                    if "read_throughput_mbps" in result.metrics:
                        performance_metrics["sysbench_read_throughput"] = (
                            result.metrics["read_throughput_mbps"]
                        )
                    if "write_throughput_mbps" in result.metrics:
                        performance_metrics["sysbench_write_throughput"] = (
                            result.metrics["write_throughput_mbps"]
                        )
                    if "total_throughput_mbps" in result.metrics:
                        performance_metrics["sysbench_total_throughput"] = (
                            result.metrics["total_throughput_mbps"]
                        )
                    if "file_operations_per_sec" in result.metrics:
                        performance_metrics["sysbench_ops_per_sec"] = result.metrics[
                            "file_operations_per_sec"
                        ]
                elif result.tool_name == "ioping":
                    if "latency_avg_us" in result.metrics:
                        performance_metrics["ioping_avg_latency_us"] = result.metrics[
                            "latency_avg_us"
                        ]
                    if "iops" in result.metrics:
                        performance_metrics["ioping_iops"] = result.metrics["iops"]
                    if "throughput_mbps" in result.metrics:
                        performance_metrics["ioping_throughput"] = result.metrics[
                            "throughput_mbps"
                        ]

        return {
            "total_benchmarks": total_benchmarks,
            "successful_benchmarks": successful_benchmarks,
            "failed_benchmarks": failed_benchmarks,
            "total_duration_seconds": total_duration,
            "performance_metrics": performance_metrics,
            "benchmark_names": [result.tool_name for result in results],
        }

    def print_summary(self, results: List[BenchmarkResult]):
        """Print a comprehensive summary to console."""
        summary = self._generate_summary(results)

        logger.info(f"{'=' * 70}")
        logger.info("COMPREHENSIVE BENCHMARK RESULTS SUMMARY")
        logger.info(f"{'=' * 70}")
        logger.info(f"Total benchmarks: {summary['total_benchmarks']}")
        logger.info(f"Successful: {summary['successful_benchmarks']}")
        logger.info(f"Failed: {summary['failed_benchmarks']}")
        logger.info(f"Total duration: {summary['total_duration_seconds']:.2f} seconds")

        # Print detailed results for each benchmark
        logger.info("DETAILED BENCHMARK RESULTS:")
        logger.info(f"{'=' * 70}")

        for result in results:
            status_icon = "✅" if result.success else "💥"
            status_text = "SUCCESS" if result.success else "FAILED"

            logger.info(
                f"{status_icon} {result.tool_name.upper()} - {status_text} ({result.duration_seconds:.2f}s)"
            )

            if result.success and result.metrics:
                # Print all metrics for this benchmark
                metrics_printed = False

                if result.tool_name == "hdparm":
                    if "buffered_reads_speed_mbps" in result.metrics:
                        logger.info(
                            f"  • Buffered reads: {result.metrics['buffered_reads_speed_mbps']:.2f} MB/s"
                        )
                        metrics_printed = True
                    if "cached_reads_speed_mbps" in result.metrics:
                        logger.info(
                            f"  • Cached reads: {result.metrics['cached_reads_speed_mbps']:.2f} MB/s"
                        )
                        metrics_printed = True
                elif result.tool_name == "dd":
                    if "transfer_rate_mbps" in result.metrics:
                        logger.info(
                            f"  • Write speed: {result.metrics['transfer_rate_mbps']:.2f} MB/s"
                        )
                        metrics_printed = True
                elif result.tool_name == "fio":
                    # Handle write test results
                    if result.metrics["test_type"] == "write_test":
                        logger.info("  • Write Test:")
                        if "write_throughput_mbps" in result.metrics:
                            logger.info(
                                f"    - Write throughput: {result.metrics['write_throughput_mbps']:.2f} MB/s"
                            )
                            metrics_printed = True
                        if "write_iops" in result.metrics:
                            logger.info(
                                f"    - Write IOPS: {result.metrics['write_iops']:.0f}"
                            )
                            metrics_printed = True
                        if "disk_util_percent" in result.metrics:
                            logger.info(
                                f"    - Disk Utilization: {result.metrics['disk_util_percent']:.1f}%"
                            )
                            metrics_printed = True

                    # Handle random read-write test results
                    if result.metrics["test_type"] == "randrw_test":
                        logger.info("  • Random Read-Write Test:")
                        if "read_throughput_mbps" in result.metrics:
                            logger.info(
                                f"    - Read throughput: {result.metrics['read_throughput_mbps']:.2f} MB/s"
                            )
                            metrics_printed = True
                        if "write_throughput_mbps" in result.metrics:
                            logger.info(
                                f"    - Write throughput: {result.metrics['write_throughput_mbps']:.2f} MB/s"
                            )
                            metrics_printed = True
                        if "read_iops" in result.metrics:
                            logger.info(
                                f"    - Read IOPS: {result.metrics['read_iops']:.0f}"
                            )
                            metrics_printed = True
                        if "write_iops" in result.metrics:
                            logger.info(
                                f"    - Write IOPS: {result.metrics['write_iops']:.0f}"
                            )
                            metrics_printed = True
                        if "disk_util_percent" in result.metrics:
                            logger.info(
                                f"    - Disk Utilization: {result.metrics['disk_util_percent']:.1f}%"
                            )
                            metrics_printed = True
                elif result.tool_name == "sysbench":
                    if "read_throughput_mbps" in result.metrics:
                        logger.info(
                            f"  • Read throughput: {result.metrics['read_throughput_mbps']:.2f} MB/s"
                        )
                        metrics_printed = True
                    if "write_throughput_mbps" in result.metrics:
                        logger.info(
                            f"  • Write throughput: {result.metrics['write_throughput_mbps']:.2f} MB/s"
                        )
                        metrics_printed = True
                elif result.tool_name == "ioping":
                    if "latency_avg_us" in result.metrics:
                        logger.info(
                            f"  • Average latency: {result.metrics['latency_avg_us']:.2f} μs"
                        )
                        metrics_printed = True
                    if "latency_min_us" in result.metrics:
                        logger.info(
                            f"  • Minimum latency: {result.metrics['latency_min_us']:.2f} μs"
                        )
                        metrics_printed = True
                    if "latency_max_us" in result.metrics:
                        logger.info(
                            f"  • Maximum latency: {result.metrics['latency_max_us']:.2f} μs"
                        )
                        metrics_printed = True
                    if "latency_mdev_us" in result.metrics:
                        logger.info(
                            f"  • Latency deviation: {result.metrics['latency_mdev_us']:.2f} μs"
                        )
                        metrics_printed = True
                    if "iops" in result.metrics:
                        logger.info(f"  • IOPS: {result.metrics['iops']:.0f}")
                        metrics_printed = True
                    if "throughput_mbps" in result.metrics:
                        logger.info(
                            f"  • Throughput: {result.metrics['throughput_mbps']:.2f} MB/s"
                        )
                        metrics_printed = True
                if not metrics_printed:
                    logger.info("  • No detailed metrics available")
            elif not result.success:
                logger.info(f"  • Error: {result.error_message or 'Unknown error'}")

        # Key performance summary
        if summary["performance_metrics"]:
            logger.info(f"{'=' * 70}")
            logger.info("KEY PERFORMANCE INDICATORS:")
            logger.info(f"{'=' * 70}")

            for key, value in summary["performance_metrics"].items():
                if "latency" in key:
                    logger.info(f"  {self._format_metric_name(key)}: {value:.2f} μs")
                elif "iops" in key:
                    logger.info(f"  {self._format_metric_name(key)}: {value:.0f} IOPS")
                elif "throughput" in key or "speed" in key or "throughput" in key:
                    logger.info(f"  {self._format_metric_name(key)}: {value:.2f} MB/s")
                elif "ops_per_sec" in key:
                    logger.info(f"  {self._format_metric_name(key)}: {value:.2f} ops/s")
                elif "disk_util" in key:
                    logger.info(f"  {self._format_metric_name(key)}: {value:.1f}%")
                else:
                    logger.info(f"  {self._format_metric_name(key)}: {value:.2f}")

        logger.info(f"{'=' * 70}")
        logger.info("📄 FULL REPORTS AVAILABLE:")
        logger.info("  • Complete detailed text report with raw output")
        logger.info("  • JSON format report for analysis and integration")
        logger.info("  • Check 'reports/' directory for timestamped files")
        logger.info(f"{'=' * 70}")

    def _format_metric_name(self, metric_name: str) -> str:
        """Format metric name for display."""
        # Convert snake_case to Title Case
        formatted = metric_name.replace("_", " ").title()

        # Handle common abbreviations
        formatted = formatted.replace("Hdparm", "HDParm")
        formatted = formatted.replace("Fio", "FIO")
        formatted = formatted.replace("Iops", "IOPS")
        formatted = formatted.replace("Mb Per Sec", "MB/s")
        formatted = formatted.replace("Ops Per Sec", "Ops/s")
        formatted = formatted.replace("Avg Latency Us", "Avg Latency (μs)")

        return formatted

    def _format_device_info(self, device_info: Dict[str, Any]) -> List[str]:
        """Format device information for human-readable output."""
        lines = []

        # Device path and basic info
        if "device_path" in device_info:
            lines.append(f"Device Path: {device_info['device_path']}")
        if "mount_point" in device_info:
            lines.append(f"Mount Point: {device_info['mount_point']}")

        # Disk usage information
        if "df_info" in device_info and device_info["df_info"]:
            lines.append("Disk Usage:")
            df_lines = device_info["df_info"].strip().split("\n")
            for line in df_lines:
                if line.strip():
                    lines.append(f"  {line}")

        # Hardware information from lshw
        if "lshw_info" in device_info and device_info["lshw_info"]:
            lines.append("Hardware Information:")
            try:
                import json

                lshw_data = json.loads(device_info["lshw_info"])
                if isinstance(lshw_data, list) and len(lshw_data) > 0:
                    device = lshw_data[0]

                    if "description" in device:
                        lines.append(f"  Description: {device['description']}")
                    if "product" in device:
                        lines.append(f"  Product: {device['product']}")
                    if "vendor" in device:
                        lines.append(f"  Vendor: {device['vendor']}")
                    if "version" in device:
                        lines.append(f"  Version: {device['version']}")
                    if "serial" in device:
                        lines.append(f"  Serial: {device['serial']}")
                    if "size" in device:
                        size_bytes = device["size"]
                        size_gb = size_bytes / (1024**3)
                        lines.append(f"  Size: {size_gb:.2f} GB ({size_bytes:,} bytes)")
                    if "configuration" in device:
                        config = device["configuration"]
                        if "logicalsectorsize" in config:
                            lines.append(
                                f"  Logical Sector Size: {config['logicalsectorsize']} bytes"
                            )
                        if "sectorsize" in config:
                            lines.append(
                                f"  Physical Sector Size: {config['sectorsize']} bytes"
                            )
                        if "signature" in config:
                            lines.append(f"  Disk Signature: {config['signature']}")
                else:
                    lines.append("  No detailed hardware information available")
            except (json.JSONDecodeError, KeyError, TypeError):
                lines.append("  Hardware information format not recognized")

        # Block device information from lsblk
        if "lsblk_info" in device_info and device_info["lsblk_info"]:
            lines.append("Block Device Information:")
            try:
                import json

                lsblk_data = json.loads(device_info["lsblk_info"])
                if "blockdevices" in lsblk_data:
                    for device in lsblk_data["blockdevices"]:
                        if "name" in device:
                            lines.append(f"  Device: /dev/{device['name']}")
                        if "size" in device:
                            lines.append(f"  Size: {device['size']}")
                        if "type" in device:
                            lines.append(f"  Type: {device['type']}")
                        if "mountpoint" in device and device["mountpoint"]:
                            lines.append(f"  Mount Point: {device['mountpoint']}")
                        if "fstype" in device and device["fstype"]:
                            lines.append(f"  Filesystem: {device['fstype']}")
                        if "model" in device and device["model"]:
                            lines.append(f"  Model: {device['model']}")
                        if "serial" in device and device["serial"]:
                            lines.append(f"  Serial: {device['serial']}")
                        if "children" in device:
                            for child in device["children"]:
                                lines.append(
                                    f"    Partition: /dev/{child.get('name', 'unknown')}"
                                )
                                if "size" in child:
                                    lines.append(f"      Size: {child['size']}")
                                if "mountpoint" in child and child["mountpoint"]:
                                    lines.append(
                                        f"      Mount Point: {child['mountpoint']}"
                                    )
                                if "fstype" in child and child["fstype"]:
                                    lines.append(f"      Filesystem: {child['fstype']}")
                else:
                    lines.append("  No block device information available")
            except (json.JSONDecodeError, KeyError, TypeError):
                lines.append("  Block device information format not recognized")

        # Any other device information
        excluded_keys = {
            "device_path",
            "mount_point",
            "df_info",
            "lshw_info",
            "lsblk_info",
        }
        other_info = {k: v for k, v in device_info.items() if k not in excluded_keys}

        if other_info:
            lines.append("Additional Information:")
            for key, value in other_info.items():
                if isinstance(value, str) and value.strip():
                    lines.append(f"  {key.replace('_', ' ').title()}: {value}")
                elif not isinstance(value, str):
                    lines.append(f"  {key.replace('_', ' ').title()}: {value}")

        return lines

    def _format_fio_metrics(self, metrics: Dict[str, Any]) -> List[str]:
        """Format FIO metrics in a human-readable way."""
        lines = []

        # Handle write test metrics
        if metrics["test_type"] == "write_test":
            lines.append("  Write Test Results:")

        # Handle random read-write test metrics
        if metrics["test_type"] == "randrw_test":
            lines.append("  Random Read-Write Test Results:")

        if isinstance(metrics, dict):
            if "read_throughput_mbps" in metrics:
                lines.append(
                    f"    Read throughput: {metrics['read_throughput_mbps']:.2f} MB/s"
                )
            if "write_throughput_mbps" in metrics:
                lines.append(
                    f"    Write throughput: {metrics['write_throughput_mbps']:.2f} MB/s"
                )
            if "read_iops" in metrics:
                lines.append(f"    Read IOPS: {metrics['read_iops']:.0f}")
            if "write_iops" in metrics:
                lines.append(f"    Write IOPS: {metrics['write_iops']:.0f}")
            if "read_lat_mean_ns" in metrics:
                lines.append(
                    f"    Read Latency (mean): {metrics['read_lat_mean_ns']:.0f} ns"
                )
            if "write_lat_mean_ns" in metrics:
                lines.append(
                    f"    Write Latency (mean): {metrics['write_lat_mean_ns']:.0f} ns"
                )
            if "read_lat_stddev_ns" in metrics:
                lines.append(
                    f"    Read Latency (stddev): {metrics['read_lat_stddev_ns']:.0f} ns"
                )
            if "write_lat_stddev_ns" in metrics:
                lines.append(
                    f"    Write Latency (stddev): {metrics['write_lat_stddev_ns']:.0f} ns"
                )
            if "cpu_user_percent" in metrics:
                lines.append(f"    CPU User: {metrics['cpu_user_percent']:.1f}%")
            if "cpu_system_percent" in metrics:
                lines.append(f"    CPU System: {metrics['cpu_system_percent']:.1f}%")
            if "disk_util_percent" in metrics:
                lines.append(
                    f"    Disk Utilization: {metrics['disk_util_percent']:.1f}%"
                )
            if "job_runtime_ms" in metrics:
                lines.append(f"    Job Runtime: {metrics['job_runtime_ms']:.0f} ms")
        else:
            lines.append(f"    {metrics}")

        return lines
