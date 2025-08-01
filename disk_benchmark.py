#!/usr/bin/env python3
"""
Disk Benchmarking Tool

A comprehensive disk benchmarking utility that tests performance of various storage devices
(SD cards, eMMC flash, RAM disks, HDDs, SSDs) with detailed reporting and logging.
"""

import argparse
import logging
import os
import sys
import atexit
from datetime import datetime
from pathlib import Path

from utils import validate_dependencies, get_system_info
from device_manager import create_device_manager
from benchmarks.orchestrator import BenchmarkOrchestrator
from safety import (
    SafetyManager,
    InterruptHandler,
    ResourceMonitor,
    validate_device_path,
)
from config import ConfigManager


def setup_logging(log_level=logging.INFO):
    """Set up logging to both console and file with colored output."""
    try:
        from colorama import init

        init(autoreset=True)  # Initialize colorama
        colorama_available = True
    except ImportError:
        colorama_available = False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"disk_benchmark_{timestamp}.log"

    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / log_filename

    # Create a custom formatter for console with colors
    class ColoredFormatter(logging.Formatter):
        """Custom formatter with colors for different log levels using colorama."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if colorama_available:
                from colorama import Fore, Style

                self.COLORS = {
                    "DEBUG": Fore.CYAN,
                    "INFO": Fore.GREEN,
                    "WARNING": Fore.YELLOW,
                    "ERROR": Fore.RED,
                    "CRITICAL": Fore.MAGENTA + Style.BRIGHT,
                }
                self.RESET = Style.RESET_ALL
            else:
                # Fallback to ANSI codes if colorama not available
                self.COLORS = {
                    "DEBUG": "\033[36m",
                    "INFO": "\033[32m",
                    "WARNING": "\033[33m",
                    "ERROR": "\033[31m",
                    "CRITICAL": "\033[35m",
                }
                self.RESET = "\033[0m"

        def format(self, record):
            log_color = self.COLORS.get(record.levelname, "")
            reset = self.RESET

            # Apply color to level name and logger name
            colored_levelname = f"{log_color}{record.levelname}{reset}"
            colored_name = f"{log_color}{record.name}{reset}"

            # Create a copy of the record to avoid modifying the original
            new_record = logging.makeLogRecord(record.__dict__)
            new_record.levelname = colored_levelname
            new_record.name = colored_name

            return super().format(new_record)

    # File formatter (no colors)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Console formatter (with colors)
    console_formatter = ColoredFormatter(
        "%(levelname)s %(asctime)s - %(name)s - %(message)s"
    )

    # Create handlers
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()  # Remove any existing handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger(__name__)
    logger.info(f"Disk Benchmark Tool started - Log file: {log_file}")
    return logger


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Comprehensive disk benchmarking tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  sudo python3 disk_benchmark.py -d /dev/sda1    # Test physical device
  sudo python3 disk_benchmark.py --ramdisk       # Test RAM disk
  sudo python3 disk_benchmark.py -d /dev/sdb1 -v # Verbose output
        """,
    )

    device_group = parser.add_mutually_exclusive_group(required=False)
    device_group.add_argument(
        "-d", "--device", help="Device path to benchmark (e.g., /dev/sda1)"
    )
    device_group.add_argument(
        "--ramdisk",
        action="store_true",
        help="Create and benchmark RAM disk automatically",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument(
        "--create-config",
        action="store_true",
        help="Create default configuration file and exit",
    )

    return parser.parse_args()


def check_privileges():
    """Check if running with required privileges."""
    if os.geteuid() != 0:
        print("Error: This script must be run with sudo privileges", file=sys.stderr)
        print("Usage: sudo python3 disk_benchmark.py [options]", file=sys.stderr)
        sys.exit(1)


def cleanup_handler(benchmark_dir=None):
    """Cleanup handler for normal exit."""
    logger = logging.getLogger(__name__)
    logger.info("Performing cleanup...")
    # Device cleanup is handled by individual device managers via atexit

    # Clean up temporary directory if provided
    if benchmark_dir:
        cleanup_temp_dir(benchmark_dir)


def create_temp_dir(mount_point: str) -> str:
    """Create temporary directory for benchmark files."""
    import os
    from datetime import datetime

    logger = logging.getLogger(__name__)
    temp_dir = ""

    try:
        # Create temporary directory in the mount point
        temp_dir_name = f"benchmark_temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        temp_dir = os.path.join(mount_point, temp_dir_name)
        os.makedirs(temp_dir, exist_ok=True)

        logger.info(f"Created temporary directory for benchmarks: {temp_dir}")
        print(f"Using temporary directory: {temp_dir}")
        return temp_dir

    except Exception as e:
        logger.error(f"Failed to create temporary directory: {e}")
        return ""


def cleanup_temp_dir(temp_dir_path: str) -> None:
    """Clean up temporary directory and all its contents."""
    import os
    import shutil

    logger = logging.getLogger(__name__)
    if not temp_dir_path:
        return

    try:
        # Only remove if it's actually a temp directory we created
        if os.path.basename(temp_dir_path).startswith("benchmark_temp_"):
            if os.path.exists(temp_dir_path):
                shutil.rmtree(temp_dir_path)
                logger.info(f"Cleaned up temporary directory: {temp_dir_path}")
                print(f"Cleaned up temporary directory: {temp_dir_path}")
            else:
                logger.debug(f"Temporary directory already removed: {temp_dir_path}")
        else:
            logger.debug(f"Skipping cleanup of non-temp directory: {temp_dir_path}")

    except Exception as e:
        logger.error(f"Failed to clean up temporary directory {temp_dir_path}: {e}")


# Signal handling is now done by InterruptHandler class


def main():
    """Main entry point."""
    # Parse arguments first to check for config creation
    args = parse_arguments()

    # Handle config creation (doesn't require sudo)
    if args.create_config:
        config_manager = ConfigManager(Path(args.config) if args.config else None)
        if config_manager.create_default_config():
            print(f"Created default configuration file: {config_manager.config_file}")
            sys.exit(0)
        else:
            print("Failed to create configuration file")
            sys.exit(1)

    # Check that either device or ramdisk is specified for benchmarking
    if not args.device and not args.ramdisk:
        print("Error: Must specify either --device or --ramdisk for benchmarking")
        sys.exit(1)

    # Check privileges for actual benchmarking
    check_privileges()

    # Set up logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = setup_logging(log_level)

    # Load configuration
    config_manager = ConfigManager(Path(args.config) if args.config else None)
    config = config_manager.load_config()

    if not config_manager.validate_config():
        logger.error("Configuration validation failed")
        sys.exit(1)

    # Set up interrupt handler and resource monitor
    interrupt_handler = InterruptHandler()
    resource_monitor = ResourceMonitor()

    logger.info("Starting disk benchmark")
    logger.info(f"Arguments: {args}")

    # Check system dependencies
    if not validate_dependencies():
        logger.error("Required dependencies are missing")
        sys.exit(1)

    # Log system information
    sys_info = get_system_info()
    logger.info(f"System info: {sys_info}")

    benchmark_dir = None

    try:
        # Additional safety checks for physical devices
        if args.device and not validate_device_path(args.device):
            logger.error("Device path validation failed")
            sys.exit(1)

        # Create device manager
        device_manager = create_device_manager(
            device_path=args.device, ramdisk=args.ramdisk, config=config
        )

        if not device_manager:
            logger.error("Failed to create device manager")
            sys.exit(1)

        # Set up the device
        if not device_manager.setup():
            logger.error("Failed to set up device")
            sys.exit(1)

        # Get device information
        device_info = device_manager.get_device_info()
        logger.info("Device information:")

        # Log device info in a more readable format
        from report_generator import ReportGenerator

        report_gen = ReportGenerator()
        device_info_lines = report_gen._format_device_info(device_info)
        for line in device_info_lines:
            logger.info(line)

        # Also print formatted device info to console
        print(f"\n{'='*60}")
        print("DEVICE INFORMATION")
        print(f"{'='*60}")
        for line in device_info_lines:
            print(line)
        print(f"{'='*60}")

        # Get mount point for benchmarking
        mount_point = device_manager.get_mount_point()
        logger.info(f"Using mount point: {mount_point}")

        # Create temporary directory for benchmarks
        benchmark_dir = create_temp_dir(mount_point)
        if benchmark_dir is None or benchmark_dir == "":
            logger.error("Failed to create temporary directory for benchmarks")
            sys.exit(1)

        atexit.register(lambda: cleanup_handler(benchmark_dir))
        interrupt_handler.register_cleanup(lambda: cleanup_handler(benchmark_dir))

        if args.ramdisk:
            logger.info("RAM disk benchmarking mode - device setup complete")
            print(f"RAM disk created and mounted at: {mount_point}")
            device_path = "ramdisk"
            ramdisk_size = (
                device_manager.size_mb if hasattr(device_manager, "size_mb") else 0
            )
        else:
            logger.info("Physical device benchmarking mode - device setup complete")
            print(f"Physical device {args.device} ready at: {mount_point}")
            device_path = args.device
            ramdisk_size = 0

        # Initialize benchmark orchestrator
        orchestrator = BenchmarkOrchestrator(config, is_ramdisk=args.ramdisk)
        available_benchmarks = orchestrator.get_available_benchmarks()

        if not available_benchmarks:
            logger.error("No benchmark tools are available")
            print("Error: No benchmark tools are available")
            sys.exit(1)

        logger.info(f"Available benchmarks: {available_benchmarks}")
        print(f"Available benchmarks: {', '.join(available_benchmarks)}")

        # Perform safety checks
        safety_manager = SafetyManager(config)
        safety_passed = safety_manager.perform_all_checks(
            mount_point=mount_point,
            ramdisk_size_mb=ramdisk_size,
            required_tools=available_benchmarks,
        )

        if not safety_passed:
            logger.error("Safety checks failed")
            print("Error: Safety checks failed. See log for details.")
            sys.exit(1)

        # Display safety report
        safety_report = safety_manager.get_safety_report()
        if safety_report["warnings"]:
            print(f"\n⚠️ Warnings: {len(safety_report['warnings'])}")
            for warning in safety_report["warnings"]:
                print(f"  🔶 {warning}")

        # Start resource monitoring
        resource_monitor.start_monitoring()

        # Run benchmarks
        print("\nStarting benchmark tests...")
        results = orchestrator.run_all_benchmarks(device_path, benchmark_dir)

        # Stop resource monitoring
        resource_monitor.stop_monitoring()
        monitoring_report = resource_monitor.get_monitoring_report()

        # Generate reports
        report_generator = ReportGenerator()

        # Print summary to console
        report_generator.print_summary(results)

        # Add safety and monitoring info to reports
        enhanced_device_info = device_info.copy()
        enhanced_device_info["safety_report"] = safety_report
        enhanced_device_info["resource_monitoring"] = monitoring_report

        # Generate detailed reports
        report_generator.generate_report(
            results, enhanced_device_info, sys_info, config
        )
        report_generator.generate_json_report(
            results, enhanced_device_info, sys_info, config
        )

        # Display resource monitoring summary
        if monitoring_report and "error" not in monitoring_report:
            print("\nResource Usage During Benchmarking:")
            print(f"  Average CPU: {monitoring_report['average_cpu_percent']:.1f}%")
            print(
                f"  Average Memory: {monitoring_report['average_memory_percent']:.1f}%"
            )
            print(f"  Peak CPU: {monitoring_report['peak_cpu_percent']:.1f}%")
            print(f"  Peak Memory: {monitoring_report['peak_memory_percent']:.1f}%")

        logger.info("Benchmark completed successfully")

        # Clean up temporary directory
        cleanup_temp_dir(benchmark_dir)

    except KeyboardInterrupt:
        logger.warning("Benchmark interrupted by user")
        cleanup_temp_dir(benchmark_dir)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        cleanup_temp_dir(benchmark_dir)
        sys.exit(1)

    logger.info("Benchmark completed successfully")


if __name__ == "__main__":
    main()
