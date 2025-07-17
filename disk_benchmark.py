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
from report_generator import ReportGenerator
from safety import (
    SafetyManager,
    InterruptHandler,
    ResourceMonitor,
    validate_device_path,
)
from config import ConfigManager


def setup_logging(log_level=logging.INFO):
    """Set up logging to both console and file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"disk_benchmark_{timestamp}.log"

    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / log_filename

    # Configure logging
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

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


def cleanup_handler():
    """Cleanup handler for normal exit."""
    logger = logging.getLogger(__name__)
    logger.info("Performing cleanup...")
    # Device cleanup is handled by individual device managers via atexit


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

    # Register cleanup handlers
    atexit.register(cleanup_handler)
    interrupt_handler.register_cleanup(cleanup_handler)

    logger.info("Starting disk benchmark")
    logger.info(f"Arguments: {args}")

    # Check system dependencies
    if not validate_dependencies():
        logger.error("Required dependencies are missing")
        sys.exit(1)

    # Log system information
    sys_info = get_system_info()
    logger.info(f"System info: {sys_info}")

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
        logger.info(f"Device info: {device_info}")

        # Get mount point for benchmarking
        mount_point = device_manager.get_mount_point()
        logger.info(f"Using mount point: {mount_point}")

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
            print(f"\nWarnings: {len(safety_report['warnings'])}")
            for warning in safety_report["warnings"]:
                print(f"  ⚠️  {warning}")

        # Start resource monitoring
        resource_monitor.start_monitoring()

        # Run benchmarks
        print("\nStarting benchmark tests...")
        results = orchestrator.run_all_benchmarks(device_path, mount_point)

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
        text_report_path = report_generator.generate_report(
            results, enhanced_device_info, sys_info
        )
        json_report_path = report_generator.generate_json_report(
            results, enhanced_device_info, sys_info
        )

        print("\nReports generated:")
        print(f"  Text report: {text_report_path}")
        print(f"  JSON report: {json_report_path}")

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

    except KeyboardInterrupt:
        logger.warning("Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

    logger.info("Benchmark completed successfully")


if __name__ == "__main__":
    main()
