#!/usr/bin/env python3
"""
Comprehensive test for the enhanced disk benchmark tool.
"""

import tempfile
import os
from pathlib import Path

from config import ConfigManager, BenchmarkConfig
from utils import get_system_info
from benchmarks.orchestrator import BenchmarkOrchestrator
from report_generator import ReportGenerator
from safety import SafetyManager, ResourceMonitor, validate_device_path


def test_configuration():
    """Test configuration management."""
    print("Testing configuration management...")

    # Test default config
    config = BenchmarkConfig()
    print(f"Default config created: {config.max_test_duration} seconds max duration")

    # Test config manager
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / "test_config.json"
        manager = ConfigManager(config_file)

        # Test save/load
        if manager.save_config(config):
            print("✓ Config saved successfully")
        else:
            print("✗ Config save failed")
            return False

        loaded_config = manager.load_config()
        if loaded_config.max_test_duration == config.max_test_duration:
            print("✓ Config loaded successfully")
        else:
            print("✗ Config load failed")
            return False

        # Test validation
        if manager.validate_config():
            print("✓ Config validation passed")
        else:
            print("✗ Config validation failed")
            return False

    return True


def test_safety_manager():
    """Test safety manager functionality."""
    print("\nTesting safety manager...")

    with tempfile.TemporaryDirectory() as temp_dir:
        safety_manager = SafetyManager()

        # Test individual checks
        if safety_manager.check_disk_space(temp_dir, required_mb=1):
            print("✓ Disk space check passed")
        else:
            print("✗ Disk space check failed")
            return False

        if safety_manager.check_memory_usage():
            print("✓ Memory usage check passed")
        else:
            print("✗ Memory usage check failed")
            return False

        if safety_manager.check_write_permissions(temp_dir):
            print("✓ Write permissions check passed")
        else:
            print("✗ Write permissions check failed")
            return False

        # Test full safety check
        if safety_manager.perform_all_checks(temp_dir):
            print("✓ All safety checks passed")
        else:
            print("✗ Some safety checks failed")
            return False

        # Test safety report
        report = safety_manager.get_safety_report()
        if report["all_checks_passed"]:
            print("✓ Safety report generated successfully")
        else:
            print("✗ Safety report indicates failures")
            return False

    return True


def test_resource_monitor():
    """Test resource monitoring."""
    print("\nTesting resource monitor...")

    monitor = ResourceMonitor(monitor_interval=0.1)

    # Test monitoring
    monitor.start_monitoring()

    # Collect some samples
    for i in range(5):
        sample = monitor.collect_sample()
        if not sample:
            print("✗ Failed to collect resource sample")
            return False

    monitor.stop_monitoring()

    # Test report generation
    report = monitor.get_monitoring_report()
    if "error" in report:
        print(f"✗ Resource monitoring report error: {report['error']}")
        return False

    if report["samples_collected"] > 0:
        print(f"✓ Resource monitoring collected {report['samples_collected']} samples")
    else:
        print("✗ No resource samples collected")
        return False

    return True


def test_enhanced_orchestrator():
    """Test enhanced orchestrator with all benchmarks."""
    print("\nTesting enhanced orchestrator...")

    orchestrator = BenchmarkOrchestrator()
    available_benchmarks = orchestrator.get_available_benchmarks()

    print(f"Available benchmarks: {available_benchmarks}")

    # Should have at least core benchmarks
    if "hdparm" in available_benchmarks and "dd" in available_benchmarks:
        print("✓ Core benchmarks available")
    else:
        print("✗ Core benchmarks missing")
        return False

    # Test with temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test dd benchmark (should work without sudo)
        if "dd" in available_benchmarks:
            print("Testing dd benchmark...")
            result = orchestrator.run_specific_benchmark("dd", "test_device", temp_dir)

            if result and result.success:
                print("✓ dd benchmark completed successfully")
                if result.metrics:
                    print(f"  Metrics: {list(result.metrics.keys())}")
            else:
                print("✗ dd benchmark failed")
                return False

    return True


def test_enhanced_reporting():
    """Test enhanced reporting with safety and monitoring data."""
    print("\nTesting enhanced reporting...")

    # Create fake data for testing
    sys_info = get_system_info()

    device_info = {
        "type": "test",
        "path": "test_device",
        "safety_report": {
            "checks_passed": {"disk_space": True, "memory_usage": True},
            "warnings": ["Test warning"],
            "errors": [],
            "all_checks_passed": True,
        },
        "resource_monitoring": {
            "duration_seconds": 60.0,
            "samples_collected": 60,
            "average_cpu_percent": 25.5,
            "average_memory_percent": 45.2,
            "peak_cpu_percent": 80.1,
            "peak_memory_percent": 67.3,
        },
    }

    # Create orchestrator and get some results
    orchestrator = BenchmarkOrchestrator()

    with tempfile.TemporaryDirectory() as temp_dir:
        if "dd" in orchestrator.get_available_benchmarks():
            results = [
                orchestrator.run_specific_benchmark("dd", "test_device", temp_dir)
            ]

            if results[0] and results[0].success:
                # Test report generation
                report_generator = ReportGenerator()

                # Test text report
                text_report = report_generator.generate_report(
                    results, device_info, sys_info
                )
                if os.path.exists(text_report):
                    print("✓ Enhanced text report generated")
                else:
                    print("✗ Enhanced text report generation failed")
                    return False

                # Test JSON report
                json_report = report_generator.generate_json_report(
                    results, device_info, sys_info
                )
                if os.path.exists(json_report):
                    print("✓ Enhanced JSON report generated")
                else:
                    print("✗ Enhanced JSON report generation failed")
                    return False

                # Test summary
                report_generator.print_summary(results)
                print("✓ Enhanced summary printed")
            else:
                print("✗ Could not generate test results")
                return False
        else:
            print("✗ No benchmarks available for testing")
            return False

    return True


def test_device_validation():
    """Test device path validation."""
    print("\nTesting device validation...")

    # Test with invalid paths
    if not validate_device_path("/nonexistent/device"):
        print("✓ Correctly rejected nonexistent device")
    else:
        print("✗ Failed to reject nonexistent device")
        return False

    # Test with regular file (should fail)
    with tempfile.NamedTemporaryFile() as temp_file:
        if not validate_device_path(temp_file.name):
            print("✓ Correctly rejected regular file")
        else:
            print("✗ Failed to reject regular file")
            return False

    print("✓ Device validation tests passed")
    return True


def main():
    """Run all comprehensive tests."""
    print("Running comprehensive tests for disk benchmark tool...")

    tests = [
        test_configuration,
        test_safety_manager,
        test_resource_monitor,
        test_enhanced_orchestrator,
        test_enhanced_reporting,
        test_device_validation,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
                print("✓ PASSED")
            else:
                print("✗ FAILED")
        except Exception as e:
            print(f"✗ CRASHED: {e}")

    print(f"\nTest Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False


if __name__ == "__main__":
    main()
