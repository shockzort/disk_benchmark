#!/usr/bin/env python3
"""
Integration test for disk benchmark tool without requiring sudo.
"""

import tempfile
from utils import get_system_info
from benchmarks.orchestrator import BenchmarkOrchestrator
from report_generator import ReportGenerator


def test_integration():
    """Test the integration of components."""
    print("Testing integration without sudo...")

    # Get system info
    sys_info = get_system_info()
    print(f"System info: {sys_info}")

    # Create temporary directory to simulate mount point
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")

        # Create orchestrator
        orchestrator = BenchmarkOrchestrator()
        available_benchmarks = orchestrator.get_available_benchmarks()
        print(f"Available benchmarks: {available_benchmarks}")

        # Test dd benchmark on temporary directory (should work without sudo)
        if "dd" in available_benchmarks:
            print("Testing dd benchmark...")
            try:
                result = orchestrator.run_specific_benchmark(
                    "dd", "test_device", temp_dir
                )
                if result:
                    print(f"dd benchmark result: {result.success}")
                    if result.metrics:
                        print(f"dd metrics: {result.metrics}")
                else:
                    print("dd benchmark failed")
            except Exception as e:
                print(f"dd benchmark error: {e}")

        # Test report generation
        print("Testing report generation...")
        device_info = {"type": "test", "path": "test_device"}

        # Create some fake results for testing
        fake_results = []
        if orchestrator.get_results():
            fake_results = orchestrator.get_results()

        if fake_results:
            report_generator = ReportGenerator()
            report_generator.print_summary(fake_results)

            # Generate reports
            text_report = report_generator.generate_report(
                fake_results, device_info, sys_info
            )
            json_report = report_generator.generate_json_report(
                fake_results, device_info, sys_info
            )

            print(f"Generated text report: {text_report}")
            print(f"Generated JSON report: {json_report}")
        else:
            print("No results to generate report")


if __name__ == "__main__":
    test_integration()
