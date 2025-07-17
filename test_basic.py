#!/usr/bin/env python3
"""
Basic test to verify benchmark modules work without requiring sudo.
"""

from benchmarks.hdparm import HdparmBenchmark
from benchmarks.dd import DdBenchmark
from benchmarks.orchestrator import BenchmarkOrchestrator


def test_hdparm_availability():
    """Test hdparm availability."""
    hdparm = HdparmBenchmark()
    print(f"hdparm available: {hdparm.is_available()}")


def test_dd_availability():
    """Test dd availability."""
    dd = DdBenchmark()
    print(f"dd available: {dd.is_available()}")


def test_orchestrator():
    """Test orchestrator initialization."""
    orchestrator = BenchmarkOrchestrator()
    benchmarks = orchestrator.get_available_benchmarks()
    print(f"Available benchmarks: {benchmarks}")


if __name__ == "__main__":
    print("Testing benchmark modules...")
    test_hdparm_availability()
    test_dd_availability()
    test_orchestrator()
    print("Basic tests completed.")
