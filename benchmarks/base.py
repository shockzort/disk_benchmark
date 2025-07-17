"""
Base class for benchmark modules.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""

    tool_name: str
    device_path: str
    mount_point: str
    timestamp: str
    success: bool
    duration_seconds: float
    raw_output: str
    error_message: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class BenchmarkModule(ABC):
    """Abstract base class for benchmark modules."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"benchmarks.{name}")
        self.config = None

    def set_config(self, config):
        """Set configuration for this benchmark module."""
        self.config = config

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the benchmark tool is available."""
        pass

    @abstractmethod
    def run(self, device_path: str, mount_point: str) -> BenchmarkResult:
        """Run the benchmark and return results."""
        pass

    def _create_result(
        self,
        device_path: str,
        mount_point: str,
        success: bool = True,
        duration_seconds: float = 0.0,
        raw_output: str = "",
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> BenchmarkResult:
        """Helper method to create benchmark result."""
        return BenchmarkResult(
            tool_name=self.name,
            device_path=device_path,
            mount_point=mount_point,
            timestamp=datetime.now().isoformat(),
            success=success,
            duration_seconds=duration_seconds,
            raw_output=raw_output,
            error_message=error_message,
            metrics=metrics or {},
        )
