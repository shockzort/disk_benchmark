"""
Configuration management for disk benchmarking tool.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict


logger = logging.getLogger(__name__)


@dataclass
class BenchmarkConfig:
    """Configuration for benchmark execution."""

    # General settings
    max_test_duration: int = 1500  # seconds

    # dd benchmark settings
    dd_block_size: str = "1M"
    dd_count: int = 1000
    dd_flags: str = "direct,fsync"

    # fio benchmark settings - Write test
    fio_write_size: str = "512M"
    fio_write_io_size: str = "10G"
    fio_write_blocksize: str = "4k"
    fio_write_ioengine: str = "libaio"
    fio_write_fsync: int = 10000
    fio_write_iodepth: int = 32
    fio_write_direct: int = 1
    fio_write_numjobs: int = 4
    fio_write_runtime: int = 60
    fio_write_group_reporting: bool = True

    # fio benchmark settings - Random read-write test
    fio_randrw_size: str = "512M"
    fio_randrw_io_size: str = "10G"
    fio_randrw_blocksize: str = "4k"
    fio_randrw_ioengine: str = "libaio"
    fio_randrw_fsync: int = 1
    fio_randrw_iodepth: int = 1
    fio_randrw_direct: int = 1
    fio_randrw_numjobs: int = 4
    fio_randrw_runtime: int = 60
    fio_randrw_group_reporting: bool = True

    # sysbench benchmark settings
    sysbench_file_total_size: str = "1G"
    sysbench_file_num: int = 16
    sysbench_file_block_size: int = 16384
    sysbench_threads: int = 4
    sysbench_max_time: int = 60

    # ioping benchmark settings
    ioping_count: int = 100
    ioping_size: str = "4k"
    ioping_deadline: int = 300

    # RAM disk settings
    ramdisk_size_percent: float = 0.75
    ramdisk_max_size_gb: int = 8

    # Safety settings
    min_free_space_mb: int = 2048
    max_cpu_threshold: float = 80.0
    max_load_threshold: float = 2.0

    # Output settings
    generate_json_report: bool = True
    generate_text_report: bool = True
    detailed_logging: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BenchmarkConfig":
        """Create from dictionary."""
        return cls(**data)

    def to_human_readable(self) -> str:
        """Convert configuration to human-readable format."""
        lines = []

        # General settings
        lines.append("GENERAL SETTINGS")
        lines.append("-" * 20)
        lines.append(f"Max test duration: {self.max_test_duration} seconds")
        lines.append("")

        # DD benchmark settings
        lines.append("DD BENCHMARK SETTINGS")
        lines.append("-" * 25)
        lines.append(f"Block size: {self.dd_block_size}")
        lines.append(f"Count: {self.dd_count}")
        lines.append(f"Flags: {self.dd_flags}")
        lines.append("")

        # FIO Write test settings
        lines.append("FIO WRITE TEST SETTINGS")
        lines.append("-" * 27)
        lines.append(f"File size: {self.fio_write_size}")
        lines.append(f"I/O size: {self.fio_write_io_size}")
        lines.append(f"Block size: {self.fio_write_blocksize}")
        lines.append(f"I/O engine: {self.fio_write_ioengine}")
        lines.append(f"Fsync frequency: {self.fio_write_fsync}")
        lines.append(f"I/O depth: {self.fio_write_iodepth}")
        lines.append(f"Direct I/O: {'Yes' if self.fio_write_direct else 'No'}")
        lines.append(f"Number of jobs: {self.fio_write_numjobs}")
        lines.append(f"Runtime: {self.fio_write_runtime} seconds")
        lines.append("")

        # FIO Random read-write test settings
        lines.append("FIO RANDOM READ-WRITE TEST SETTINGS")
        lines.append("-" * 38)
        lines.append(f"File size: {self.fio_randrw_size}")
        lines.append(f"I/O size: {self.fio_randrw_io_size}")
        lines.append(f"Block size: {self.fio_randrw_blocksize}")
        lines.append(f"I/O engine: {self.fio_randrw_ioengine}")
        lines.append(f"Fsync frequency: {self.fio_randrw_fsync}")
        lines.append(f"I/O depth: {self.fio_randrw_iodepth}")
        lines.append(f"Direct I/O: {'Yes' if self.fio_randrw_direct else 'No'}")
        lines.append(f"Number of jobs: {self.fio_randrw_numjobs}")
        lines.append(f"Runtime: {self.fio_randrw_runtime} seconds")
        lines.append("")

        # Sysbench settings
        lines.append("SYSBENCH SETTINGS")
        lines.append("-" * 18)
        lines.append(f"File total size: {self.sysbench_file_total_size}")
        lines.append(f"Number of files: {self.sysbench_file_num}")
        lines.append(f"File block size: {self.sysbench_file_block_size} bytes")
        lines.append(f"Threads: {self.sysbench_threads}")
        lines.append(f"Max time: {self.sysbench_max_time} seconds")
        lines.append("")

        # Ioping settings
        lines.append("IOPING SETTINGS")
        lines.append("-" * 16)
        lines.append(f"Count: {self.ioping_count}")
        lines.append(f"Request size: {self.ioping_size}")
        lines.append(f"Deadline: {self.ioping_deadline} seconds")
        lines.append("")

        # RAM disk settings
        lines.append("RAM DISK SETTINGS")
        lines.append("-" * 18)
        lines.append(
            f"Size percentage: {self.ramdisk_size_percent * 100:.1f}% of available RAM"
        )
        lines.append(f"Maximum size: {self.ramdisk_max_size_gb} GB")
        lines.append("")

        # Safety settings
        lines.append("SAFETY SETTINGS")
        lines.append("-" * 16)
        lines.append(f"Minimum free space: {self.min_free_space_mb} MB")
        lines.append(f"Max CPU threshold: {self.max_cpu_threshold}%")
        lines.append(f"Max load threshold: {self.max_load_threshold}")
        lines.append("")

        # Output settings
        lines.append("OUTPUT SETTINGS")
        lines.append("-" * 16)
        lines.append(
            f"Generate JSON report: {'Yes' if self.generate_json_report else 'No'}"
        )
        lines.append(
            f"Generate text report: {'Yes' if self.generate_text_report else 'No'}"
        )
        lines.append(f"Detailed logging: {'Yes' if self.detailed_logging else 'No'}")

        return "\n".join(lines)


class ConfigManager:
    """Manages configuration loading and saving."""

    def __init__(self, config_file: Optional[Path] = None):
        self.config_file = config_file or Path("benchmark_config.json")
        self.config = BenchmarkConfig()

    def load_config(self) -> BenchmarkConfig:
        """Load configuration from file."""
        if not self.config_file.exists():
            logger.info(f"Config file {self.config_file} not found, using defaults")
            return self.config

        try:
            with open(self.config_file, "r") as f:
                data = json.load(f)

            self.config = BenchmarkConfig.from_dict(data)
            logger.info(f"Loaded configuration from {self.config_file}")
            return self.config

        except Exception as e:
            logger.error(f"Failed to load config from {self.config_file}: {e}")
            logger.info("Using default configuration")
            return self.config

    def save_config(self, config: Optional[BenchmarkConfig] = None) -> bool:
        """Save configuration to file."""
        config_to_save = config or self.config

        try:
            with open(self.config_file, "w") as f:
                json.dump(config_to_save.to_dict(), f, indent=2)

            logger.info(f"Saved configuration to {self.config_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to save config to {self.config_file}: {e}")
            return False

    def create_default_config(self) -> bool:
        """Create a default configuration file."""
        default_config = BenchmarkConfig()
        return self.save_config(default_config)

    def get_config(self) -> BenchmarkConfig:
        """Get current configuration."""
        return self.config

    def update_config(self, updates: Dict[str, Any]) -> bool:
        """Update configuration with new values."""
        try:
            config_dict = self.config.to_dict()
            config_dict.update(updates)
            self.config = BenchmarkConfig.from_dict(config_dict)
            logger.info("Configuration updated")
            return True

        except Exception as e:
            logger.error(f"Failed to update configuration: {e}")
            return False

    def validate_config(self) -> bool:
        """Validate configuration values."""
        errors = []

        # Validate time limits
        if self.config.max_test_duration <= 0:
            errors.append("max_test_duration must be positive")

        # Validate dd settings
        if self.config.dd_count <= 0:
            errors.append("dd_count must be positive")

        # Validate fio write test settings
        if self.config.fio_write_numjobs <= 0:
            errors.append("fio_write_numjobs must be positive")

        if self.config.fio_write_runtime <= 0:
            errors.append("fio_write_runtime must be positive")

        if self.config.fio_write_iodepth <= 0:
            errors.append("fio_write_iodepth must be positive")

        if self.config.fio_write_fsync <= 0:
            errors.append("fio_write_fsync must be positive")

        if self.config.fio_write_direct not in [0, 1]:
            errors.append("fio_write_direct must be 0 or 1")

        # Validate fio random read-write test settings
        if self.config.fio_randrw_numjobs <= 0:
            errors.append("fio_randrw_numjobs must be positive")

        if self.config.fio_randrw_runtime <= 0:
            errors.append("fio_randrw_runtime must be positive")

        if self.config.fio_randrw_iodepth <= 0:
            errors.append("fio_randrw_iodepth must be positive")

        if self.config.fio_randrw_fsync <= 0:
            errors.append("fio_randrw_fsync must be positive")

        if self.config.fio_randrw_direct not in [0, 1]:
            errors.append("fio_randrw_direct must be 0 or 1")

        # Validate sysbench settings
        if self.config.sysbench_file_num <= 0:
            errors.append("sysbench_file_num must be positive")

        if self.config.sysbench_file_block_size <= 0:
            errors.append("sysbench_file_block_size must be positive")

        if self.config.sysbench_threads <= 0:
            errors.append("sysbench_threads must be positive")

        if self.config.sysbench_max_time <= 0:
            errors.append("sysbench_max_time must be positive")

        # Validate ioping settings
        if self.config.ioping_count <= 0:
            errors.append("ioping_count must be positive")

        if self.config.ioping_deadline <= 0:
            errors.append("ioping_deadline must be positive")

        # Validate RAM disk settings
        if not 0 < self.config.ramdisk_size_percent <= 1:
            errors.append("ramdisk_size_percent must be between 0 and 1")

        if self.config.ramdisk_max_size_gb <= 0:
            errors.append("ramdisk_max_size_gb must be positive")

        # Validate safety settings
        if self.config.min_free_space_mb <= 0:
            errors.append("min_free_space_mb must be positive")

        if self.config.max_cpu_threshold <= 0:
            errors.append("max_cpu_threshold must be positive")

        if self.config.max_load_threshold <= 0:
            errors.append("max_load_threshold must be positive")

        if errors:
            for error in errors:
                logger.error(f"Configuration validation error: {error}")
            return False

        logger.info("Configuration validation passed")
        return True


def get_default_config() -> BenchmarkConfig:
    """Get default configuration."""
    return BenchmarkConfig()


def load_config_from_file(config_file: Path) -> BenchmarkConfig:
    """Load configuration from specified file."""
    manager = ConfigManager(config_file)
    return manager.load_config()


def save_config_to_file(config: BenchmarkConfig, config_file: Path) -> bool:
    """Save configuration to specified file."""
    manager = ConfigManager(config_file)
    return manager.save_config(config)
