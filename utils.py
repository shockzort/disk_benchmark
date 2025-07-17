"""
Utility functions for disk benchmarking tool.

This module provides safe subprocess execution, dependency checking,
and system utility interactions.
"""

import subprocess
import logging
import shutil
import os
from typing import Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


class CommandExecutionError(Exception):
    """Raised when a command execution fails."""

    pass


def run_command(
    command: List[str], timeout: int = 300, check_return_code: bool = True
) -> Tuple[str, str, int]:
    """
    Execute a system command safely with proper error handling.

    Args:
        command: List of command arguments
        timeout: Command timeout in seconds
        check_return_code: Whether to raise exception on non-zero return code

    Returns:
        Tuple of (stdout, stderr, return_code)

    Raises:
        CommandExecutionError: If command fails and check_return_code is True
    """
    logger.debug(f"Executing command: {' '.join(command)}")

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,  # We'll handle return code ourselves
        )

        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        return_code = result.returncode

        logger.debug(f"Command completed with return code: {return_code}")

        if check_return_code and return_code != 0:
            error_msg = f"Command failed: {' '.join(command)}\nReturn code: {return_code}\nStderr: {stderr}"
            logger.error(error_msg)
            raise CommandExecutionError(error_msg)

        return stdout, stderr, return_code

    except subprocess.TimeoutExpired:
        error_msg = f"Command timed out after {timeout} seconds: {' '.join(command)}"
        logger.error(error_msg)
        raise CommandExecutionError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error executing command {' '.join(command)}: {e}"
        logger.error(error_msg)
        raise CommandExecutionError(error_msg)


def check_command_exists(command: str) -> bool:
    """
    Check if a command exists in the system PATH.

    Args:
        command: Command name to check

    Returns:
        True if command exists, False otherwise
    """
    return shutil.which(command) is not None


def get_system_info() -> Dict[str, str]:
    """
    Get basic system information.

    Returns:
        Dictionary with system information
    """
    info = {}

    try:
        # Get kernel version
        stdout, _, _ = run_command(["uname", "-r"])
        info["kernel"] = stdout

        # Get OS information
        if os.path.exists("/etc/os-release"):
            with open("/etc/os-release", "r") as f:
                for line in f:
                    if line.startswith("PRETTY_NAME="):
                        info["os"] = line.split("=", 1)[1].strip('"')
                        break

        # Get architecture
        stdout, _, _ = run_command(["uname", "-m"])
        info["arch"] = stdout

    except Exception as e:
        logger.warning(f"Could not get system info: {e}")

    return info


def check_dependencies() -> Dict[str, bool]:
    """
    Check if all required system utilities are available.

    Returns:
        Dictionary mapping utility names to their availability status
    """
    required_tools = [
        "hdparm",
        "lsblk",
        "lshw",
        "fio",
        "sysbench",
        "ioping",
        "mount",
        "umount",
        "df",
        "sync",
    ]

    availability = {}

    for tool in required_tools:
        available = check_command_exists(tool)
        availability[tool] = available

        if available:
            logger.debug(f"✓ {tool} is available")
        else:
            logger.warning(f"✗ {tool} is not available")

    return availability


def validate_dependencies() -> bool:
    """
    Validate all required dependencies are available.

    Returns:
        True if all dependencies are available, False otherwise
    """
    dependencies = check_dependencies()
    missing = [tool for tool, available in dependencies.items() if not available]

    if missing:
        logger.error(f"Missing required utilities: {', '.join(missing)}")
        print(f"Error: Missing required utilities: {', '.join(missing)}")
        print("Please install the missing utilities before running the benchmark.")
        return False

    logger.info("All required dependencies are available")
    return True


def get_memory_info() -> Dict[str, int]:
    """
    Get system memory information from /proc/meminfo.

    Returns:
        Dictionary with memory information in KB
    """
    memory_info = {}

    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith(("MemTotal:", "MemFree:", "MemAvailable:")):
                    key, value = line.split(":", 1)
                    # Extract numeric value (remove 'kB' suffix)
                    value_kb = int(value.strip().split()[0])
                    memory_info[key] = value_kb
    except Exception as e:
        logger.error(f"Could not read memory information: {e}")

    return memory_info


def calculate_ramdisk_size() -> int:
    """
    Calculate optimal RAM disk size (75% of available RAM, max 8GB).

    Returns:
        RAM disk size in MB
    """
    memory_info = get_memory_info()

    if "MemAvailable" not in memory_info:
        logger.error("Could not determine available memory")
        return 0

    available_kb = memory_info["MemAvailable"]
    available_mb = available_kb // 1024

    # Calculate 75% of available memory
    ramdisk_mb = int(available_mb * 0.75)

    # Cap at 8GB (8192 MB)
    max_size_mb = 8192
    ramdisk_mb = min(ramdisk_mb, max_size_mb)

    logger.info(f"Available memory: {available_mb} MB")
    logger.info(f"Calculated RAM disk size: {ramdisk_mb} MB")

    return ramdisk_mb


def is_device_mounted(device_path: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a device is mounted and return mount point.

    Args:
        device_path: Path to the device

    Returns:
        Tuple of (is_mounted, mount_point)
    """
    try:
        stdout, _, _ = run_command(["lsblk", "-o", "MOUNTPOINT", "-nr", device_path])
        mount_point = stdout.strip()

        if mount_point and mount_point != "":
            return True, mount_point
        else:
            return False, None

    except CommandExecutionError:
        return False, None
