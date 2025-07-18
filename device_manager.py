"""
Device management module for disk benchmarking tool.

This module handles device validation, mounting/unmounting, and device information gathering
for both physical devices and RAM disks.
"""

import os
import stat
import logging
import atexit
import time
from datetime import datetime
from typing import Dict, Optional, Any
from abc import ABC, abstractmethod

from utils import (
    run_command,
    CommandExecutionError,
    calculate_ramdisk_size,
    is_device_mounted,
)


logger = logging.getLogger(__name__)


class DeviceManager(ABC):
    """Abstract base class for device management."""

    def __init__(self):
        self.mount_point: Optional[str] = None
        self.is_mounted: bool = False

    @abstractmethod
    def setup(self) -> bool:
        """Set up the device for benchmarking."""
        pass

    @abstractmethod
    def cleanup(self) -> bool:
        """Clean up the device after benchmarking."""
        pass

    @abstractmethod
    def get_device_info(self) -> Dict[str, Any]:
        """Get device information."""
        pass

    def get_mount_point(self) -> Optional[str]:
        """Get the current mount point."""
        return self.mount_point

    def _safe_unmount(
        self,
        device_name: str,
        mount_point: str,
        max_retries: int = 5,
        retry_delay: float = 1.0,
    ) -> bool:
        """Safely unmount a device with retry logic and proper error handling.

        Args:
            device_name: Name of the device for logging (e.g., '/dev/sda1' or 'RAM disk')
            mount_point: Mount point to unmount
            max_retries: Maximum number of unmount attempts
            retry_delay: Delay between retry attempts in seconds

        Returns:
            True if unmount was successful, False otherwise
        """
        if not mount_point:
            logger.warning(f"No mount point specified for {device_name}")
            return False

        logger.info(f"Unmounting {device_name} from {mount_point}")

        for attempt in range(max_retries):
            try:
                # Try to sync filesystem buffers first
                try:
                    run_command(["sync"])
                    logger.debug(f"Filesystem sync completed for {device_name}")
                except CommandExecutionError as e:
                    logger.warning(f"Sync failed for {device_name}: {e}")

                # Attempt to unmount
                run_command(["umount", mount_point])
                self.is_mounted = False
                logger.info(f"Successfully unmounted {device_name}")

                # Remove mount point directory
                if os.path.exists(mount_point):
                    try:
                        os.rmdir(mount_point)
                        logger.info(f"Removed mount point {mount_point}")
                    except OSError as e:
                        logger.warning(
                            f"Could not remove mount point {mount_point}: {e}"
                        )

                return True

            except CommandExecutionError as e:
                attempt_num = attempt + 1
                logger.warning(
                    f"Unmount attempt {attempt_num}/{max_retries} failed for {device_name}: {e}"
                )

                if attempt < max_retries - 1:
                    # Check if mount point is still busy
                    try:
                        stdout, _, _ = run_command(
                            ["lsof", "+D", mount_point], check_return_code=False
                        )
                        if stdout.strip():
                            logger.info(
                                f"Files still open in {mount_point}, processes using it:"
                            )
                            logger.info(stdout)
                    except CommandExecutionError:
                        # lsof might not be available, that's OK
                        pass

                    logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)

                    # Try force unmount on final attempts
                    if attempt >= max_retries - 2:
                        logger.info(f"Attempting force unmount for {device_name}")
                        try:
                            run_command(["umount", "-f", mount_point])
                            self.is_mounted = False
                            logger.info(f"Force unmount successful for {device_name}")

                            # Remove mount point directory
                            if os.path.exists(mount_point):
                                try:
                                    os.rmdir(mount_point)
                                    logger.info(f"Removed mount point {mount_point}")
                                except OSError as e:
                                    logger.warning(
                                        f"Could not remove mount point {mount_point}: {e}"
                                    )

                            return True
                        except CommandExecutionError as force_e:
                            logger.warning(
                                f"Force unmount also failed for {device_name}: {force_e}"
                            )
                else:
                    logger.error(
                        f"All {max_retries} unmount attempts failed for {device_name}"
                    )
                    return False

        return False


class PhysicalDeviceManager(DeviceManager):
    """Manager for physical storage devices."""

    def __init__(self, device_path: str):
        super().__init__()
        self.device_path = device_path
        self.original_mount_point = None

    def validate_device(self) -> bool:
        """Validate that the device path exists and is accessible."""
        if not os.path.exists(self.device_path):
            logger.error(f"Device {self.device_path} does not exist")
            return False

        if not os.access(self.device_path, os.R_OK):
            logger.error(f"Device {self.device_path} is not readable")
            return False

        # Check if it's a block device
        try:
            st = os.stat(self.device_path)
            if not stat.S_ISBLK(st.st_mode):
                logger.error(f"Device {self.device_path} is not a block device")
                return False
        except OSError as e:
            logger.error(f"Cannot stat device path {self.device_path}: {e}")
            return False

        logger.info(f"Device {self.device_path} validation passed")
        return True

    def setup(self) -> bool:
        """Set up the physical device for benchmarking."""
        logger.info(f"Setting up physical device: {self.device_path}")

        # Validate device
        if not self.validate_device():
            return False

        # Check if device is already mounted
        mounted, mount_point = is_device_mounted(self.device_path)

        if mounted:
            logger.info(
                f"Device {self.device_path} is already mounted at {mount_point}"
            )
            self.original_mount_point = mount_point
            self.mount_point = mount_point
            self.is_mounted = True
        else:
            # Create temporary mount point
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.mount_point = f"/mnt/disk_benchmark_{timestamp}"

            try:
                # Create mount point directory
                os.makedirs(self.mount_point, exist_ok=True)

                # Mount the device
                run_command(["mount", self.device_path, self.mount_point])

                logger.info(f"Device {self.device_path} mounted at {self.mount_point}")
                self.is_mounted = True

                # Register cleanup
                atexit.register(self.cleanup)

            except (CommandExecutionError, OSError) as e:
                logger.error(f"Failed to mount device {self.device_path}: {e}")
                self.cleanup()
                return False

        return True

    def cleanup(self) -> bool:
        """Clean up the physical device after benchmarking."""
        success = True

        if self.is_mounted:
            success = self._safe_unmount(self.device_path, self.mount_point)

        return success

    def get_device_info(self) -> Dict[str, Any]:
        """Get physical device information."""
        info = {"device_path": self.device_path, "type": "physical"}

        try:
            # Get device info with lsblk
            stdout, _, _ = run_command(
                [
                    "lsblk",
                    "-J",
                    "-o",
                    "NAME,SIZE,TYPE,FSTYPE,MOUNTPOINT",
                    self.device_path,
                ]
            )
            info["lsblk_info"] = stdout

            # Get hardware info with lshw
            stdout, _, _ = run_command(
                ["lshw", "-class", "disk", "-json"], check_return_code=False
            )
            info["lshw_info"] = stdout

            # Get filesystem info
            if self.mount_point:
                stdout, _, _ = run_command(["df", "-h", self.mount_point])
                info["df_info"] = stdout

        except CommandExecutionError as e:
            logger.warning(f"Could not get complete device info: {e}")

        return info


class RAMDiskManager(DeviceManager):
    """Manager for RAM disk devices."""

    def __init__(self, config=None):
        super().__init__()
        self.size_mb = 0
        self.config = config

    def setup(self) -> bool:
        """Set up the RAM disk for benchmarking."""
        logger.info("Setting up RAM disk")

        # Calculate RAM disk size using configuration if available
        if self.config:
            from utils import get_memory_info

            memory_info = get_memory_info()

            if "MemAvailable" not in memory_info:
                logger.error("Could not determine available memory")
                return False

            available_mb = memory_info["MemAvailable"] / 1024
            ramdisk_mb = int(available_mb * self.config.ramdisk_size_percent)
            max_size_mb = self.config.ramdisk_max_size_gb * 1024
            self.size_mb = min(ramdisk_mb, max_size_mb)

            logger.info(
                f"Using config: {self.config.ramdisk_size_percent*100}% of {available_mb:.0f} MB, max {self.config.ramdisk_max_size_gb} GB"
            )
        else:
            self.size_mb = calculate_ramdisk_size()

        if self.size_mb == 0:
            logger.error("Could not determine RAM disk size")
            return False

        logger.info(f"Creating RAM disk of size {self.size_mb} MB")

        # Create temporary mount point
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.mount_point = f"/mnt/ramdisk_benchmark_{timestamp}"

        try:
            # Create mount point directory
            os.makedirs(self.mount_point, exist_ok=True)

            # Create RAM disk using tmpfs
            size_param = f"size={self.size_mb}M"
            run_command(
                ["mount", "-t", "tmpfs", "-o", size_param, "tmpfs", self.mount_point]
            )

            logger.info(f"RAM disk created and mounted at {self.mount_point}")
            self.is_mounted = True

            # Register cleanup
            atexit.register(self.cleanup)

            return True

        except (CommandExecutionError, OSError) as e:
            logger.error(f"Failed to create RAM disk: {e}")
            self.cleanup()
            return False

    def cleanup(self) -> bool:
        """Clean up the RAM disk after benchmarking."""
        success = True

        if self.is_mounted:
            success = self._safe_unmount("RAM disk", self.mount_point)

        return success

    def get_device_info(self) -> Dict[str, Any]:
        """Get RAM disk information."""
        info = {
            "type": "ramdisk",
            "size_mb": self.size_mb,
            "mount_point": self.mount_point,
        }

        try:
            # Get filesystem info
            if self.mount_point:
                stdout, _, _ = run_command(["df", "-h", self.mount_point])
                info["df_info"] = stdout

        except CommandExecutionError as e:
            logger.warning(f"Could not get complete RAM disk info: {e}")

        return info


def create_device_manager(
    device_path: Optional[str] = None, ramdisk: bool = False, config=None
) -> Optional[DeviceManager]:
    """
    Factory function to create appropriate device manager.

    Args:
        device_path: Path to physical device
        ramdisk: Whether to create RAM disk
        config: Configuration object

    Returns:
        Device manager instance or None on error
    """
    if ramdisk:
        return RAMDiskManager(config)
    elif device_path:
        return PhysicalDeviceManager(device_path)
    else:
        logger.error("Must specify either device_path or ramdisk=True")
        return None
