# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive disk benchmarking utility written in Python that tests performance of various storage devices (SD cards, eMMC flash, RAM disks, HDDs, SSDs). The tool provides detailed performance analysis with comprehensive reporting and logging capabilities.

## Key Commands

### Running the benchmark

```bash
# Test physical device
sudo python3 disk_benchmark.py -d /dev/sdXY

# Test RAM disk (automatically created and managed)
sudo python3 disk_benchmark.py --ramdisk

# Use custom configuration
sudo python3 disk_benchmark.py --ramdisk --config my_config.json

# Verbose output
sudo python3 disk_benchmark.py -d /dev/sdXY -v

# Debug mode
sudo python3 disk_benchmark.py --ramdisk --debug
```

Replace `/dev/sdXY` with the actual device path.

### Configuration management

```bash
# Create default configuration file
python3 disk_benchmark.py --create-config

# Create custom configuration file
python3 disk_benchmark.py --create-config --config my_config.json
```

### Development commands

```bash
# Run comprehensive tests
python3 test_comprehensive.py

# Run basic tests
python3 test_basic.py

# Run integration tests
python3 test_integration.py

# Install dependencies
pip3 install -r requirements.txt
```

### Dependencies

**Python dependencies:**

- psutil>=5.8.0 (system resource monitoring)

**System utilities required:**

- hdparm (disk timing and info) - **Core**
- lsblk (block device info) - **Core**
- lshw (hardware info) - **Core**
- mount/umount (partition management) - **Core**
- fio (flexible I/O tester) - **Optional**
- sysbench (system performance benchmark) - **Optional**
- ioping (disk latency measurement) - **Optional**

**Note:** The tool will work with only the core utilities available, but will skip optional benchmarks if their tools are not installed.

## Architecture

The Python implementation includes:

### Core Components

1. **Device Management** (`device_manager.py`): Mount/unmount partitions safely with proper error handling
2. **RAM Disk Management**: Create, mount, unmount, and delete RAM disks automatically
3. **Hardware Detection**: Use lshw, lsblk to gather comprehensive device information
4. **Benchmark Orchestrator** (`benchmarks/orchestrator.py`): Coordinate multiple benchmark tools in sequence
5. **Benchmark Modules** (`benchmarks/`):
   - hdparm (buffered disk reads, device info)
   - dd (direct I/O write performance)
   - fio (advanced I/O patterns, random/sequential, various block sizes)
   - sysbench (file I/O performance)
   - ioping (disk latency measurement)
6. **Report Generator** (`report_generator.py`): Combine all results into a single, well-formatted report
7. **Logging System**: Dual output to stdout and log files with different verbosity levels
8. **Safety Manager** (`safety.py`): Comprehensive safety checks and resource monitoring
9. **Configuration Manager** (`config.py`): JSON-based configuration management

### Key Features

- **Comprehensive Reporting**: Single unified report with all benchmark results (text and JSON)
- **Advanced Logging**: Structured logging to both console and files with timestamps
- **Latency Testing**: ioping integration for disk latency measurements
- **RAM Disk Testing**: Automatic RAM disk creation and management for memory performance testing
- **Safety Features**: Device validation, mount point management, cleanup procedures, resource monitoring
- **Error Handling**: Robust error handling for system utility failures with graceful degradation
- **Configuration**: JSON-based configuration with validation and customizable test parameters
- **Resource Monitoring**: Real-time CPU, memory, and I/O monitoring during benchmarks
- **Interrupt Handling**: Graceful shutdown with proper cleanup on interrupts

### RAM Disk Mode

The `--ramdisk` mode provides automatic RAM disk benchmarking:

- **Size Calculation**: Creates RAM disk using 75% of available free RAM, capped at 8GB maximum
- **Automatic Lifecycle**: Handles creation, formatting, mounting, testing, unmounting, and deletion
- **Memory Detection**: Uses `/proc/meminfo` to determine available memory
- **Filesystem**: Creates tmpfs filesystem for optimal performance
- **Cleanup**: Ensures complete cleanup even if benchmark fails or is interrupted

## Implementation Status

### ✅ **Completed Features**

1. **Core Infrastructure**
   - ✅ Argument parsing with argparse
   - ✅ Comprehensive logging system (console + file)
   - ✅ System utilities interface with subprocess wrapper
   - ✅ Dependency checking for required tools
   - ✅ Privilege validation

2. **Device Management**
   - ✅ Physical device validation and mounting
   - ✅ RAM disk creation and management
   - ✅ Safe cleanup procedures with atexit and signal handlers
   - ✅ Device information gathering (lshw, lsblk, df)

3. **Benchmark Modules**
   - ✅ hdparm (disk timing and device info)
   - ✅ dd (direct I/O write performance)
   - ✅ fio (advanced I/O patterns with JSON output parsing)
   - ✅ sysbench (file I/O performance)
   - ✅ ioping (disk latency measurement)

4. **Safety and Monitoring**
   - ✅ Comprehensive safety checks (disk space, memory, CPU, permissions)
   - ✅ Real-time resource monitoring during benchmarks
   - ✅ Device path validation and critical device protection
   - ✅ Graceful interrupt handling with cleanup

5. **Reporting and Output**
   - ✅ Text and JSON report generation
   - ✅ Performance metrics extraction and calculation
   - ✅ Timestamped output files
   - ✅ Console summary with formatted metrics

6. **Configuration Management**
   - ✅ JSON-based configuration system
   - ✅ Configuration validation
   - ✅ Default configuration creation
   - ✅ Customizable benchmark parameters

### Implementation Notes

- Uses subprocess module for safe system utility interaction
- Implements proper cleanup for temporary files and mount points
- Uses argparse for command-line argument handling
- Uses Python logging module for structured logging
- Results saved to timestamped files with clear naming conventions
- Supports various storage device types (SD, eMMC, RAM disk, etc.)
- Requires psutil for system resource monitoring

### RAM Disk Implementation Details

- Parses `/proc/meminfo` to get `MemAvailable` value
- Calculates RAM disk size: `min(MemAvailable * 0.75, 8GB)`
- Uses `mount -t tmpfs` to create RAM disk
- Mount point: `/mnt/ramdisk_benchmark_<timestamp>`
- Implements signal handlers for cleanup on interruption
- Uses `atexit` module to ensure cleanup on normal exit

## Safety Considerations

- Always verify device path before running to prevent data loss
- Implement proper mount/unmount sequences with error handling
- Create temporary mount points safely (e.g., `/mnt/disk_benchmark_test`)
- Clean up all test files and temporary mounts on exit
- Validate device accessibility and permissions before testing
- Provide clear warnings about destructive operations

### RAM Disk Safety

- Verify sufficient available memory before creating RAM disk
- Implement robust cleanup to prevent memory leaks
- Handle edge cases where system runs out of memory during testing
- Provide clear warnings about RAM disk size and memory usage
- Ensure RAM disk is properly unmounted and freed on all exit paths
