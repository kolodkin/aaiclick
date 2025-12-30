#!/usr/bin/env python3
"""
Setup script for running ClickHouse and tests.

This script:
1. Detects if Docker is available
2. If yes, uses docker compose
3. If no, downloads and runs ClickHouse locally
4. Waits for ClickHouse to be ready
5. Runs pytest
"""

import os
import sys
import subprocess
import time
import platform
import shutil
import urllib.request
import tarfile
from pathlib import Path


def check_command(cmd):
    """Check if a command is available."""
    return shutil.which(cmd) is not None


def check_docker():
    """Check if Docker is available and working."""
    if not check_command("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def wait_for_clickhouse(host="localhost", port=8123, max_retries=30):
    """Wait for ClickHouse to be ready."""
    print(f"Waiting for ClickHouse at {host}:{port}...")

    for i in range(max_retries):
        try:
            import urllib.request
            response = urllib.request.urlopen(f"http://{host}:{port}/ping", timeout=1)
            if response.status == 200:
                print("✓ ClickHouse is ready!")
                return True
        except Exception:
            pass

        time.sleep(1)
        print(f"  Waiting... ({i + 1}/{max_retries})")

    print("✗ ClickHouse did not start in time")
    return False


def start_clickhouse_docker():
    """Start ClickHouse using Docker Compose."""
    print("=" * 60)
    print("Starting ClickHouse with Docker Compose...")
    print("=" * 60)

    result = subprocess.run(
        ["docker", "compose", "up", "-d"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False

    print("✓ Docker Compose started")
    return wait_for_clickhouse()


def download_clickhouse():
    """Download ClickHouse binary for the current platform."""
    system = platform.system().lower()
    machine = platform.machine().lower()

    print("=" * 60)
    print("Downloading ClickHouse binary...")
    print(f"Platform: {system} {machine}")
    print("=" * 60)

    # Use stable release version
    version = "25.11.5.42"

    # ClickHouse download URLs (stable releases)
    if system == "linux" and machine in ["x86_64", "amd64"]:
        url = f"https://github.com/ClickHouse/ClickHouse/releases/download/v{version}/clickhouse-linux-amd64"
    elif system == "linux" and machine in ["aarch64", "arm64"]:
        url = f"https://github.com/ClickHouse/ClickHouse/releases/download/v{version}/clickhouse-linux-aarch64"
    elif system == "darwin" and machine in ["x86_64", "amd64"]:
        url = f"https://github.com/ClickHouse/ClickHouse/releases/download/v{version}/clickhouse-macos-amd64"
    elif system == "darwin" and machine in ["arm64", "aarch64"]:
        url = f"https://github.com/ClickHouse/ClickHouse/releases/download/v{version}/clickhouse-macos-aarch64"
    else:
        print(f"✗ Unsupported platform: {system} {machine}")
        return None

    clickhouse_dir = Path.home() / ".aaiclick" / "clickhouse"
    clickhouse_dir.mkdir(parents=True, exist_ok=True)

    clickhouse_bin = clickhouse_dir / "clickhouse"

    if clickhouse_bin.exists():
        print("✓ ClickHouse binary already exists")
        return clickhouse_bin

    print(f"Downloading from {url}...")
    try:
        urllib.request.urlretrieve(url, clickhouse_bin)
        clickhouse_bin.chmod(0o755)
        print("✓ Downloaded successfully")
        return clickhouse_bin
    except Exception as e:
        print(f"✗ Download failed: {e}")
        return None


def start_clickhouse_local(clickhouse_bin):
    """Start ClickHouse locally."""
    print("=" * 60)
    print("Starting ClickHouse locally...")
    print("=" * 60)

    clickhouse_dir = Path.home() / ".aaiclick" / "clickhouse"
    data_dir = clickhouse_dir / "data"
    log_file = clickhouse_dir / "clickhouse.log"
    pid_file = clickhouse_dir / "clickhouse.pid"

    data_dir.mkdir(parents=True, exist_ok=True)

    # Check if already running
    if pid_file.exists():
        try:
            with open(pid_file) as f:
                pid = int(f.read().strip())
            # Check if process is running
            os.kill(pid, 0)
            print("✓ ClickHouse is already running")
            return wait_for_clickhouse()
        except (OSError, ValueError):
            pid_file.unlink()

    # Start ClickHouse server
    cmd = [
        str(clickhouse_bin),
        "server",
        "--",
        f"--path={data_dir}",
        f"--http_port=8123",
        f"--tcp_port=9000",
    ]

    print(f"Starting: {' '.join(cmd)}")

    with open(log_file, "w") as log:
        process = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    # Save PID
    with open(pid_file, "w") as f:
        f.write(str(process.pid))

    print(f"✓ Started with PID {process.pid}")
    print(f"  Log file: {log_file}")

    return wait_for_clickhouse()


def run_tests():
    """Run pytest."""
    print("\n" + "=" * 60)
    print("Running tests...")
    print("=" * 60 + "\n")

    result = subprocess.run(
        ["pytest", "-v"],
        env={**os.environ, "PYTEST_SKIP_DOCKER_SETUP": "1"},
    )

    return result.returncode


def main():
    """Main entry point."""
    print("\n" + "=" * 60)
    print("aaiclick Test Setup")
    print("=" * 60 + "\n")

    # Check for Docker
    has_docker = check_docker()

    if has_docker:
        print("✓ Docker is available")
        if not start_clickhouse_docker():
            print("\n✗ Failed to start ClickHouse with Docker")
            sys.exit(1)
    else:
        print("✗ Docker not available, using local ClickHouse")
        clickhouse_bin = download_clickhouse()
        if not clickhouse_bin:
            print("\n✗ Failed to download ClickHouse")
            sys.exit(1)

        if not start_clickhouse_local(clickhouse_bin):
            print("\n✗ Failed to start local ClickHouse")
            sys.exit(1)

    # Run tests
    exit_code = run_tests()

    if exit_code == 0:
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print(f"✗ Tests failed with exit code {exit_code}")
        print("=" * 60)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
