#!/usr/bin/env python3
"""
Setup script for running ClickHouse and tests.

This script:
1. Detects if Docker is available
2. If yes, uses docker compose
3. If no, installs ClickHouse via apt-get (Linux) or downloads binary (macOS)
4. Waits for ClickHouse to be ready
5. Runs pytest
"""

import os
import sys
import subprocess
import time
import platform
import shutil
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


def wait_for_clickhouse(host="localhost", port=8123, max_retries=60):
    """Wait for ClickHouse to be ready."""
    print(f"Waiting for ClickHouse at {host}:{port}...")

    for i in range(max_retries):
        try:
            import urllib.request
            response = urllib.request.urlopen(f"http://{host}:{port}/ping", timeout=2)
            if response.status == 200:
                print("✓ ClickHouse is ready!")
                return True
        except Exception:
            pass

        time.sleep(1)
        if (i + 1) % 5 == 0:  # Print every 5 seconds
            print(f"  Still waiting... ({i + 1}/{max_retries})")

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


def install_clickhouse_apt():
    """Install ClickHouse using apt-get on Ubuntu/Debian."""
    print("=" * 60)
    print("Installing ClickHouse via apt-get...")
    print("=" * 60)

    # Check if already installed
    if check_command("clickhouse-server"):
        print("✓ ClickHouse is already installed")
        return True

    print("Adding ClickHouse repository...")
    commands = [
        # Add GPG key
        [
            "sudo",
            "apt-key",
            "adv",
            "--keyserver",
            "hkp://keyserver.ubuntu.com:80",
            "--recv",
            "8919F6BD2B48D754",
        ],
        # Add repository
        [
            "sudo",
            "sh",
            "-c",
            "echo 'deb https://packages.clickhouse.com/deb stable main' > /etc/apt/sources.list.d/clickhouse.list",
        ],
        # Update package list
        ["sudo", "apt-get", "update"],
        # Install ClickHouse
        ["sudo", "apt-get", "install", "-y", "clickhouse-server", "clickhouse-client"],
    ]

    for cmd in commands:
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"✗ Failed: {result.stderr}")
            return False

    print("✓ ClickHouse installed successfully")
    return True


def start_clickhouse_service():
    """Start ClickHouse service."""
    print("=" * 60)
    print("Starting ClickHouse service...")
    print("=" * 60)

    # Try systemctl first
    result = subprocess.run(
        ["sudo", "systemctl", "start", "clickhouse-server"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("✓ ClickHouse service started")
        return wait_for_clickhouse()

    # Fallback to service command
    result = subprocess.run(
        ["sudo", "service", "clickhouse-server", "start"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("✓ ClickHouse service started")
        return wait_for_clickhouse()

    print("✗ Failed to start ClickHouse service")
    print(result.stderr)
    return False


def install_clickhouse_macos():
    """Install ClickHouse on macOS using Homebrew."""
    print("=" * 60)
    print("Installing ClickHouse via Homebrew...")
    print("=" * 60)

    if not check_command("brew"):
        print("✗ Homebrew is not installed")
        print("Please install Homebrew from https://brew.sh")
        return False

    # Check if already installed
    result = subprocess.run(
        ["brew", "list", "clickhouse"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("✓ ClickHouse is already installed")
    else:
        print("Installing ClickHouse...")
        result = subprocess.run(
            ["brew", "install", "clickhouse"],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print(f"✗ Installation failed: {result.stderr}")
            return False

        print("✓ ClickHouse installed successfully")

    # Start ClickHouse
    print("Starting ClickHouse...")
    result = subprocess.run(
        ["brew", "services", "start", "clickhouse"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"✗ Failed to start: {result.stderr}")
        return False

    print("✓ ClickHouse service started")
    return wait_for_clickhouse()


def setup_clickhouse_local():
    """Set up ClickHouse locally based on the platform."""
    system = platform.system().lower()

    if system == "linux":
        # Use apt-get on Linux
        if not install_clickhouse_apt():
            return False
        return start_clickhouse_service()
    elif system == "darwin":
        # Use Homebrew on macOS
        return install_clickhouse_macos()
    else:
        print(f"✗ Unsupported platform: {system}")
        return False


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
        print("✗ Docker not available, installing ClickHouse locally")
        if not setup_clickhouse_local():
            print("\n✗ Failed to set up local ClickHouse")
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
