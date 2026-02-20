#!/usr/bin/env python3
"""
Install Claude MCP servers from mcp.json configuration.
Removes and re-adds each server to ensure configuration is up to date.

Usage:
    python claude_mcp_setup.py [--config PATH] [server_name ...]

    --config PATH   Path to mcp.json (default: contrib/mcp.json)
    server_name     One or more server names to install (default: all)
"""
import argparse
import json
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def find_claude_cmd() -> str | None:
    claude_path = Path.home() / ".claude" / "local" / "claude"
    if claude_path.exists():
        return str(Path("~/.claude/local/claude").expanduser())

    result = subprocess.run(["which", "claude"], capture_output=True)
    if result.returncode == 0:
        return "claude"

    return None


def setup_mcp_servers(mcp_json_path: str, server_names: list[str] | None = None) -> None:
    try:
        with open(mcp_json_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("MCP config not found: %s", mcp_json_path)
        return
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", mcp_json_path, e)
        return

    if "mcpServers" not in config:
        logger.error("No mcpServers found in %s", mcp_json_path)
        return

    all_servers = config["mcpServers"]

    if server_names:
        unknown = set(server_names) - set(all_servers)
        if unknown:
            logger.error("Unknown server(s): %s. Available: %s", ", ".join(sorted(unknown)), ", ".join(sorted(all_servers)))
            return
        servers = {name: all_servers[name] for name in server_names}
    else:
        servers = all_servers

    claude_cmd = find_claude_cmd()
    if not claude_cmd:
        logger.warning("Claude CLI not found, skipping MCP setup")
        return

    for server_name, server_config in servers.items():
        server_json = json.dumps(server_config, separators=(",", ":"))

        try:
            subprocess.run(
                [claude_cmd, "mcp", "remove", server_name],
                capture_output=True,
            )
        except Exception:
            pass

        try:
            subprocess.run(
                [claude_cmd, "mcp", "add-json", server_name, server_json],
                capture_output=True,
                check=True,
            )
            logger.info("Installed MCP server: %s", server_name)
        except subprocess.CalledProcessError as e:
            logger.warning("Failed to install MCP server %s: %s", server_name, e.stderr.decode().strip())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Install Claude MCP servers from mcp.json configuration.")
    parser.add_argument("--config", default=str(Path(__file__).parent / "mcp.json"), help="Path to mcp.json (default: contrib/mcp.json)")
    parser.add_argument("servers", nargs="*", help="Specific server names to install (default: all)")
    args = parser.parse_args()

    setup_mcp_servers(args.config, args.servers or None)


if __name__ == "__main__":
    main()
