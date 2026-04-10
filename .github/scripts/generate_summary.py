#!/usr/bin/env python3
"""
Generate GitHub Actions summary from example outputs.

run_all.py already outputs markdown with collapsible <details> sections,
so this script just passes the content through to GITHUB_STEP_SUMMARY.
"""

import argparse
import os
import sys
from pathlib import Path


def generate_summary(title: str):
    """Generate markdown summary from example output."""
    summary_parts = []
    summary_parts.append(f"## {title}\n")

    output_file = Path("tmp/examples_output.txt")
    if not output_file.exists():
        summary_parts.append("\nNo example output file found\n")
        return "".join(summary_parts)

    content = output_file.read_text()
    if not content.strip():
        summary_parts.append("\nExample output file is empty\n")
        return "".join(summary_parts)

    summary_parts.append(content)
    return "".join(summary_parts)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--title", default="Examples Output")
    args = parser.parse_args()

    summary = generate_summary(args.title)

    summary_file = os.getenv('GITHUB_STEP_SUMMARY')

    if summary_file:
        with open(summary_file, 'a') as f:
            f.write(summary)
        print("Summary written to GITHUB_STEP_SUMMARY")
    else:
        print(summary)

    return 0


if __name__ == "__main__":
    sys.exit(main())
