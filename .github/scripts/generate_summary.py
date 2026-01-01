#!/usr/bin/env python3
"""
Generate GitHub Actions summary from example outputs.

This script automatically discovers example output files and generates
a markdown summary for GitHub Actions.
"""

import sys
from pathlib import Path


# List of examples (in display order)
# Format: (module_name, display_title)
EXAMPLES = [
    ("basic_operators", "Basic Operators Example"),
    ("statistics", "Statistics Example"),
]


def generate_summary():
    """Generate markdown summary from example outputs."""
    summary_parts = []

    # Header
    summary_parts.append("## 📊 Example Outputs\n")

    # Add each example output
    for module_name, display_title in EXAMPLES:
        output_file = f"{module_name}_output.txt"

        summary_parts.append(f"\n### {display_title}\n")
        summary_parts.append("```\n")

        # Read output file
        try:
            with open(output_file, 'r') as f:
                content = f.read()
                summary_parts.append(content)
        except FileNotFoundError:
            summary_parts.append(f"Error: Output file '{output_file}' not found\n")
        except Exception as e:
            summary_parts.append(f"Error reading '{output_file}': {e}\n")

        summary_parts.append("```\n")

    return "".join(summary_parts)


def main():
    """Main entry point."""
    summary = generate_summary()

    # Get GITHUB_STEP_SUMMARY path from environment
    import os
    summary_file = os.getenv('GITHUB_STEP_SUMMARY')

    if summary_file:
        # Write to GitHub Actions summary file
        with open(summary_file, 'a') as f:
            f.write(summary)
        print("✓ Summary written to GITHUB_STEP_SUMMARY")
    else:
        # If not in GitHub Actions, print to stdout
        print(summary)

    return 0


if __name__ == "__main__":
    sys.exit(main())
