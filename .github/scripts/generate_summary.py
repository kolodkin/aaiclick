#!/usr/bin/env python3
"""
Generate GitHub Actions summary from example outputs.

This script automatically discovers example output files and generates
a markdown summary for GitHub Actions.
"""

import sys
from pathlib import Path


# Display order and titles for examples
# If an example is not listed here, it will be auto-discovered and displayed
# with a title derived from the filename
EXAMPLE_METADATA = {
    "basic_operators": "Basic Operators Example",
    "statistics": "Statistics Example",
}

# Preferred display order (examples not in this list appear at the end)
DISPLAY_ORDER = [
    "basic_operators",
    "statistics",
]


def get_example_title(module_name):
    """Get display title for an example."""
    if module_name in EXAMPLE_METADATA:
        return EXAMPLE_METADATA[module_name]
    # Auto-generate title from module name
    return " ".join(word.capitalize() for word in module_name.split("_")) + " Example"


def discover_example_outputs():
    """Discover all example output files."""
    output_files = sorted(Path("tmp").glob("*_output.txt"))

    # Extract module names
    examples = []
    for output_file in output_files:
        module_name = output_file.stem.replace("_output", "")
        examples.append(module_name)

    # Sort by display order
    def sort_key(module_name):
        try:
            return (0, DISPLAY_ORDER.index(module_name))
        except ValueError:
            return (1, module_name)  # Not in order list, sort alphabetically

    return sorted(examples, key=sort_key)


def generate_summary():
    """Generate markdown summary from example outputs."""
    summary_parts = []

    # Header
    summary_parts.append("## 📊 Example Outputs\n")

    # Discover and process all examples
    examples = discover_example_outputs()

    if not examples:
        summary_parts.append("\n⚠️ No example output files found\n")
        return "".join(summary_parts)

    # Add each example output
    for module_name in examples:
        output_file = f"tmp/{module_name}_output.txt"
        display_title = get_example_title(module_name)

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
