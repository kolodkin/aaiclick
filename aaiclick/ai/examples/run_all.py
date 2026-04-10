"""Run all aaiclick AI examples."""

from aaiclick.example_runner import run_all

from .ai_lineage import amain as ai_lineage_example

EXAMPLES = [
    ("AI Lineage", ai_lineage_example),
]


def main():
    run_all(EXAMPLES, "ALL AI EXAMPLES COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
