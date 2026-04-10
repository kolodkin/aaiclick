"""Run all aaiclick orchestration examples."""

from aaiclick.example_runner import run_all

from .orchestration_basic import amain as orchestration_basic_example
from .orchestration_dynamic import amain as orchestration_dynamic_example

EXAMPLES = [
    ("Orchestration Basic", orchestration_basic_example),
    ("Orchestration Dynamic", orchestration_dynamic_example),
]


def main():
    run_all(EXAMPLES, "ALL ORCHESTRATION EXAMPLES COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
