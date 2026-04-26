"""Run all aaiclick data examples."""

from aaiclick.example_runner import run_all

from .aggregation_table import amain as aggregation_table_example
from .array_operators import amain as array_operators_example
from .basic_join import amain as basic_join_example
from .basic_operators import amain as basic_operators_example
from .data_manipulation import amain as data_manipulation_example
from .explode import amain as explode_example
from .group_by import amain as group_by_example
from .isin import amain as isin_example
from .nested_arrays import amain as nested_arrays_example
from .nullable import amain as nullable_example
from .order_by import amain as order_by_example
from .selectors import amain as selectors_example
from .statistics import amain as statistics_example
from .transforms import amain as transforms_example
from .views import amain as views_example

EXAMPLES = [
    ("Array Operators", array_operators_example),
    ("Explode", explode_example),
    ("Basic Operators", basic_operators_example),
    ("Data Manipulation", data_manipulation_example),
    ("Nested Arrays", nested_arrays_example),
    ("Statistics", statistics_example),
    ("Transforms", transforms_example),
    ("Views", views_example),
    ("Group By", group_by_example),
    ("Isin", isin_example),
    ("Join", basic_join_example),
    ("Nullable Columns", nullable_example),
    ("Dict Selectors", selectors_example),
    ("Aggregation Table", aggregation_table_example),
    ("Order By", order_by_example),
]


def main():
    run_all(EXAMPLES, "ALL DATA EXAMPLES COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
