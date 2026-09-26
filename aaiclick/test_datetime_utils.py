import pytest
from sqlalchemy import DateTime
from sqlmodel import SQLModel

import aaiclick.audit.models  # noqa: F401  # register every table with SQLModel.metadata
import aaiclick.auth.models  # noqa: F401
import aaiclick.orchestration.lifecycle.db_lifecycle  # noqa: F401
import aaiclick.orchestration.models  # noqa: F401
import aaiclick.viewer.models  # noqa: F401

DATETIME_COLUMNS = [
    pytest.param(column, id=f"{table.name}.{column.name}")
    for table in SQLModel.metadata.sorted_tables
    for column in table.columns
    # getattr unwraps a TypeDecorator such as sqlmodel's UTCDateTime.
    if isinstance(getattr(column.type, "impl_instance", column.type), DateTime)
]


@pytest.mark.parametrize("column", DATETIME_COLUMNS)
def test_datetime_columns_are_naive(column):
    """Every SQL datetime column stores naive UTC. sqlmodel >= 0.0.45 maps a bare
    ``datetime`` field to a tz-aware type that rejects naive values, so each
    field must be declared with ``utc_field`` (or an explicit naive ``sa_column``)."""
    assert type(column.type) is DateTime and column.type.timezone is False, repr(column.type)
