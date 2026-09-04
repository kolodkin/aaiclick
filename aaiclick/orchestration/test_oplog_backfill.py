"""``backfill_registry_names`` — legacy ``p_<name>`` rows gain a registry name."""

from sqlalchemy import text

from aaiclick.data.data_context import list_persistent_objects
from aaiclick.orchestration.oplog_backfill import backfill_registry_names
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.tenancy import DEFAULT_TENANT_ID, active_tenant


async def _insert_legacy_row(table_name: str, tenant_id: int) -> None:
    async with get_sql_session() as session:
        await session.execute(
            text(
                "INSERT INTO table_registry (table_name, tenant_id, created_at, schema_doc) "
                "VALUES (:table_name, :tenant_id, CURRENT_TIMESTAMP, "
                '\'{"fieldtype": "a", "columns": {"value": {"type": "Int64"}}}\')'
            ),
            {"table_name": table_name, "tenant_id": tenant_id},
        )
        await session.commit()


async def test_backfill_names_legacy_rows_per_tenant(orch_ctx):
    """Pre-registry rows become listable and openable under their old names."""
    await _insert_legacy_row("p_orders", DEFAULT_TENANT_ID)
    await _insert_legacy_row("p_7_sales", 7)
    await _insert_legacy_row("p_7501679039461998598", DEFAULT_TENANT_ID)

    await backfill_registry_names()

    assert await list_persistent_objects() == ["orders"]
    with active_tenant(7):
        assert await list_persistent_objects() == ["sales"]
    async with get_sql_session() as session:
        result = await session.execute(
            text("SELECT name FROM table_registry WHERE table_name = 'p_7501679039461998598'")
        )
        assert result.scalar_one() is None


async def test_backfill_is_idempotent_and_keeps_physical_table(orch_ctx):
    """A second pass changes nothing, and the object resolves to its original table."""
    await _insert_legacy_row("p_orders", DEFAULT_TENANT_ID)
    await backfill_registry_names()
    await backfill_registry_names()
    # The CH table itself never existed here; resolution is what matters.
    async with get_sql_session() as session:
        result = await session.execute(text("SELECT table_name FROM table_registry WHERE name = 'orders'"))
        assert result.scalar_one() == "p_orders"
