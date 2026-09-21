"""SQL text helpers shared across orchestration modules."""


def in_clause(ids: list, prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause compatible with both SQLite and PostgreSQL.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params
