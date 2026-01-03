"""
Tests for SQL template loader functionality.
"""

from aaiclick.sql_template_loader import load_sql_template


def test_load_apply_op_array_template():
    """Test loading the apply operation array template."""
    template = load_sql_template("apply_op_array")

    # Verify template contains expected placeholders
    assert "{result_table}" in template
    assert "{expression}" in template
    assert "{left_table}" in template
    assert "{right_table}" in template

    # Verify template contains expected SQL keywords
    assert "CREATE TABLE" in template
    assert "MergeTree" in template
    assert "row_number()" in template
    assert "INNER JOIN" in template
    assert "aai_id" in template


def test_load_apply_op_scalar_template():
    """Test loading the apply operation scalar template."""
    template = load_sql_template("apply_op_scalar")

    # Verify template contains expected placeholders
    assert "{result_table}" in template
    assert "{expression}" in template
    assert "{left_table}" in template
    assert "{right_table}" in template

    # Verify template contains expected SQL keywords
    assert "CREATE TABLE" in template
    assert "MergeTree" in template


def test_template_format_array():
    """Test formatting the array template with actual values."""
    template = load_sql_template("apply_op_array")

    # Format template
    sql = template.format(
        result_table="result_table",
        expression="a.value + b.value",
        left_table="left_table",
        right_table="right_table",
        ttl_clause="TTL created_at + INTERVAL 1 DAY"
    )

    # Verify placeholders are replaced
    assert "{result_table}" not in sql
    assert "{expression}" not in sql
    assert "{left_table}" not in sql
    assert "{right_table}" not in sql
    assert "{ttl_clause}" not in sql

    # Verify formatted values are present
    assert "result_table" in sql
    assert "a.value + b.value" in sql
    assert "left_table" in sql
    assert "right_table" in sql
    assert "TTL created_at + INTERVAL 1 DAY" in sql


def test_template_format_scalar():
    """Test formatting the scalar template with actual values."""
    template = load_sql_template("apply_op_scalar")

    # Format template
    sql = template.format(
        result_table="result_table",
        expression="a.value - b.value",
        left_table="left_table",
        right_table="right_table",
        ttl_clause="TTL created_at + INTERVAL 1 DAY"
    )

    # Verify placeholders are replaced
    assert "{result_table}" not in sql
    assert "{expression}" not in sql
    assert "{left_table}" not in sql
    assert "{right_table}" not in sql
    assert "{ttl_clause}" not in sql

    # Verify formatted values are present
    assert "result_table" in sql
    assert "a.value - b.value" in sql
    assert "left_table" in sql
    assert "right_table" in sql
    assert "TTL created_at + INTERVAL 1 DAY" in sql


def test_template_caching():
    """Test that templates are cached properly."""
    # Load template twice
    template1 = load_sql_template("apply_op_array")
    template2 = load_sql_template("apply_op_array")

    # Verify same object is returned (cached)
    assert template1 is template2
