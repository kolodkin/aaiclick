"""IMDb Dataset Builder report generation."""

import os
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from aaiclick.data.models import ColumnInfo
from aaiclick.data.object import Object
from aaiclick.orchestration import task

from .constants import CLEAN_COLUMNS, HF_REPO_ID, IMDB_RAW_COLUMNS, IMDB_URL


def _fmt(value: object) -> str:
    """Format a numeric value for display."""
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def _print_field_table(columns: dict[str, ColumnInfo]) -> None:
    """Print a markdown table of field names, types, and descriptions."""
    described = {f: c for f, c in columns.items() if c.description}
    if not described:
        return
    name_w = max(len("Field"), max(len(f) for f in described))
    type_w = max(len("Type"), max(len(c.ch_type()) for c in described.values()))
    desc_w = max(len("Description"), max(len(c.description) for c in described.values()))
    print(f"| {'Field':<{name_w}s} | {'Type':<{type_w}s} | {'Description':<{desc_w}s} |")
    print(f"|{'-' * (name_w + 2)}|{'-' * (type_w + 2)}|{'-' * (desc_w + 2)}|")
    for field, col in described.items():
        print(f"| {field:<{name_w}s} | {col.ch_type():<{type_w}s} | {col.description:<{desc_w}s} |")


def _print_report(
    profile: dict,
    quality_issues: dict,
    genre_data: dict,
    hf_result: dict,
    raw_md: str,
    clean_md: str,
    genre_md: str,
) -> None:
    """Print the IMDb dataset builder report as markdown."""
    print("\n## IMDb Movie Dataset Builder\n")

    # ---- Raw Data Profile ----
    p = profile
    print("### Raw Data Profile\n")
    print(f"URL: {IMDB_URL}")
    print(f"Total titles: {_fmt(p['total_titles'])}")
    print(f"Adult titles: {_fmt(p['adult_count'])} ({_fmt(p['adult_pct'])}%)\n")

    print("#### Field Schema\n")
    _print_field_table(IMDB_RAW_COLUMNS)

    print("\n#### Title Type Breakdown\n")
    for title_type, count in sorted(p["by_type"].items(), key=lambda x: -x[1]):
        print(f"- {title_type}: {_fmt(count)}")

    print("\n#### Sample (first 5 rows)\n")
    print(raw_md)

    # ---- Movie Filter ----
    q = quality_issues
    dropped = p["total_titles"] - q["total_movies"]
    print("\n### Movie Filter\n")
    print(f"- Non-adult movies with genres + year: {_fmt(q['total_movies'])}")
    print(f"- Dropped (non-movie, adult, missing genres/year): {_fmt(dropped)}")

    # ---- Quality Issues ----
    print("\n### Quality Issues Detected\n")
    print(f"- Missing runtime (`\\N`): {_fmt(q['missing_runtime'])} ({_fmt(q['missing_runtime_pct'])}%)")
    print(f"- Runtime < 40 min: {_fmt(q['short_runtime'])}")
    print(f"- Runtime > 300 min: {_fmt(q['long_runtime'])}")
    print(f"- Pre-1970 movies: {_fmt(q['pre_1970'])} ({_fmt(q['pre_1970_pct'])}%)")

    # ---- Genre Distribution ----
    print("\n### Genre Distribution\n")
    print("#### Top genres (by title count)\n")
    print(genre_md)

    if genre_data:
        pairs = sorted(genre_data.items(), key=lambda x: -x[1])
        total_genre_rows = sum(c for _, c in pairs)
        print("\n#### Statistics\n")
        print(f"- Distinct genres: {_fmt(len(pairs))}")
        print(f"- Total genre-title rows (after explode): {_fmt(total_genre_rows)}")
        print("- Top 5:")
        for genre, count in pairs[:5]:
            pct = count / total_genre_rows * 100 if total_genre_rows > 0 else 0.0
            print(f"  - {genre}: {_fmt(count)} ({_fmt(pct)}%)")

    # ---- Clean Dataset ----
    print("\n### Clean Dataset\n")
    print("#### Field Schema\n")
    _print_field_table(CLEAN_COLUMNS)

    print("\n#### Sample (first 5 rows)\n")
    print(clean_md)

    # ---- Publish Result ----
    print("\n### Published\n")
    status = hf_result.get("status", "unknown")
    if status == "published":
        print(f"- Hugging Face: https://huggingface.co/datasets/{hf_result['repo']}")
        print(f"- Rows published: {_fmt(hf_result['rows'])}")
    elif status == "skipped":
        print(f"- Skipped: {hf_result.get('reason', 'unknown reason')}")
        print(f"- Set `HF_TOKEN` to publish to: https://huggingface.co/datasets/{HF_REPO_ID}")
    else:
        print(f"- Status: {status}")


@task
async def generate_report(
    raw: Object,
    movies: Object,
    clean: Object,
    genre_balance: Object,
    profile: dict,
    quality_issues: dict,
    hf_result: dict,
) -> dict:
    """Combine all pipeline outputs into a unified IMDb dataset builder report."""
    raw_md = await raw[
        ["tconst", "titleType", "primaryTitle", "startYear", "genres", "runtimeMinutes"]
    ].view(limit=5).markdown(truncate={"primaryTitle": 40})

    clean_md = await clean.view(limit=5).markdown(truncate={"primaryTitle": 40})

    genre_md = await genre_balance.view(
        order_by="tconst DESC",
        limit=15,
    ).markdown()

    genre_data_raw = await genre_balance.data()
    genre_data = dict(zip(genre_data_raw["genre"], genre_data_raw["tconst"]))

    buf = StringIO()
    with redirect_stdout(buf):
        _print_report(profile, quality_issues, genre_data, hf_result, raw_md, clean_md, genre_md)
    rendered = buf.getvalue()

    report_file = os.environ.get("AAICLICK_REPORT_FILE")
    if report_file:
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        with open(report_file, "w") as f:
            f.write(rendered)
    else:
        sys.stdout.write(rendered)

    return {
        "total_titles": profile["total_titles"],
        "total_movies": quality_issues["total_movies"],
        "hf_status": hf_result.get("status"),
    }
