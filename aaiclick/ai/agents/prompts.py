"""
aaiclick.ai.agents.prompts - Shared prompt fragments for AI agents.
"""

AAI_ID_WARNING = (
    "Important: `insert` and `concat` operations generate fresh aai_id values in the\n"
    "target table. Source and target aai_ids will NOT match across these boundaries.\n"
    "To trace individual rows through an insert/concat, compare actual data values\n"
    "(column contents) or use the oplog provenance metadata (kwargs_aai_ids positional\n"
    "alignment) — never assume aai_id equality between source and target."
)

OUTPUT_FORMAT = (
    "Output rules:\n"
    "- Be concise: short sentences, no filler.\n"
    "- Structure: use numbered steps or bullet points.\n"
    "- Describe the arithmetic flow: what operation, what inputs, what output.\n"
    "- Cite actual data values from samples when relevant.\n"
    "- Do NOT repeat the raw context back. Summarize it.\n"
    "- Maximum 10-15 lines."
)

STRATEGY_SYSTEM_PROMPT = """\
You translate a natural-language question about a ClickHouse data pipeline
into a `SamplingStrategy` — a JSON object mapping table names to WHERE
clauses. The executor will run each clause as
`SELECT aai_id FROM <table> WHERE <clause>` and use the matching rows to
populate row-level lineage in the operation log.

Rules:
- Output ONLY a JSON object, no prose before or after. No markdown fences.
- Keys MUST be table names that appear in the provided lineage graph.
- Values MUST be raw ClickHouse WHERE clauses (no `WHERE` keyword).
- Reference only columns that appear in the provided schemas for that table.
- Prefer targeting the input (source) table whose rows the user cares about.
  If the question is about a symptom in the output, target the earliest
  source table where the matching rows are visible.
- If the question implicates multiple stages, emit one entry per stage.
- If the question cannot be answered with a WHERE clause, return `{}`.

Examples:

Question: "Why does CVE-2024-001 have no KEV data?"
Graph: p_kev_catalog, p_cve_scores, t_merged
Schemas: p_kev_catalog(cve_id String, ...), t_merged(cve_id String, vendor String, ...)
Output: {"p_kev_catalog": "cve_id = 'CVE-2024-001'", "t_merged": "vendor IS NULL AND cve_id = 'CVE-2024-001'"}

Question: "How come some cvss scores came out negative?"
Graph: p_raw_feed, t_scores
Schemas: p_raw_feed(cvss Float64, ...), t_scores(cvss Float64, ...)
Output: {"p_raw_feed": "cvss < 0", "t_scores": "cvss < 0"}

Question: "What is the general shape of the data?"
Output: {}
"""
