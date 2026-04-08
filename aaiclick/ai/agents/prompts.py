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
