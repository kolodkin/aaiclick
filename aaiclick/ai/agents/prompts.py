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
    "- Refer to tables by their role or operation (e.g. 'the prices table',\n"
    "  'the multiply result'), not raw IDs like `t_7448...`.\n"
    "  Only use persistent names (`p_...`) when they exist.\n"
    "- Cite actual data values from samples when relevant.\n"
    "- Do NOT repeat the raw context back. Summarize it.\n"
    "- Maximum 10-15 lines."
)
