"""
Generate the data lake section of the Intelligence system prompt from the ontology registry.
Called at server startup to produce the auto-generated portion of the prompt.
"""
from pathlib import Path

GENERATED_PROMPT = Path(__file__).parent.parent.parent / "ontology" / "generated_prompt.md"


def generate_intelligence_prompt_section() -> str:
    """
    Returns the auto-generated data lake description for the Intelligence system prompt.
    Reads from ontology/generated_prompt.md (produced by scripts/generate_ontology.py).
    Falls back to a minimal description if the file doesn't exist.
    """
    if GENERATED_PROMPT.exists():
        return GENERATED_PROMPT.read_text()

    # Fallback: minimal description
    return (
        "## The Aradune Data Lake\n\n"
        "667+ tables with 400M+ rows of public Medicaid data across rates, enrollment, "
        "hospitals, quality, workforce, pharmacy, behavioral health, LTSS/HCBS, expenditure, "
        "economic, Medicare, providers, and public health domains organized into 18 domains.\n\n"
        "Use `list_tables` to discover available tables and `describe_table` for schemas.\n"
    )
