"""
Tests that the ontology YAML files are valid and consistent.
Wraps scripts/validate_ontology.py as a pytest test.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def test_ontology_validates():
    """Run validate_ontology.py and assert it exits 0."""
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "validate_ontology.py")],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    assert result.returncode == 0, (
        f"Ontology validation failed:\n{result.stdout}\n{result.stderr}"
    )
