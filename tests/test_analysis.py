"""
Test Challenge 03 - Staging Model Source Reference

Validates student's stg_customers.sql uses the identifier-mapped source name.
Tests run in TWO environments:
1. Local: Students run `pytest tests/test_analysis.py -v` in challenge directory
2. Remote: Glovebox server runs on git push

Tests check:
- jaffle_shop_dbt/models/staging/stg_customers.sql exists
- File uses {{ source('jaffle_shop', 'customers') }} (clean identifier, not raw_customers)
"""

import pytest
from pathlib import Path


class TestStagingModelReference:
    """Test that student updated staging model to use the identifier-mapped source name."""

    @pytest.fixture
    def project_dir(self):
        """Get jaffle_shop_dbt directory within challenge repo."""
        challenge_dir = Path(__file__).parent.parent
        dbt_project_dir = challenge_dir / "jaffle_shop_dbt"

        assert dbt_project_dir.exists(), (
            f"❌ jaffle_shop_dbt/ directory not found in {challenge_dir}\n"
            f"   Did you copy your dbt project from the previous challenge? (Section 0)\n"
            f"   Run: cp -rP ../PREVIOUS-CHALLENGE/jaffle_shop_dbt ."
        )

        return dbt_project_dir

    def test_staging_model_exists(self, project_dir):
        """stg_customers.sql must exist (copied and modified in this challenge)."""
        staging_file = project_dir / "models" / "staging" / "stg_customers.sql"

        assert staging_file.exists(), (
            "❌ Missing jaffle_shop_dbt/models/staging/stg_customers.sql\n"
            "   Did you copy your dbt project from the previous challenge? (Section 0)\n"
            "   Run: cp -rP ../PREVIOUS-CHALLENGE/jaffle_shop_dbt ."
        )

    def test_staging_model_uses_clean_identifier(self, project_dir):
        """stg_customers.sql must use 'customers' identifier, not 'raw_customers'."""
        staging_file = project_dir / "models" / "staging" / "stg_customers.sql"

        if not staging_file.exists():
            pytest.skip("stg_customers.sql not found")

        with open(staging_file, 'r') as f:
            content = f.read()

        # Must use source() macro at all
        assert "{{ source(" in content or "{{source(" in content, (
            "❌ stg_customers.sql doesn't use {{ source() }} macro\n"
            "   Did you reference the source correctly? It should read:\n"
            "   SELECT * FROM {{ source('jaffle_shop', 'customers') }}"
        )

        # Must use the clean identifier 'customers', not 'raw_customers' in source()
        uses_clean = (
            "source('jaffle_shop', 'customers')" in content
            or 'source("jaffle_shop", "customers")' in content
        )
        assert uses_clean, (
            "❌ stg_customers.sql must reference the clean identifier: {{ source('jaffle_shop', 'customers') }}\n"
            "   Did you complete Section 2.3?\n"
            "   Change: {{ source('jaffle_shop', 'raw_customers') }}\n"
            "   To:     {{ source('jaffle_shop', 'customers') }}"
        )
