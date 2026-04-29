"""
Checkpoint 1: Schema Configuration

Validates student's enhanced source configuration with schema and identifier mapping.
Tests run in TWO environments:
1. Local: Students run `pytest tests/test_schema_config.py -v` in challenge directory
2. Remote: Glovebox server runs on git push

Tests check:
- jaffle_shop_dbt/models/schema.yml exists
- YAML is valid
- Has required dbt structure (version, sources)
- Has schema and identifier mapping (Section 2.1)
"""

import pytest
import yaml
from pathlib import Path


class TestSchemaConfig:
    """Test that student completed schema configuration (Checkpoint 1)."""

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

    @pytest.fixture
    def sources_file(self, project_dir):
        """Find sources configuration file (schema.yml or sources.yml)."""
        possible_locations = [
            project_dir / "models" / "schema.yml",
            project_dir / "models" / "sources.yml",
        ]

        for location in possible_locations:
            if location.exists():
                return location

        return None

    def test_sources_file_exists(self, sources_file):
        """Student must have models/schema.yml (copied from Challenge 1)."""
        assert sources_file is not None, (
            "❌ No sources configuration file found in jaffle_shop_dbt/models/\n"
            "   Expected: jaffle_shop_dbt/models/schema.yml\n"
            "   Did you copy your dbt project from the previous challenge? (Section 0)\n"
            "   Run: cp -rP ../PREVIOUS-CHALLENGE/jaffle_shop_dbt ."
        )

    def test_sources_file_is_valid_yaml(self, sources_file):
        """YAML syntax must be correct."""
        if sources_file is None:
            pytest.skip("schema.yml not found in repository")

        try:
            with open(sources_file, 'r') as f:
                content = yaml.safe_load(f)
                assert content is not None, "YAML file is empty"
        except yaml.YAMLError as e:
            pytest.fail(
                f"❌ schema.yml has invalid YAML syntax\n"
                f"   Error: {str(e)}\n"
                f"   Common issues:\n"
                f"   - Inconsistent indentation (use 2 spaces, not tabs)\n"
                f"   - Missing spaces after colons: 'name: value' not 'name:value'\n"
                f"   - Misaligned list items (dashes must align)\n"
                f"   Validate at: https://www.yamllint.com/"
            )

    def test_sources_has_required_structure(self, sources_file):
        """Validate schema.yml has proper dbt structure with schema and identifier mapping."""
        if sources_file is None:
            pytest.skip("schema.yml not found")

        with open(sources_file, 'r') as f:
            content = yaml.safe_load(f)

        # Check version
        assert 'version' in content, (
            "❌ Missing 'version: 2' in schema.yml\n"
            "   dbt requires version specification at top of file"
        )

        assert content['version'] == 2, (
            f"❌ version should be 2, found {content['version']}"
        )

        # Check sources key
        assert 'sources' in content, (
            "❌ Missing 'sources' key in schema.yml\n"
            "   File should contain source definitions"
        )

        assert len(content['sources']) > 0, (
            "❌ No sources defined\n"
            "   Expected: jaffle_shop source with 3 tables"
        )

        # Check Section 2.1: schema and identifier mapping
        sources = content.get('sources', [])
        jaffle_source = next((s for s in sources if s.get('name') == 'jaffle_shop'), None)

        assert jaffle_source is not None, (
            "❌ Source 'jaffle_shop' not found\n"
            "   Expected in Section 2.1"
        )

        assert 'database' in jaffle_source, (
            "❌ Missing 'database' field in jaffle_shop source\n"
            "   Section 2.1 requires: database: dev"
        )

        assert 'schema' in jaffle_source, (
            "❌ Missing 'schema' field in jaffle_shop source\n"
            "   Section 2.1 requires: schema: raw"
        )

        tables = jaffle_source.get('tables', [])
        assert len(tables) >= 3, (
            f"❌ Expected at least 3 tables, found {len(tables)}\n"
            "   Required: customers, orders, payments"
        )

        # Check at least one table has identifier
        has_identifier = any('identifier' in table for table in tables)
        assert has_identifier, (
            "❌ No tables have 'identifier' field\n"
            "   Section 2.1 requires identifier mapping (e.g., identifier: raw_customers)"
        )
