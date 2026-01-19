"""Tests for TAPDB ORM models."""
import pytest


class TestTapdbCore:
    """Tests for the tapdb_core base class."""

    def test_import_base(self):
        """Test that Base can be imported."""
        from daylily_tapdb.models.base import Base
        assert Base is not None

    def test_import_tapdb_core(self):
        """Test that tapdb_core can be imported."""
        from daylily_tapdb.models.base import tapdb_core
        assert tapdb_core is not None
        assert tapdb_core.__abstract__ is True

    def test_tapdb_core_columns_via_template(self):
        """Test that tapdb_core columns are inherited by concrete classes."""
        from daylily_tapdb.models.template import generic_template
        # Check column names exist on a concrete subclass
        columns = [c.name for c in generic_template.__table__.columns]
        expected = ["uuid", "euid", "name", "created_dt", "modified_dt",
                    "polymorphic_discriminator", "super_type", "btype",
                    "b_sub_type", "version", "bstatus", "json_addl",
                    "is_singleton", "is_deleted"]
        for col in expected:
            assert col in columns, f"Missing column: {col}"


class TestGenericTemplate:
    """Tests for the generic_template model."""

    def test_import(self):
        """Test that generic_template can be imported."""
        from daylily_tapdb.models.template import generic_template
        assert generic_template is not None

    def test_tablename(self):
        """Test that generic_template has correct tablename."""
        from daylily_tapdb.models.template import generic_template
        assert generic_template.__tablename__ == "generic_template"

    def test_polymorphic_identity(self):
        """Test polymorphic identity."""
        from daylily_tapdb.models.template import generic_template
        assert generic_template.__mapper_args__["polymorphic_identity"] == "generic_template"

    def test_typed_subclasses(self):
        """Test that typed template subclasses exist."""
        from daylily_tapdb.models.template import (
            workflow_template, container_template, content_template,
            equipment_template, data_template, actor_template,
            action_template, file_template, subject_template
        )
        assert workflow_template.__mapper_args__["polymorphic_identity"] == "workflow_template"
        assert container_template.__mapper_args__["polymorphic_identity"] == "container_template"


class TestGenericInstance:
    """Tests for the generic_instance model."""

    def test_import(self):
        """Test that generic_instance can be imported."""
        from daylily_tapdb.models.instance import generic_instance
        assert generic_instance is not None

    def test_tablename(self):
        """Test that generic_instance has correct tablename."""
        from daylily_tapdb.models.instance import generic_instance
        assert generic_instance.__tablename__ == "generic_instance"

    def test_typed_subclasses(self):
        """Test that typed instance subclasses exist."""
        from daylily_tapdb.models.instance import (
            workflow_instance, container_instance, content_instance,
            equipment_instance, data_instance, actor_instance,
            action_instance, file_instance, subject_instance
        )
        assert workflow_instance.__mapper_args__["polymorphic_identity"] == "workflow_instance"


class TestGenericInstanceLineage:
    """Tests for the generic_instance_lineage model."""

    def test_import(self):
        """Test that generic_instance_lineage can be imported."""
        from daylily_tapdb.models.lineage import generic_instance_lineage
        assert generic_instance_lineage is not None

    def test_tablename(self):
        """Test that generic_instance_lineage has correct tablename."""
        from daylily_tapdb.models.lineage import generic_instance_lineage
        assert generic_instance_lineage.__tablename__ == "generic_instance_lineage"
