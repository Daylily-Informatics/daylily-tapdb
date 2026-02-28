"""
daylily-tapdb: Templated Abstract Polymorphic Database Library

A reusable library for building template-driven database applications
with PostgreSQL and SQLAlchemy.

Example:
    import os
    from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory
    from daylily_tapdb.cli.db_config import get_db_config_for_env

    # Connect using canonical config loader (recommended)
    env = os.environ.get("TAPDB_ENV", "dev")
    cfg = get_db_config_for_env(env)
    db = TAPDBConnection(
        db_hostname=f"{cfg['host']}:{cfg['port']}",
        db_user=cfg["user"],
        db_pass=cfg["password"],
        db_name=cfg["database"],
    )

    templates = TemplateManager()
    factory = InstanceFactory(templates)

    with db.session_scope(commit=True) as session:
        plate = factory.create_instance(
            session=session,
            template_code='container/plate/fixed-plate-96/1.0/',
            name='PLATE-001'
        )
"""

try:
    from daylily_tapdb._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"
from daylily_tapdb.actions import ActionDispatcher
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.euid import EUIDConfig
from daylily_tapdb.factory import InstanceFactory
from daylily_tapdb.models.base import Base, tapdb_core
from daylily_tapdb.models.instance import (
    action_instance,
    actor_instance,
    container_instance,
    content_instance,
    data_instance,
    equipment_instance,
    file_instance,
    generic_instance,
    health_event_instance,
    subject_instance,
    test_requisition_instance,
    workflow_instance,
    workflow_step_instance,
)
from daylily_tapdb.models.lineage import (
    action_instance_lineage,
    actor_instance_lineage,
    container_instance_lineage,
    content_instance_lineage,
    data_instance_lineage,
    equipment_instance_lineage,
    file_instance_lineage,
    generic_instance_lineage,
    health_event_instance_lineage,
    subject_instance_lineage,
    test_requisition_instance_lineage,
    workflow_instance_lineage,
    workflow_step_instance_lineage,
)
from daylily_tapdb.models.template import (
    action_template,
    actor_template,
    container_template,
    content_template,
    data_template,
    equipment_template,
    file_template,
    generic_template,
    health_event_template,
    subject_template,
    test_requisition_template,
    workflow_step_template,
    workflow_template,
)
from daylily_tapdb.templates import TemplateManager

__all__ = [
    "__version__",
    # Core classes
    "TAPDBConnection",
    "TemplateManager",
    "InstanceFactory",
    "ActionDispatcher",
    "EUIDConfig",
    # Base
    "tapdb_core",
    "Base",
    # Generic classes
    "generic_template",
    "generic_instance",
    "generic_instance_lineage",
    # Typed templates
    "workflow_template",
    "workflow_step_template",
    "container_template",
    "content_template",
    "equipment_template",
    "data_template",
    "test_requisition_template",
    "actor_template",
    "action_template",
    "health_event_template",
    "file_template",
    "subject_template",
    # Typed instances
    "workflow_instance",
    "workflow_step_instance",
    "container_instance",
    "content_instance",
    "equipment_instance",
    "data_instance",
    "test_requisition_instance",
    "actor_instance",
    "action_instance",
    "health_event_instance",
    "file_instance",
    "subject_instance",
    # Typed lineages
    "workflow_instance_lineage",
    "workflow_step_instance_lineage",
    "container_instance_lineage",
    "content_instance_lineage",
    "equipment_instance_lineage",
    "data_instance_lineage",
    "test_requisition_instance_lineage",
    "actor_instance_lineage",
    "action_instance_lineage",
    "health_event_instance_lineage",
    "file_instance_lineage",
    "subject_instance_lineage",
]
