"""
daylily-tapdb: Templated Abstract Polymorphic Database Library

A reusable library for building template-driven database applications
with PostgreSQL and SQLAlchemy.

Example:
    from daylily_tapdb import TAPDBConnection, TemplateManager, InstanceFactory

    db = TAPDBConnection(os.environ['DATABASE_URL'])
    templates = TemplateManager(db, Path('./config'))
    factory = InstanceFactory(db, templates)

    plate = factory.create_instance(
        template_code='container/plate/fixed-plate-96/1.0/',
        name='PLATE-001'
    )
"""
from daylily_tapdb._version import __version__
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.euid import EUIDConfig
from daylily_tapdb.templates import TemplateManager
from daylily_tapdb.factory import InstanceFactory
from daylily_tapdb.actions import ActionDispatcher
from daylily_tapdb.models.base import tapdb_core, Base
from daylily_tapdb.models.template import (
    generic_template,
    workflow_template,
    workflow_step_template,
    container_template,
    content_template,
    equipment_template,
    data_template,
    test_requisition_template,
    actor_template,
    action_template,
    health_event_template,
    file_template,
    subject_template,
)
from daylily_tapdb.models.instance import (
    generic_instance,
    workflow_instance,
    workflow_step_instance,
    container_instance,
    content_instance,
    equipment_instance,
    data_instance,
    test_requisition_instance,
    actor_instance,
    action_instance,
    health_event_instance,
    file_instance,
    subject_instance,
)
from daylily_tapdb.models.lineage import (
    generic_instance_lineage,
    workflow_instance_lineage,
    workflow_step_instance_lineage,
    container_instance_lineage,
    content_instance_lineage,
    equipment_instance_lineage,
    data_instance_lineage,
    test_requisition_instance_lineage,
    actor_instance_lineage,
    action_instance_lineage,
    health_event_instance_lineage,
    file_instance_lineage,
    subject_instance_lineage,
)

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
