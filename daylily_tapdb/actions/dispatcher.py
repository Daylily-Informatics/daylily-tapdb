"""Action dispatcher for TAPDB."""

import logging
from abc import ABC
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from daylily_tapdb.models.instance import action_instance, generic_instance

logger = logging.getLogger(__name__)


class ActionDispatcher(ABC):
    """
    Abstract base class for action execution.

    Applications extend this class to implement concrete do_action_* methods.
    The dispatcher routes action requests to the appropriate handler method
    and optionally creates action_instance records for audit/scheduling.

    Example:
        class MyActionHandler(ActionDispatcher):
            def do_action_set_status(self, instance, action_ds, captured_data):
                instance.bstatus = captured_data.get("status")
                return {"status": "success", "message": "Status updated"}
    """

    def __init__(self):
        """Initialize action dispatcher.

        Phase 2 moonshot: dispatcher does not own DB connections/sessions.
        Callers pass an explicit Session into execute_action().
        """

    def execute_action(
        self,
        session: Session,
        instance: generic_instance,
        action_group: str,
        action_key: str,
        action_ds: Dict[str, Any],
        captured_data: Optional[Dict[str, Any]] = None,
        create_action_record: bool = True,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Route action to appropriate handler method.

        Args:
            instance: The instance to act on.
            action_group: The action group name (e.g., 'core_actions').
            action_key: The action key within the group.
            action_ds: The action definition from json_addl.
            captured_data: User-provided data from action form.
            create_action_record: Whether to create an action_instance record.
            user: Username for audit tracking.

        Returns:
            Result dictionary with status and message.
        """
        captured_data = captured_data or {}

        # Find handler method
        method_name = f"do_action_{action_key}"
        handler = getattr(self, method_name, None)

        if handler is None:
            logger.warning(f"No handler found for action: {action_key}")
            return {
                "status": "error",
                "message": f"No handler for action: {action_key}",
            }

        # Execute action
        try:
            result = handler(instance, action_ds, captured_data)
        except Exception as e:
            logger.exception(f"Action {action_key} failed: {e}")
            result = {"status": "error", "message": str(e)}

        # Update action tracking in json_addl
        self._update_action_tracking(instance, action_group, action_key, result)

        # Create action record if requested
        if create_action_record and result.get("status") == "success":
            self._create_action_record(
                session,
                instance,
                action_group,
                action_key,
                action_ds,
                captured_data,
                result,
                user,
            )

        return result

    def _update_action_tracking(
        self,
        instance: generic_instance,
        action_group: str,
        action_key: str,
        result: Dict[str, Any],
    ):
        """Update action execution tracking in instance json_addl."""
        if action_group not in instance.json_addl.get("action_groups", {}):
            return

        action_def = instance.json_addl["action_groups"][action_group].get(action_key)
        if not action_def:
            return

        # Update execution count and timestamp
        exec_count = int(action_def.get("action_executed", "0"))
        action_def["action_executed"] = str(exec_count + 1)
        action_def["executed_datetime"].append(datetime.now(timezone.utc).isoformat())

        flag_modified(instance, "json_addl")

    def _create_action_record(
        self,
        session: Session,
        instance: generic_instance,
        action_group: str,
        action_key: str,
        action_ds: Dict[str, Any],
        captured_data: Dict[str, Any],
        result: Dict[str, Any],
        user: Optional[str],
    ):
        """Create an action_instance record for audit/scheduling."""
        action_template_uuid = action_ds.get("action_template_uuid")
        if not action_template_uuid:
            raise ValueError(
                "action_ds is missing required 'action_template_uuid'. "
                "Instances must be created with Phase 2 action materialization."
            )

        template_uuid = (
            UUID(action_template_uuid)
            if isinstance(action_template_uuid, str)
            else action_template_uuid
        )

        # This creates a first-class action record (XX prefix)
        action_record = action_instance(
            name=f"{action_key}@{instance.euid}",
            polymorphic_discriminator="action_instance",
            category="action",
            type="action",
            subtype=action_key,
            version="1.0",
            template_uuid=template_uuid,
            json_addl={
                "target_instance_uuid": str(instance.uuid),
                "target_instance_euid": instance.euid,
                "action_group": action_group,
                "action_key": action_key,
                "action_definition": action_ds,
                "captured_data": captured_data,
                "result": result,
                "executed_by": user,
                "executed_at": datetime.now(timezone.utc).isoformat(),
            },
            bstatus="completed",
        )

        session.add(action_record)
        session.flush()
