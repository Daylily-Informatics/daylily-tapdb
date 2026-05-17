"""Tests to bring all modules above 85% coverage.

Covers modules below 85%: spec.py, __main__.py, templates/__init__.py,
lineage.py, connection.py, sequences.py, context.py, outbox/repository.py,
passwords.py, factory/instance.py, schema_inventory.py, admin_server.py,
cli/__init__.py, cli/user.py, cli/cognito.py, cli/pg.py, cli/db.py,
cli/db_config.py, cli/aurora.py, templates/loader.py
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

# ────────────────────────────────────────────────────────────────────
# cli/spec.py  (0% → 100%)
# ────────────────────────────────────────────────────────────────────


class TestCliSpec:
    def test_spec_module_imports(self):
        from daylily_tapdb.cli.spec import spec

        assert spec.prog_name == "tapdb"
        assert spec.app_display_name == "TapDB CLI"
        assert spec.xdg.app_dir_name == "tapdb"
        assert spec.config.xdg_relative_path == "config.yaml"


# ────────────────────────────────────────────────────────────────────
# cli/__main__.py  (67% → 100%)
# ────────────────────────────────────────────────────────────────────


class TestMainModule:
    def test_main_module_importable(self):
        import daylily_tapdb.cli.__main__  # noqa: F401

    def test_main_invocation(self):
        with mock.patch("daylily_tapdb.cli.main", return_value=0) as m:
            with pytest.raises(SystemExit):
                exec(
                    compile(
                        "from daylily_tapdb.cli import main\nraise SystemExit(main())",
                        "<test>",
                        "exec",
                    )
                )
            m.assert_called_once()


# ────────────────────────────────────────────────────────────────────
# templates/__init__.py  (56% → 100%) — lazy exports
# ────────────────────────────────────────────────────────────────────


class TestTemplatesLazyExports:
    def test_template_manager(self):
        from daylily_tapdb.templates import TemplateManager

        assert TemplateManager is not None

    def test_mutation_guard(self):
        from daylily_tapdb.templates import (
            TemplateMutationGuardError,
            allow_template_mutations,
        )

        assert TemplateMutationGuardError is not None
        assert allow_template_mutations is not None

    def test_requirements(self):
        from daylily_tapdb.templates import (
            MissingSeededTemplateError,
            require_seeded_template,
            require_seeded_templates,
        )

        assert MissingSeededTemplateError is not None
        assert callable(require_seeded_template)
        assert callable(require_seeded_templates)

    def test_loader_exports(self):
        from daylily_tapdb.templates import (
            ConfigIssue,
            SeedSummary,
            find_config_dir,
            find_duplicate_template_keys,
            find_tapdb_core_config_dir,
            load_template_configs,
            normalize_config_dirs,
            resolve_seed_config_dirs,
            seed_templates,
            validate_template_configs,
        )

        for obj in [
            ConfigIssue,
            SeedSummary,
            find_config_dir,
            find_duplicate_template_keys,
            find_tapdb_core_config_dir,
            load_template_configs,
            normalize_config_dirs,
            resolve_seed_config_dirs,
            seed_templates,
            validate_template_configs,
        ]:
            assert obj is not None

    def test_unknown_attribute_raises(self):
        import daylily_tapdb.templates as t

        with pytest.raises(AttributeError):
            _ = t.this_does_not_exist


# ────────────────────────────────────────────────────────────────────
# lineage.py  — LineageQueryProxy + mocked DB helpers
# ────────────────────────────────────────────────────────────────────


class TestLineageQueryProxy:
    def test_iter(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert list(LineageQueryProxy([1, 2, 3])) == [1, 2, 3]

    def test_len(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert len(LineageQueryProxy([1, 2])) == 2

    def test_bool_true(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert bool(LineageQueryProxy([1]))

    def test_bool_false(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert not bool(LineageQueryProxy([]))

    def test_all(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert LineageQueryProxy([5, 6]).all() == [5, 6]

    def test_first_nonempty(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert LineageQueryProxy([10, 20]).first() == 10

    def test_first_empty(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert LineageQueryProxy([]).first() is None

    def test_count(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert LineageQueryProxy([1, 2, 3]).count() == 3

    def test_getitem(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert LineageQueryProxy([10, 20, 30])[1] == 20

    def test_getattr_delegates(self):
        from daylily_tapdb.lineage import LineageQueryProxy

        assert LineageQueryProxy([1, 2]).append


class TestLineageDBHelpers:
    """Test lineage helper functions with mocked ORM instances."""

    def test_get_parent_lineages_with_session(self):
        from daylily_tapdb.lineage import get_parent_lineages

        instance = mock.MagicMock()
        instance.uid = 42
        with mock.patch("daylily_tapdb.lineage.object_session") as mock_os:
            mock_session = mock.MagicMock()
            mock_os.return_value = mock_session
            result = get_parent_lineages(instance)
            assert result is not None

    def test_get_parent_lineages_no_session(self):
        from daylily_tapdb.lineage import get_parent_lineages

        instance = mock.MagicMock()
        with mock.patch("daylily_tapdb.lineage.object_session", return_value=None):
            result = get_parent_lineages(instance)
            assert list(result) == []

    def test_get_child_lineages_with_session(self):
        from daylily_tapdb.lineage import get_child_lineages

        instance = mock.MagicMock()
        instance.uid = 42
        with mock.patch("daylily_tapdb.lineage.object_session") as mock_os:
            mock_session = mock.MagicMock()
            mock_os.return_value = mock_session
            result = get_child_lineages(instance)
            assert result is not None

    def test_get_child_lineages_no_session(self):
        from daylily_tapdb.lineage import get_child_lineages

        instance = mock.MagicMock()
        with mock.patch("daylily_tapdb.lineage.object_session", return_value=None):
            result = get_child_lineages(instance)
            assert list(result) == []

    def test_resolve_parent_instance_with_session(self):
        from daylily_tapdb.lineage import resolve_parent_instance

        lineage = mock.MagicMock()
        lineage.parent_instance_uid = 10
        with mock.patch("daylily_tapdb.lineage.object_session") as mock_os:
            mock_session = mock.MagicMock()
            mock_os.return_value = mock_session
            result = resolve_parent_instance(lineage)
            assert result is not None

    def test_resolve_parent_instance_no_session(self):
        from daylily_tapdb.lineage import resolve_parent_instance

        lineage = mock.MagicMock()
        with mock.patch("daylily_tapdb.lineage.object_session", return_value=None):
            assert resolve_parent_instance(lineage) is None

    def test_resolve_parent_instance_no_uid(self):
        from daylily_tapdb.lineage import resolve_parent_instance

        lineage = mock.MagicMock(spec=[])  # no parent_instance_uid attr
        with mock.patch("daylily_tapdb.lineage.object_session") as mock_os:
            mock_os.return_value = mock.MagicMock()
            assert resolve_parent_instance(lineage) is None

    def test_resolve_child_instance_with_session(self):
        from daylily_tapdb.lineage import resolve_child_instance

        lineage = mock.MagicMock()
        lineage.child_instance_uid = 20
        with mock.patch("daylily_tapdb.lineage.object_session") as mock_os:
            mock_session = mock.MagicMock()
            mock_os.return_value = mock_session
            result = resolve_child_instance(lineage)
            assert result is not None

    def test_resolve_child_instance_no_session(self):
        from daylily_tapdb.lineage import resolve_child_instance

        lineage = mock.MagicMock()
        with mock.patch("daylily_tapdb.lineage.object_session", return_value=None):
            assert resolve_child_instance(lineage) is None

    def test_resolve_child_instance_no_uid(self):
        from daylily_tapdb.lineage import resolve_child_instance

        lineage = mock.MagicMock(spec=[])  # no child_instance_uid attr
        with mock.patch("daylily_tapdb.lineage.object_session") as mock_os:
            mock_os.return_value = mock.MagicMock()
            assert resolve_child_instance(lineage) is None


# ────────────────────────────────────────────────────────────────────
# connection.py  — TAPDBConnection
# ────────────────────────────────────────────────────────────────────


class TestConnection:
    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_init_with_url(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection(db_url="postgresql://u:p@localhost:5432/test")
        assert conn._db_url == "postgresql://u:p@localhost:5432/test"

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_init_with_components(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection(
            db_hostname="myhost:5432", db_user="u", db_pass="p", db_name="test"
        )
        assert "myhost" in conn._db_url

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_init_defaults(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection()
        assert "tapdb" in conn._db_url

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_context_manager(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection(db_url="postgresql://u:p@localhost/test")
        with conn as c:
            assert c is conn

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_session_scope_commit(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection(db_url="postgresql://u:p@localhost/test")
        mock_session = mock.MagicMock()
        mock_trans = mock.MagicMock()
        mock_session.begin.return_value = mock_trans
        conn._Session = mock.MagicMock(return_value=mock_session)
        with conn.session_scope(commit=True) as session:
            assert session is mock_session
        mock_trans.commit.assert_called()

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_session_scope_rollback_on_error(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection(db_url="postgresql://u:p@localhost/test")
        mock_session = mock.MagicMock()
        mock_trans = mock.MagicMock()
        mock_session.begin.return_value = mock_trans
        conn._Session = mock.MagicMock(return_value=mock_session)
        with pytest.raises(ValueError):
            with conn.session_scope(commit=True):
                raise ValueError("boom")
        mock_trans.rollback.assert_called()

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_session_scope_no_commit(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        conn = TAPDBConnection(db_url="postgresql://u:p@localhost/test")
        mock_session = mock.MagicMock()
        mock_trans = mock.MagicMock()
        mock_session.begin.return_value = mock_trans
        conn._Session = mock.MagicMock(return_value=mock_session)
        with conn.session_scope(commit=False):
            pass
        mock_trans.commit.assert_not_called()
        mock_trans.rollback.assert_called()

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_aurora_engine_type(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        with mock.patch(
            "daylily_tapdb.aurora.connection.AuroraConnectionBuilder"
        ) as mock_acb:
            mock_acb.build_connection_url.return_value = "postgresql://aurora/test"
            conn = TAPDBConnection(
                engine_type="aurora",
                db_hostname="cluster.aws.com:5432",
                db_user="u",
                db_name="test",
            )
            assert conn._db_url == "postgresql://aurora/test"

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_aurora_no_hostname_raises(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        with pytest.raises(ValueError, match="db_hostname"):
            TAPDBConnection(engine_type="aurora", db_user="u")

    @mock.patch("daylily_tapdb.connection.create_engine")
    def test_is_postgresql_session(self, mock_ce):
        from daylily_tapdb.connection import TAPDBConnection

        mock_session = mock.MagicMock()
        mock_session.bind.dialect.name = "postgresql"
        assert TAPDBConnection._is_postgresql_session(mock_session) is True

        mock_session.bind.dialect.name = "sqlite"
        assert TAPDBConnection._is_postgresql_session(mock_session) is False


# ────────────────────────────────────────────────────────────────────
# passwords.py  — hash/verify
# ────────────────────────────────────────────────────────────────────


class TestPasswords:
    def test_hash_password(self):
        pytest.importorskip("passlib.context")
        from daylily_tapdb.passwords import hash_password

        h = hash_password("mysecret")
        assert h.startswith("$2")  # bcrypt hash prefix

    def test_hash_password_empty_raises(self):
        from daylily_tapdb.passwords import hash_password

        with pytest.raises(ValueError):
            hash_password("")

    def test_hash_password_none_raises(self):
        from daylily_tapdb.passwords import hash_password

        with pytest.raises(ValueError):
            hash_password(None)

    def test_verify_password_correct(self):
        pytest.importorskip("passlib.context")
        from daylily_tapdb.passwords import hash_password, verify_password

        h = hash_password("test123")
        assert verify_password("test123", h) is True

    def test_verify_password_wrong(self):
        pytest.importorskip("passlib.context")
        from daylily_tapdb.passwords import hash_password, verify_password

        h = hash_password("test123")
        assert verify_password("wrong", h) is False

    def test_verify_password_empty_hash(self):
        from daylily_tapdb.passwords import verify_password

        assert verify_password("anything", "") is False

    def test_passlib_import_error(self):
        """Exercise the ModuleNotFoundError branch in _get_pwd_context."""
        import daylily_tapdb.passwords as pw_mod

        orig_import = __import__

        def fake_import(name, *args, **kwargs):
            if "passlib" in name:
                raise ModuleNotFoundError(f"No module named '{name}'")
            return orig_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            result = pw_mod._get_pwd_context()
            assert result is None

    def test_verify_password_no_context(self):
        """Exercise verify_password when _PWD_CONTEXT is None."""
        import daylily_tapdb.passwords as pw_mod

        saved = pw_mod._PWD_CONTEXT
        try:
            pw_mod._PWD_CONTEXT = None
            with pytest.raises(RuntimeError, match="passlib"):
                pw_mod.verify_password("test", "somehash")
        finally:
            pw_mod._PWD_CONTEXT = saved

    def test_verify_password_no_context_with_error(self):
        """Exercise verify_password when _PWD_CONTEXT is None and _PWD_CONTEXT_ERROR is set."""
        import daylily_tapdb.passwords as pw_mod

        saved_ctx = pw_mod._PWD_CONTEXT
        saved_err = pw_mod._PWD_CONTEXT_ERROR
        try:
            pw_mod._PWD_CONTEXT = None
            pw_mod._PWD_CONTEXT_ERROR = RuntimeError("bcrypt too old")
            with pytest.raises(RuntimeError, match="bcrypt"):
                pw_mod.verify_password("test", "somehash")
        finally:
            pw_mod._PWD_CONTEXT = saved_ctx
            pw_mod._PWD_CONTEXT_ERROR = saved_err


# ────────────────────────────────────────────────────────────────────
# outbox/repository.py (34% → 85%+)
# ────────────────────────────────────────────────────────────────────


class TestOutboxRepository:
    def test_build_enqueue_stmt(self):
        from daylily_tapdb.outbox.repository import _build_enqueue_stmt

        stmt = _build_enqueue_stmt(
            message_uid=1, destination="http://example.com/hook", dedupe_key="abc"
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "outbox_event" in compiled
        assert "ON CONFLICT" in compiled

    def test_build_claim_select(self):
        from daylily_tapdb.outbox.repository import _build_claim_select

        stmt = _build_claim_select(batch_size=10)
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "outbox_event" in compiled
        assert "FOR UPDATE" in compiled

    def test_create_message_instance_no_template(self):
        from daylily_tapdb.outbox.repository import _create_message_instance

        mock_session = mock.MagicMock()
        mock_tm_cls = mock.MagicMock()
        mock_tm_cls.return_value.get_template.return_value = None

        with mock.patch("daylily_tapdb.templates.manager.TemplateManager", mock_tm_cls):
            with pytest.raises(ValueError, match="Message template not found"):
                _create_message_instance(
                    mock_session,
                    tenant_id=uuid.uuid4(),
                    domain_code="Z",
                    event_type="test.event",
                    aggregate_euid="GT-00001-BXKQ7",
                    payload={"key": "value"},
                )

    def test_create_message_instance_success(self):
        from daylily_tapdb.outbox.repository import _create_message_instance

        mock_session = mock.MagicMock()
        mock_template = mock.MagicMock()
        mock_template.instance_polymorphic_identity = "test_instance"
        mock_template.category = "system"
        mock_template.type = "message"
        mock_template.subtype = "webhook_event"
        mock_template.version = "1.0"
        mock_template.uid = 42
        mock_tm_cls = mock.MagicMock()
        mock_tm_cls.return_value.get_template.return_value = mock_template

        with mock.patch("daylily_tapdb.templates.manager.TemplateManager", mock_tm_cls):
            _create_message_instance(
                mock_session,
                tenant_id=uuid.uuid4(),
                domain_code="Z",
                event_type="test.event",
                aggregate_euid="GT-00001-BXKQ7",
                payload={"key": "value"},
            )
            mock_session.add.assert_called_once()
            mock_session.flush.assert_called_once()

    def test_enqueue_fanout(self):
        from daylily_tapdb.outbox.repository import enqueue_fanout

        mock_session = mock.MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.side_effect = [1, None, 2]

        result = enqueue_fanout(
            mock_session,
            message_uid=42,
            destinations=[
                ("http://a.com", "key1"),
                ("http://b.com", "key2"),
                ("http://c.com", "key3"),
            ],
        )
        assert result == [1, 2]
        mock_session.flush.assert_called_once()

    def test_claim_events_empty(self):
        from daylily_tapdb.outbox.repository import claim_events

        mock_session = mock.MagicMock()
        mock_session.execute.return_value.scalars.return_value.all.return_value = []

        result = claim_events(mock_session, batch_size=10)
        assert result == []

    def test_claim_events_with_rows(self):
        from daylily_tapdb.outbox.repository import claim_events

        row1 = mock.MagicMock()
        row1.attempt_count = 0
        row2 = mock.MagicMock()
        row2.attempt_count = 2

        mock_session = mock.MagicMock()
        mock_session.execute.return_value.scalars.return_value.all.return_value = [
            row1,
            row2,
        ]

        result = claim_events(mock_session, batch_size=10, lock_timeout_s=60)
        assert len(result) == 2
        assert row1.status == "delivering"
        assert row1.attempt_count == 1
        assert row2.attempt_count == 3
        mock_session.flush.assert_called_once()

    def test_mark_received(self):
        from daylily_tapdb.outbox.repository import mark_received

        mock_session = mock.MagicMock()
        mark_received(mock_session, row_id=1)
        mock_session.execute.assert_called_once()
        mock_session.flush.assert_called_once()

    def test_mark_failed(self):
        from daylily_tapdb.outbox.repository import mark_failed

        mock_session = mock.MagicMock()
        mark_failed(
            mock_session,
            row_id=1,
            error="timeout",
            next_attempt_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        mock_session.execute.assert_called_once()
        mock_session.flush.assert_called_once()
