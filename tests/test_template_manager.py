"""Tests for TemplateManager.

These are unit tests with fake Session/Query objects to exercise caching and
input validation paths without a database.
"""

from dataclasses import dataclass


@dataclass
class _FakeTemplate:
    uuid: str
    euid: str
    is_deleted: bool
    category: str = "generic"
    type: str = "generic"
    subtype: str = "generic"
    version: str = "1.0"


class _FakeQuery:
    def __init__(self, first_result):
        self._first_result = first_result

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._first_result


class _FakeSession:
    def __init__(self, get_map=None, query_first=None):
        self._get_map = get_map or {}
        self._query_first = query_first
        self.query_called = 0

    def get(self, _cls, uuid):
        return self._get_map.get(uuid)

    def query(self, _cls):
        self.query_called += 1
        return _FakeQuery(self._query_first)


def test_get_template_invalid_code_warns_and_returns_none(caplog):
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    sess = _FakeSession()

    got = tm.get_template(sess, "bad/format")
    assert got is None
    assert any("Invalid template code format" in r.message for r in caplog.records)


def test_get_template_uses_uuid_cache():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tmpl = _FakeTemplate(uuid="u1", euid="GT1", is_deleted=False)
    code = "generic/generic/generic/1.0/"
    tm._template_uuid_cache[code] = "u1"

    sess = _FakeSession(get_map={"u1": tmpl})
    got = tm.get_template(sess, code)

    assert got is tmpl
    assert sess.query_called == 0


def test_get_template_query_populates_caches():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tmpl = _FakeTemplate(uuid="u2", euid="GT2", is_deleted=False)
    code = "generic/generic/generic/1.0/"

    sess = _FakeSession(query_first=tmpl)
    got = tm.get_template(sess, code)

    assert got is tmpl
    assert tm._template_uuid_cache[code] == "u2"
    assert tm._template_euid_cache["GT2"] == "u2"


def test_get_template_by_euid_uses_cache():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tmpl = _FakeTemplate(uuid="u3", euid="GT3", is_deleted=False)
    tm._template_euid_cache["GT3"] = "u3"

    sess = _FakeSession(get_map={"u3": tmpl})
    got = tm.get_template_by_euid(sess, "GT3")
    assert got is tmpl
    assert sess.query_called == 0
