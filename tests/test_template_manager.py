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
    def __init__(self, first_result=None, all_result=None):
        self._first_result = first_result
        self._all_result = all_result or []
        self.filters = []

    def filter(self, *args, **kwargs):
        self.filters.append((args, kwargs))
        return self

    def first(self):
        return self._first_result

    def all(self):
        return list(self._all_result)


class _FakeSession:
    def __init__(self, get_map=None, query_first=None, query_all=None):
        self._get_map = get_map or {}
        self._query_first = query_first
        self._query_all = query_all or []
        self.query_called = 0
        self.last_query = None

    def get(self, _cls, uuid):
        return self._get_map.get(uuid)

    def query(self, _cls):
        self.query_called += 1
        q = _FakeQuery(first_result=self._query_first, all_result=self._query_all)
        self.last_query = q
        return q


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


def test_get_template_cache_hit_deleted_falls_back_to_query():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    code = "generic/generic/generic/1.0/"

    deleted = _FakeTemplate(uuid="ud", euid="GTd", is_deleted=True)
    fresh = _FakeTemplate(uuid="uf", euid="GTf", is_deleted=False)
    tm._template_uuid_cache[code] = "ud"

    sess = _FakeSession(get_map={"ud": deleted}, query_first=fresh)
    got = tm.get_template(sess, code)

    assert got is fresh
    assert sess.query_called == 1
    assert tm._template_uuid_cache[code] == "uf"
    assert tm._template_euid_cache["GTf"] == "uf"


def test_get_template_by_euid_uses_cache():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tmpl = _FakeTemplate(uuid="u3", euid="GT3", is_deleted=False)
    tm._template_euid_cache["GT3"] = "u3"

    sess = _FakeSession(get_map={"u3": tmpl})
    got = tm.get_template_by_euid(sess, "GT3")
    assert got is tmpl
    assert sess.query_called == 0


def test_get_template_by_euid_query_populates_cache():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tmpl = _FakeTemplate(uuid="u4", euid="GT4", is_deleted=False)

    sess = _FakeSession(query_first=tmpl)
    got = tm.get_template_by_euid(sess, "GT4")

    assert got is tmpl
    assert tm._template_euid_cache["GT4"] == "u4"
    assert sess.query_called == 1


def test_get_template_by_euid_cache_hit_deleted_falls_back_to_query():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    deleted = _FakeTemplate(uuid="ud", euid="GTd", is_deleted=True)
    fresh = _FakeTemplate(uuid="uf", euid="GTf", is_deleted=False)
    tm._template_euid_cache["GTf"] = "ud"

    sess = _FakeSession(get_map={"ud": deleted}, query_first=fresh)
    got = tm.get_template_by_euid(sess, "GTf")

    assert got is fresh
    assert sess.query_called == 1
    assert tm._template_euid_cache["GTf"] == "uf"


def test_clear_cache_empties_caches():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tm._template_uuid_cache["a/b/c/d/"] = "u1"
    tm._template_euid_cache["GT1"] = "u1"
    tm.clear_cache()
    assert tm._template_uuid_cache == {}
    assert tm._template_euid_cache == {}


def test_list_templates_applies_filters_and_returns_all():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    t1 = _FakeTemplate(uuid="u1", euid="GT1", is_deleted=False, category="a", type="t")
    t2 = _FakeTemplate(uuid="u2", euid="GT2", is_deleted=True, category="a", type="t")

    sess = _FakeSession(query_all=[t1, t2])
    got = tm.list_templates(sess, category="a", type_="t", include_deleted=False)

    assert got == [t1, t2]
    # include_deleted=False + category + type => 3 filter calls
    assert sess.last_query is not None
    assert len(sess.last_query.filters) == 3


def test_template_code_from_template_formats_code():
    from daylily_tapdb.templates.manager import TemplateManager

    tm = TemplateManager()
    tmpl = _FakeTemplate(
        uuid="u9",
        euid="GT9",
        is_deleted=False,
        category="c",
        type="t",
        subtype="s",
        version="1.2",
    )
    assert tm.template_code_from_template(tmpl) == "c/t/s/1.2/"
