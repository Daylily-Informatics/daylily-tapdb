"""Unit tests for graph category style mapping."""

from __future__ import annotations

import admin.main as admin_main


def test_graph_category_styles_have_required_fields() -> None:
    for category, style in admin_main.GRAPH_CATEGORY_STYLES.items():
        assert isinstance(category, str)
        assert style["color"].startswith("#")
        assert style["shape"]
        assert style["marker"]


def test_get_graph_category_style_known_and_unknown() -> None:
    known = admin_main.get_graph_category_style("workflow")
    assert known["color"] == admin_main.GRAPH_CATEGORY_STYLES["workflow"]["color"]
    assert known["shape"] == admin_main.GRAPH_CATEGORY_STYLES["workflow"]["shape"]
    assert known["marker"] == admin_main.GRAPH_CATEGORY_STYLES["workflow"]["marker"]

    unknown = admin_main.get_graph_category_style("not_a_real_category")
    assert unknown == admin_main.DEFAULT_GRAPH_NODE_STYLE

    missing = admin_main.get_graph_category_style(None)
    assert missing == admin_main.DEFAULT_GRAPH_NODE_STYLE
