from __future__ import annotations

from daylily_tapdb.services import external_refs as eg
from daylily_tapdb.services import external_refs as svc


def test_external_graph_helpers_are_imported_from_service_module() -> None:
    assert eg.ExternalGraphRef is svc.ExternalGraphRef
    assert eg.resolve_external_graph_refs is svc.resolve_external_graph_refs
    assert eg.get_external_ref_by_index is svc.get_external_ref_by_index
    assert eg.fetch_remote_graph is svc.fetch_remote_graph
    assert eg.fetch_remote_object_detail is svc.fetch_remote_object_detail
    assert eg.namespace_external_graph is svc.namespace_external_graph
