#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, List, Set

from arango.graph import Graph as ADBGraph
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from .typings import ArangoMetagraph, CUGId, Json


class Abstract_ADBCUG_Adapter(ABC):
    def __init__(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def arangodb_to_cugraph(
        self,
        name: str,
        metagraph: ArangoMetagraph,
        is_keep: bool = True,
        **query_options: Any,
    ) -> CUGMultiGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_collections_to_cugraph(
        self,
        name: str,
        v_cols: Set[str],
        e_cols: Set[str],
        **query_options: Any,
    ) -> CUGMultiGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_graph_to_cugraph(
        self, name: str, **query_options: Any
    ) -> CUGMultiGraph:
        raise NotImplementedError  # pragma: no cover

    def cugraph_to_arangodb(
        self,
        name: str,
        cug_graph: CUGGraph,
        edge_definitions: List[Json],
        batch_size: int = 1000,
        keyify_nodes: bool = False,
        keyify_edges: bool = False,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def __validate_attributes(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def __fetch_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def __insert_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def METAGRAPH_ATRIBS(self) -> Set[str]:
        return {"vertexCollections", "edgeCollections"}

    @property
    def EDGE_DEFINITION_ATRIBS(self) -> Set[str]:
        return {"edge_collection", "from_vertex_collections", "to_vertex_collections"}


class Abstract_ADBCUG_Controller(ABC):
    def _prepare_arangodb_vertex(self, adb_vertex: Json, col: str) -> None:
        raise NotImplementedError  # pragma: no cover

    def _identify_cugraph_node(self, cug_node_id: CUGId, adb_v_cols: Set[str]) -> str:
        raise NotImplementedError  # pragma: no cover

    def _identify_cugraph_edge(
        self,
        from_cug_node: Json,
        to_cug_node: Json,
        adb_e_cols: Set[str],
    ) -> str:
        raise NotImplementedError  # pragma: no cover

    def _keyify_cugraph_node(self, cug_node_id: CUGId, col: str) -> str:
        raise NotImplementedError  # pragma: no cover

    def _keyify_cugraph_edge(
        self,
        from_cug_node: Json,
        to_cug_node: Json,
        col: str,
    ) -> str:
        raise NotImplementedError  # pragma: no cover
