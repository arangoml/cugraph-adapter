#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, List, Optional, Set

from arango.graph import Graph as ADBGraph
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from .typings import ADBMetagraph, CUGId, Json


class Abstract_ADBCUG_Adapter(ABC):
    def __init__(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def arangodb_to_cugraph(
        self,
        name: str,
        metagraph: ADBMetagraph,
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
        edge_definitions: Optional[List[Json]] = None,
        keyify_nodes: bool = False,
        keyify_edges: bool = False,
        overwrite_graph: bool = False,
        **import_options: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover

    def __fetch_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover


class Abstract_ADBCUG_Controller(ABC):
    def _prepare_arangodb_vertex(self, adb_vertex: Json, col: str) -> None:
        raise NotImplementedError  # pragma: no cover

    def _identify_cugraph_node(self, cug_node_id: CUGId, adb_v_cols: List[str]) -> str:
        raise NotImplementedError  # pragma: no cover

    def _identify_cugraph_edge(
        self,
        from_cug_node: Json,
        to_cug_node: Json,
        adb_e_cols: List[str],
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

    @property
    def VALID_KEY_CHARS(self) -> Set[str]:
        return {
            "_",
            "-",
            ":",
            ".",
            "@",
            "(",
            ")",
            "+",
            ",",
            "=",
            ";",
            "$",
            "!",
            "*",
            "'",
            "%",
        }
