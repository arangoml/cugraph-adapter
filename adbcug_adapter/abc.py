#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, Dict, List, Optional, Set

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
        **adb_export_kwargs: Any,
    ) -> CUGMultiGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_collections_to_cugraph(
        self,
        name: str,
        v_cols: Set[str],
        e_cols: Set[str],
        **adb_export_kwargs: Any,
    ) -> CUGMultiGraph:
        raise NotImplementedError  # pragma: no cover

    def arangodb_graph_to_cugraph(
        self, name: str, **adb_export_kwargs: Any
    ) -> CUGMultiGraph:
        raise NotImplementedError  # pragma: no cover

    def cugraph_to_arangodb(
        self,
        name: str,
        cug_graph: CUGGraph,
        edge_definitions: Optional[List[Json]] = None,
        orphan_collections: Optional[List[str]] = None,
        overwrite_graph: bool = False,
        batch_size: Optional[int] = None,
        use_async: bool = False,
        **adb_import_kwargs: Any,
    ) -> ADBGraph:
        raise NotImplementedError  # pragma: no cover


class Abstract_ADBCUG_Controller(ABC):
    def _prepare_arangodb_vertex(self, adb_vertex: Json, col: str) -> None:
        raise NotImplementedError  # pragma: no cover

    def _identify_cugraph_node(self, cug_node_id: CUGId, adb_v_cols: List[str]) -> str:
        raise NotImplementedError  # pragma: no cover

    def _identify_cugraph_edge(
        self,
        from_cug_id: CUGId,
        to_cug_id: CUGId,
        cug_map: Dict[CUGId, str],
        adb_e_cols: List[str],
    ) -> str:
        raise NotImplementedError  # pragma: no cover

    def _keyify_cugraph_node(self, i: int, cug_node_id: CUGId, col: str) -> str:
        raise NotImplementedError  # pragma: no cover

    def _keyify_cugraph_edge(
        self,
        i: int,
        from_cug_id: CUGId,
        to_cug_id: CUGId,
        cug_map: Dict[CUGId, str],
        col: str,
    ) -> str:
        raise NotImplementedError  # pragma: no cover

    def _prepare_cugraph_node(
        self,
        cug_node: Json,
        col: str,
    ) -> None:
        raise NotImplementedError  # pragma: no cover

    def _prepare_cugraph_edge(
        self,
        cug_edge: Json,
        col: str,
    ) -> None:
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
