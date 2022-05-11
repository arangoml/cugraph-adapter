#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from typing import Any, List, Set

from arango.graph import Graph as ArangoDBGraph
from cugraph import MultiGraph as cuGraphMultiGraph

from .typings import ArangoMetagraph


class Abstract_ADBCUG_Adapter(ABC):
    def __init__(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def arangodb_to_cugraph(
        self,
        name: str,
        metagraph: ArangoMetagraph,
        is_keep: bool = True,
        **query_options: Any,
    ) -> cuGraphMultiGraph(directed=True):  # type: ignore
        raise NotImplementedError  # pragma: no cover

    def arangodb_collections_to_cugraph(
        self,
        name: str,
        v_cols: Set[str],
        e_cols: Set[str],
        **query_options: Any,
    ) -> cuGraphMultiGraph(directed=True):  # type: ignore
        raise NotImplementedError  # pragma: no cover

    def arangodb_graph_to_cugraph(
        self, name: str, **query_options: Any
    ) -> cuGraphMultiGraph(directed=True):  # type: ignore
        raise NotImplementedError  # pragma: no cover

    def __validate_attributes(self) -> None:
        raise NotImplementedError  # pragma: no cover

    def __fetch_adb_docs(self) -> None:
        raise NotImplementedError  # pragma: no cover

    @property
    def CONNECTION_ATRIBS(self) -> Set[str]:
        return {"hostname", "username", "password", "dbName"}

    @property
    def METAGRAPH_ATRIBS(self) -> Set[str]:
        return {"vertexCollections", "edgeCollections"}

    @property
    def EDGE_DEFINITION_ATRIBS(self) -> Set[str]:
        return {"edge_collection", "from_vertex_collections", "to_vertex_collections"}
