#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from .abc import Abstract_ADBCUG_Controller
from .typings import CuGId, Json


class ADBCUG_Controller(Abstract_ADBCUG_Controller):
    """ArangoDB-NetworkX controller.

    Responsible for controlling how nodes & edges are handled when
    transitioning from ArangoDB to cuGraph.

    You can derive your own custom ADBNX_Controller, but it is not
    necessary for Homogeneous graphs.
    """

    def _prepare_arangodb_vertex(self, adb_vertex: Json, col: str) -> CuGId:
        """Prepare an ArangoDB vertex before it gets inserted into the cuGraph
        graph.

        Given an ArangoDB vertex, you can modify it before it gets inserted
        into the cuGraph graph, and/or derive a custom node id for networkx to use.
        In most cases, it is only required to return the ArangoDB _id of the vertex.

        :param adb_vertex: The ArangoDB vertex object to (optionally) modify.
        :type adb_vertex: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection the vertex belongs to.
        :type col: str
        :return: The ArangoDB _id attribute of the vertex.
        :rtype: str
        """
        adb_vertex_id: str = adb_vertex["_id"]
        return adb_vertex_id

    def _prepare_arangodb_edge(self, adb_edge: Json, col: str) -> CuGId:
        """Prepare an ArangoDB edge before it gets inserted into the cuGraph
        graph.

        Given an ArangoDB edge, you can modify it before it gets inserted
        into the cuGraph graph. In most cases, it is only required to
        return the ArangoDB _id of the edge.

        :param adb_edge: The ArangoDB edge object to (optionally) modify.
        :type adb_edge: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection the edge belongs to.
        :type col: str
        :return: The ArangoDB _id attribute of the edge.
        :rtype: str
        """
        adb_edge_id: str = adb_edge["_id"]
        return adb_edge_id
