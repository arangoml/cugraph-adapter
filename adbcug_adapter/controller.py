#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Any, Set, Tuple

from .abc import Abstract_ADBCUG_Controller
from .typings import CUGId, Json


class ADBCUG_Controller(Abstract_ADBCUG_Controller):
    """ArangoDB-cuGraph controller.

    Responsible for controlling how nodes & edges are handled when
    transitioning from ArangoDB to cuGraph.

    You can derive your own custom ADBCUG_Controller, but it is not
    necessary.
    """

    def _prepare_arangodb_vertex(self, adb_vertex: Json, col: str) -> None:
        """Prepare an ArangoDB vertex before it gets inserted into the cuGraph
        graph.

        Given an ArangoDB vertex, you can modify it before it gets inserted
        into the cuGraph graph, and/or derive a custom node id for cuGraph to use.
        In most cases, it is only required to return the ArangoDB _id of the vertex.

        :param adb_vertex: The ArangoDB vertex object to (optionally) modify.
        :type adb_vertex: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection the vertex belongs to.
        :type col: str
        :return: The ArangoDB _id attribute of the vertex.
        :rtype: str
        """
        return

    def _identify_cugraph_node(self, cug_node_id: CUGId, adb_v_cols: Set[str]) -> str:
        """Given a cuGraph node, and a set of ArangoDB vertex collections defined,
        identify which ArangoDB vertex collection it should belong to.

        NOTE: You must override this function if your cuGraph graph is NOT Homogeneous
        or does NOT comply to ArangoDB standards (i.e the vertex IDs are not formatted
        like "{collection}/{key}").

        :param cug_node_id: The cuGraph ID of the vertex.
        :type cug_node_id: adbcug_adapter.typings.CUGId
        :param adb_v_cols: All ArangoDB vertex collections specified
            by the **edge_definitions** parameter of cugraph_to_arangodb()
        :type adb_v_cols: Set[str]
        :return: The ArangoDB collection name
        :rtype: str
        """
        # In this case, we assume that **cug_node_id** is already a valid ArangoDB _id
        adb_vertex_id: str = str(cug_node_id)
        return adb_vertex_id.split("/")[0]

    def _identify_cugraph_edge(
        self,
        from_cug_node: Json,
        to_cug_node: Json,
        adb_e_cols: Set[str],
    ) -> str:
        """Given a pair of connected cuGraph nodes, and a set of ArangoDB
        edge collections defined, identify which ArangoDB edge collection it
        should belong to.

        NOTE #1: You must override this function if your cuGraph graph is NOT
        Homogeneous.

        NOTE #2: The pair of associated cuGraph nodes can be accessed
        by the **from_cug_node** & **to_cug_node** parameters, and are guaranteed
        to have the following attributes: `{"cug_id", "adb_id", "adb_col", "adb_key"}`

        :param from_cug_node: The cuGraph node representing the edge source.
        :type from_cug_node: adbcug_adapter.typings.Json
        :param to_cug_node: The cuGraph node representing the edge destination.
        :type to_cug_node: adbcug_adapter.typings.Json
        :param adb_e_cols: All ArangoDB edge collections specified
            by the **edge_definitions** parameter of
            ADBCUG_Adapter.cugraph_to_arangodb()
        :type adb_e_cols: Set[str]
        :return: The ArangoDB collection name
        :rtype: str
        """
        # User must override this function if cuGraph graph is not Homogeneous
        raise NotImplementedError  # pragma: no cover

    def _keyify_cugraph_node(self, cug_node_id: CUGId, col: str) -> str:
        """Given a cuGraph node, derive its valid ArangoDB key.

        NOTE: You can override this function if you want to create custom ArangoDB _key
        values from your cuGraph nodes. To enable the use of this method, enable the
        **keyify_nodes** parameter in ADBCUG_Adapter.cugraph_to_arangodb().

        :param cug_node_id: The cuGraph node id.
        :type cug_node_id: adbcug_adapter.typings.CUGId
        :param col: The ArangoDB collection the vertex belongs to.
        :type col: str
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        # In this case, we assume that **cug_node_id** is already a valid ArangoDB _id
        adb_vertex_id: str = str(cug_node_id)
        return adb_vertex_id.split("/")[1]

    def _keyify_cugraph_edge(
        self,
        from_cug_node: Json,
        to_cug_node: Json,
        col: str,
    ) -> str:
        """Given a pair of connected cuGraph nodes, and the collection
        this edge belongs to, derive the edge's valid ArangoDB key.

        NOTE #1: You can override this function if you want to create custom ArangoDB
        _key values from your cuGraph edges. To enable the use of this method, enable
        the **keyify_edges** parameter in ADBCUG_Adapter.cugraph_to_arangodb().

        NOTE #2: The pair of associated cuGraph nodes can be accessed
        by the **from_cug_node** & **to_cug_node** parameters, and are guaranteed
        to have the following attributes: `{"cug_id", "adb_id", "adb_col", "adb_key"}`

        :param from_cug_node: The cuGraph node representing the edge source.
        :type from_cug_node: adbcug_adapter.typings.Json
        :param to_cug_node: The cuGraph node representing the edge destination.
        :type to_cug_node: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection the edge belongs to.
        :type col: str
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        # User must override this function if custom ArangoDB _key values are required
        raise NotImplementedError  # pragma: no cover

    def _string_to_arangodb_key_helper(self, string: str) -> str:
        """Given a string, derive a valid ArangoDB _key string.

        :param string: A (possibly) invalid _key string value.
        :type string: str
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        res: str = ""
        for s in string:
            if s.isalnum() or s in self.VALID_KEY_CHARS:
                res += s

        return res
