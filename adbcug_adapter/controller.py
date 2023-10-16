#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict

from .abc import Abstract_ADBCUG_Controller
from .typings import CUGId, Json


class ADBCUG_Controller(Abstract_ADBCUG_Controller):
    """ArangoDB-cuGraph controller.

    Responsible for controlling how nodes & edges are handled when
    transitioning from ArangoDB to cuGraph.

    You can derive your own custom ADBCUG_Controller.
    """

    def _prepare_arangodb_vertex(self, adb_vertex: Json, col: str) -> None:
        """Prepare an ArangoDB vertex before it gets inserted into the cuGraph
        graph.

        Given an ArangoDB vertex, you can modify it before it gets inserted
        into the cuGraph graph, and/or derive a custom node id for cuGraph
        to use by updating the "_id" attribute of the vertex (otherwise the
        vertex's current "_id" value will be used)

        :param adb_vertex: The ArangoDB vertex object to (optionally) modify.
        :type adb_vertex: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection the vertex belongs to.
        :type col: str
        """
        pass

    def _identify_cugraph_node(self, cug_node_id: CUGId, adb_v_cols: List[str]) -> str:
        """Given a cuGraph node, and a list of ArangoDB vertex collections defined,
        identify which ArangoDB vertex collection it should belong to.

        NOTE: You must override this function if len(**adb_v_cols**) > 1
        AND **cug_node_id* does NOT comply to ArangoDB standards
        (i.e "{collection}/{key}").

        :param cug_node_id: The cuGraph ID of the vertex.
        :type cug_node_id: adbcug_adapter.typings.CUGId
        :param adb_v_cols: All ArangoDB vertex collections specified
            by the **edge_definitions** parameter of cugraph_to_arangodb()
        :type adb_v_cols: List[str]
        :return: The ArangoDB collection name
        :rtype: str
        """
        # In this case, we assume that **cug_node_id** is already a valid ArangoDB _id
        adb_vertex_id: str = str(cug_node_id)
        return adb_vertex_id.split("/")[0]

    def _identify_cugraph_edge(
        self, from_node_id: str, to_node_id: str, adb_e_cols: List[str], cug_map: Dict[CUGId, str]
    ) -> str:
        """Given a pair of connected cuGraph nodes, and a list of ArangoDB
        edge collections defined, identify which ArangoDB edge collection it
        should belong to.

        NOTE: You must override this function if len(**adb_e_cols**) > 1.

        :param from_node_id: The ID of the cuGraph node representing the edge source.
        :type from_node_id: str
        :param to_node_id: The ID of the cuGraph node representing the edge destination.
        :type to_node_id: str
        :param adb_e_cols: All ArangoDB edge collections specified
            by the **edge_definitions** parameter of
            ADBCUG_Adapter.cugraph_to_arangodb()
        :type adb_e_cols: List[str]
        :param cug_map: The mapping of cuGraph node IDs to ArangoDB vertex IDs.
        :type cug_map: Dict[CUGId, str]
        :return: The ArangoDB collection name
        :rtype: str
        """
        # User must override this function if len(adb_e_cols) > 1
        raise NotImplementedError  # pragma: no cover

    def _keyify_cugraph_node(self, cug_node_id: CUGId, col: str) -> str:
        """Given a cuGraph node, derive its valid ArangoDB key.

        NOTE: You can override this function if you want to create custom ArangoDB _key
        values from your cuGraph nodes. To enable the use of this method, enable the
        **keyify_nodes** parameter in ADBCUG_Adapter.cugraph_to_arangodb().

        :param cug_node_id: The cuGraph node id.
        :type cug_node_id: adbcug_adapter.typings.CUGId
        :param col: The ArangoDB collection the **cug_node_id** belongs to.
        :type col: str
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        # In this case, we assume that **cug_node_id** is already a valid ArangoDB _id
        # Otherwise, user must override this function if custom ArangoDB _key
        # values are required for nodes
        adb_vertex_id: str = str(cug_node_id)
        return self._string_to_arangodb_key_helper(adb_vertex_id.split("/")[1])

    def _keyify_cugraph_edge(
        self,
        from_node_id: str,
        to_node_id: str,
        col: str,
        cug_map: Dict[CUGId, str],
    ) -> str:
        """Given a pair of connected cuGraph nodes, and the collection
        this edge belongs to, derive the edge's valid ArangoDB key.

        NOTE #1: You can override this function if you want to create custom ArangoDB
        _key values from your cuGraph edges. To enable the use of this method, enable
        the **keyify_edges** parameter in ADBCUG_Adapter.cugraph_to_arangodb().

        :param from_node_id: The ID of the cuGraph node representing the edge source.
        :type from_node_id: str
        :param to_node_id: The ID of the cuGraph node representing the edge destination.
        :type to_node_id: str
        :param col: The ArangoDB collection the edge belongs to.
        :type col: str
        :param cug_map: The mapping of cuGraph node IDs to ArangoDB vertex IDs.
        :type cug_map: Dict[CUGId, str]
        :return: A valid ArangoDB _key value.
        :rtype: str
        """
        # User must override this function if custom ArangoDB _key values are
        # required for edges
        raise NotImplementedError  # pragma: no cover

    def _prepare_cugraph_node(self, cug_node: Json, col: str) -> None:
        """Prepare a cuGraph node before it gets inserted into the ArangoDB
        collection **col**.

        Given an ArangoDB representation of a cuGraph node (i.e {_key: ...}),
        you can (optionally) modify the object before it gets inserted into its
        designated ArangoDB collection.

        :param cug_node: The ArangoDB representation of the cuGraph node
            to (optionally) modify.
        :type cug_node: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection associated to the node.
        :type col: str
        """
        pass

    def _prepare_cugraph_edge(self, cug_edge: Json, col: str) -> None:
        """Prepare a cuGraph edge before it gets inserted into the ArangoDB
        collection **col**.

        Given an ArangoDB representation of a cuGraph edge
        (i.e {_key: ..., _from: ..., _to: ...}), you can (optionally) modify
        the object before it gets inserted into its designated ArangoDB collection.

        :param cug_edge: The ArangoDB representation of the cuGraph edge
            to (optionally) modify.
        :type cug_edge: adbcug_adapter.typings.Json
        :param col: The ArangoDB collection associated to the edge.
        :type col: str
        """
        pass

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
