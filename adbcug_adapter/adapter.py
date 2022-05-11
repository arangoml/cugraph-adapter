#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Set, Tuple, Union

from arango import ArangoClient
from arango.cursor import Cursor
from arango.graph import Graph as ArangoDBGraph
from arango.result import Result
from cudf import DataFrame
from cugraph import MultiGraph as cuGraphMultiGraph

from .abc import Abstract_ADBCUG_Adapter
from .typings import ArangoMetagraph, CuGId, Json


class ADBCUG_Adapter(Abstract_ADBCUG_Adapter):
    """ArangoDB-cuGraph adapter.

    :param conn: Connection details to an ArangoDB instance.
    :type conn: ADBCUG_adapter.typings.Json
    :raise ValueError: If missing required keys in conn
    """

    def __init__(
        self, conn: Json,
    ):
        self.__validate_attributes("connection", set(conn), self.CONNECTION_ATRIBS)

        username: str = conn["username"]
        password: str = conn["password"]
        db_name: str = conn["dbName"]
        host: str = conn["hostname"]
        protocol: str = conn.get("protocol", "https")
        port = str(conn.get("port", 8529))

        url = protocol + "://" + host + ":" + port

        print(f"Connecting to {url}")
        self.__db = ArangoClient(hosts=url).db(db_name, username, password, verify=True)

    def arangodb_to_cugraph(
        self,
        name: str,
        metagraph: ArangoMetagraph,
        is_keep: bool = True,
        **query_options: Any,
    ) -> cuGraphMultiGraph(directed=True):  # type: ignore
        """Create a cuGraph graph from graph attributes.
        :param name: The cuGraph graph name.
        :type name: str
        :param metagraph: An object defining vertex & edge collections to import to
            cuGraph, along with their associated attributes to keep.
        :type metagraph: adbcug_adapter.typings.ArangoMetagraph
        :param is_keep: Only keep the document attributes specified in **metagraph**
            when importing to cuGraph (is True by default).
        :type is_keep: bool
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        :raise ValueError: If missing required keys in metagraph
        Here is an example entry for parameter **metagraph**:
        .. code-block:: python
        {
            "vertexCollections": {
                "account": {"Balance", "account_type", "customer_id", "rank"},
                "bank": {"Country", "Id", "bank_id", "bank_name"},
                "customer": {"Name", "Sex", "Ssn", "rank"},
            },
            "edgeCollections": {
                "accountHolder": {},
                "transaction": {
                    "transaction_amt", "receiver_bank_id", "sender_bank_id"
                },
            },
        }
        """
        self.__validate_attributes("graph", set(metagraph), self.METAGRAPH_ATRIBS)

        # Maps ArangoDB vertex IDs to cuGraph node IDs
        adb_map: Dict[str, Dict[str, Union[CuGId, str]]] = dict()
        cg_edges: List[Tuple[CuGId, CuGId]] = []

        adb_v: Json
        for col, atribs in metagraph["vertexCollections"].items():
            for adb_v in self.__fetch_adb_docs(col, atribs, is_keep, query_options):
                adb_id: str = adb_v["_id"]
                nx_id = self.__cntrl._prepare_arangodb_vertex(adb_v, col)
                adb_map[adb_id] = {"nx_id": nx_id, "collection": col}

        adb_e: Json
        for col, atribs in metagraph["edgeCollections"].items():
            for adb_e in self.__fetch_adb_docs(col, atribs, is_keep, query_options):
                from_node_id: CuGId = adb_map[adb_e["_from"]]["nx_id"]
                to_node_id: CuGId = adb_map[adb_e["_to"]]["nx_id"]
                self.__cntrl._prepare_arangodb_edge(adb_e, col)
                cg_edges.append((from_node_id, to_node_id))

        srcs = [s for (s, _) in cg_edges]
        dsts = [d for (_, d) in cg_edges]
        cg_graph = cuGraphMultiGraph(directed=True)
        cg_graph.from_cudf_edgelist(
            DataFrame({"source": srcs, "destination": dsts})
        )

        print(f"cuGraph: {name} created")
        return cg_graph

    def arangodb_collections_to_cugraph(
        self,
        name: str,
        v_cols: Set[str],
        e_cols: Set[str],
        **query_options: Any,
    ) -> cuGraphMultiGraph(directed=True):  # type: ignore
        """Create a cuGraph graph from ArangoDB collections.
        :param name: The cuGraph graph name.
        :type name: str
        :param v_cols: A set of vertex collections to import to cuGraph.
        :type v_cols: Set[str]
        :param e_cols: A set of edge collections to import to cuGraph.
        :type e_cols: Set[str]
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        metagraph: ArangoMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        return self.arangodb_to_cugraph(
            name, metagraph, is_keep=True, **query_options
        )

    def arangodb_graph_to_cugraph(
        self, name: str, **query_options: Any
    ) -> cuGraphMultiGraph(directed=True):  # type: ignore
        """Create a cuGraph graph from an ArangoDB graph.
        :param name: The ArangoDB graph name.
        :type name: str
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        graph = self.__db.graph(name)
        v_cols = graph.vertex_collections()
        e_cols = {col["edge_collection"] for col in graph.edge_definitions()}

        return self.arangodb_collections_to_cugraph(
            name, v_cols, e_cols, **query_options
        )

    def __validate_attributes(
        self, type: str, attributes: Set[str], valid_attributes: Set[str]
    ) -> None:
        """Validates that a set of attributes includes the required valid
        attributes.

        :param type: The context of the attribute validation
            (e.g connection attributes, graph attributes, etc).
        :type type: str
        :param attributes: The provided attributes, possibly invalid.
        :type attributes: Set[str]
        :param valid_attributes: The valid attributes.
        :type valid_attributes: Set[str]
        :raise ValueError: If **valid_attributes** is not a subset of **attributes**
        """
        if valid_attributes.issubset(attributes) is False:
            missing_attributes = valid_attributes - attributes
            raise ValueError(f"Missing {type} attributes: {missing_attributes}")

    def __fetch_adb_docs(
        self, col: str, attributes: Set[str], is_keep: bool, query_options: Any
    ) -> Result[Cursor]:
        """Fetches ArangoDB documents within a collection.

        :param col: The ArangoDB collection.
        :type col: str
        :param attributes: The set of document attributes.
        :type attributes: Set[str]
        :param is_keep: Only keep the attributes specified in **attributes** when
            returning the document. Otherwise, all document attributes are included.
        :type is_keep: bool
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: Result cursor.
        :rtype: arango.cursor.Cursor
        """
        aql = f"""
            FOR doc IN {col}
                RETURN {is_keep} ?
                    MERGE(
                        KEEP(doc, {list(attributes)}),
                        {{"_id": doc._id}},
                        doc._from ? {{"_from": doc._from, "_to": doc._to}}: {{}}
                    )
                : doc
        """

        return self.__db.aql.execute(aql, **query_options)
