#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Set, Tuple, Union

from arango.cursor import Cursor
from arango.database import Database
from arango.graph import Graph as ADBGraph
from arango.result import Result
from cudf import DataFrame
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from .abc import Abstract_ADBCUG_Adapter
from .controller import ADBCUG_Controller
from .typings import ADBMetagraph, CUGId, Json
from .utils import logger


class ADBCUG_Adapter(Abstract_ADBCUG_Adapter):
    """ArangoDB-cuGraph adapter.

    :param db: A python-arango database instance
    :type db: arango.database.Database
    :param controller: The ArangoDB-cuGraph controller, used to prepare ArangoDB
        nodes before insertion into cuGraph, optionally re-defined by the user
        if needed (otherwise defaults to ADBCUG_Controller).
    :type controller: ADBCUG_Controller
    :param logging_lvl: Defaults to logging.INFO. Other useful options are
        logging.DEBUG (more verbose), and logging.WARNING (less verbose).
    :type logging_lvl: str | int
    :raise TypeError: If invalid parameters
    """

    def __init__(
        self,
        db: Database,
        controller: ADBCUG_Controller = ADBCUG_Controller(),
        logging_lvl: Union[str, int] = logging.INFO,
    ):
        self.set_logging(logging_lvl)

        if issubclass(type(db), Database) is False:
            msg = "**db** parameter must inherit from arango.database.Database"
            raise TypeError(msg)

        if issubclass(type(controller), ADBCUG_Controller) is False:
            msg = "**controller** parameter must inherit from ADBCUG_Controller"
            raise TypeError(msg)

        self.__db = db
        self.__cntrl: ADBCUG_Controller = controller

        logger.info(f"Instantiated ADBCUG_Adapter with database '{db.name}'")

    @property
    def db(self) -> Database:
        return self.__db

    @property
    def cntrl(self) -> ADBCUG_Controller:
        return self.__cntrl

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    def arangodb_to_cugraph(
        self,
        name: str,
        metagraph: ADBMetagraph,
        **query_options: Any,
    ) -> CUGMultiGraph:
        """Create a cuGraph graph from an ArangoDB metagraph.

        :param name: The cuGraph graph name.
        :type name: str
        :param metagraph: An object defining vertex & edge collections to import to
            cuGraph.
        :type metagraph: adbcug_adapter.typings.ADBMetagraph
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        :raise ValueError: If missing required keys in metagraph
        """
        logger.debug(f"Starting arangodb_to_cugraph({name}, ...):")
        self.__validate_attributes("graph", set(metagraph), self.METAGRAPH_ATRIBS)

        # Maps ArangoDB vertex IDs to cuGraph node IDs
        adb_map: Dict[str, Dict[str, Union[CUGId, str]]] = dict()
        cug_edges: List[Tuple[CUGId, CUGId, Any]] = []

        adb_v: Json
        for col, _ in metagraph["vertexCollections"].items():
            logger.debug(f"Preparing '{col}' vertices")
            for adb_v in self.__fetch_adb_docs(col, query_options):
                adb_id: str = adb_v["_id"]
                self.__cntrl._prepare_arangodb_vertex(adb_v, col)
                cug_id: str = adb_v["_id"]
                adb_map[adb_id] = {"cug_id": cug_id, "collection": col}

        adb_e: Json
        for col, _ in metagraph["edgeCollections"].items():
            logger.debug(f"Preparing '{col}' edges")
            for adb_e in self.__fetch_adb_docs(col, query_options):
                from_node_id: CUGId = adb_map[adb_e["_from"]]["cug_id"]
                to_node_id: CUGId = adb_map[adb_e["_to"]]["cug_id"]
                cug_edges.append((from_node_id, to_node_id, adb_e.get("weight", 0)))

        logger.debug(f"Inserting {len(cug_edges)} edges")
        cug_graph = CUGMultiGraph(directed=True)
        cug_graph.from_cudf_edgelist(
            DataFrame(cug_edges, columns=["src", "dst", "weight"]),
            source="src",
            destination="dst",
            edge_attr="weight",
            renumber=False,
        )

        logger.info(f"Created cuGraph '{name}' Graph")
        return cug_graph

    def arangodb_collections_to_cugraph(
        self,
        name: str,
        v_cols: Set[str],
        e_cols: Set[str],
        **query_options: Any,
    ) -> CUGMultiGraph:
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
        metagraph: ADBMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        return self.arangodb_to_cugraph(name, metagraph, **query_options)

    def arangodb_graph_to_cugraph(
        self, name: str, **query_options: Any
    ) -> CUGMultiGraph:
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
        e_cols = {e_d["edge_collection"] for e_d in graph.edge_definitions()}

        return self.arangodb_collections_to_cugraph(
            name, v_cols, e_cols, **query_options
        )

    def cugraph_to_arangodb(
        self,
        name: str,
        cug_graph: CUGGraph,
        edge_definitions: List[Json],
        batch_size: int = 1000,
        keyify_nodes: bool = False,
        keyify_edges: bool = False,
    ) -> ADBGraph:
        """Create an ArangoDB graph from a cuGraph graph, and a set of edge
        definitions.

        :param name: The ArangoDB graph name.
        :type name: str
        :param cug_graph: The existing cuGraph graph.
        :type cug_graph: cugraph.classes.graph.Graph
        :param edge_definitions: List of edge definitions, where each edge definition
            entry is a dictionary with fields "edge_collection",
            "from_vertex_collections" and "to_vertex_collections"
            (see below for example).
        :type edge_definitions: List[adbcug_adapter.typings.Json]
        :param batch_size: The maximum number of documents to insert at once
        :type batch_size: int
        :param keyify_nodes: If set to True, will create custom node keys based on the
            behavior of ADBCUG_Controller._keyify_cugraph_node().
            Otherwise, ArangoDB _key values for vertices will range from 1 to N,
            where N is the number of cugraph nodes.
        :type keyify_nodes: bool
        :param keyify_edges: If set to True, will create custom edge keys based on
            the behavior of the ADBNX_Controller._keyify_cugraph_edge().
            Otherwise, ArangoDB _key values for edges will range from 1 to E,
            where E is the number of cugraph edges.
        :type keyify_edges: bool
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph

        Here is an example entry for parameter **edge_definitions**:

        .. code-block:: python
        [
            {
                "edge_collection": "teach",
                "from_vertex_collections": ["teachers"],
                "to_vertex_collections": ["lectures"]
            }
        ]
        """
        logger.debug(f"Starting cugraph_to_arangodb('{name}', ...):")
        for e_d in edge_definitions:
            self.__validate_attributes(
                "Edge Definitions", set(e_d), self.EDGE_DEFINITION_ATRIBS
            )

        self.__db.delete_graph(name, ignore_missing=True)
        adb_graph: ADBGraph = self.__db.create_graph(name, edge_definitions)

        adb_v_cols = adb_graph.vertex_collections()
        adb_e_cols = [e_d["edge_collection"] for e_d in edge_definitions]

        has_one_vcol = len(adb_v_cols) == 1
        has_one_ecol = len(adb_e_cols) == 1
        logger.debug(f"Is graph '{name}' homogeneous? {has_one_vcol and has_one_ecol}")

        cug_map = dict()  # Maps cuGraph node IDs to ArangoDB vertex IDs
        adb_documents: DefaultDict[str, List[Json]] = defaultdict(list)

        cug_id: CUGId
        cug_nodes = cug_graph.nodes().values_host
        logger.debug(f"Preparing {len(cug_nodes)} cugraph nodes")
        for i, cug_id in enumerate(cug_nodes, 1):
            col = (
                adb_v_cols[0]
                if has_one_vcol
                else self.__cntrl._identify_cugraph_node(cug_id, adb_v_cols)
            )
            key = (
                self.__cntrl._keyify_cugraph_node(cug_id, col)
                if keyify_nodes
                else str(i)
            )

            adb_v_id = col + "/" + key
            cug_map[cug_id] = {
                "cug_id": cug_id,
                "adb_id": adb_v_id,
                "adb_col": col,
                "adb_key": key,
            }

            adb_vertex = {"_id": adb_v_id}
            self.__insert_adb_docs(col, adb_documents[col], adb_vertex, batch_size)

        from_node_id: CUGId
        to_node_id: CUGId
        logger.debug(f"Preparing {cug_graph.number_of_edges()} cugraph edges")
        for i, (from_node_id, to_node_id, *weight) in enumerate(
            cug_graph.view_edge_list().values_host, 1
        ):
            from_n = cug_map[from_node_id]
            to_n = cug_map[to_node_id]

            col = (
                adb_e_cols[0]
                if has_one_ecol
                else self.__cntrl._identify_cugraph_edge(from_n, to_n, adb_e_cols)
            )
            key = (
                self.__cntrl._keyify_cugraph_edge(from_n, to_n, col)
                if keyify_edges
                else str(i)
            )

            adb_edge = {
                "_id": col + "/" + key,
                "_from": from_n["adb_id"],
                "_to": to_n["adb_id"],
            }

            if cug_graph.is_weighted():
                adb_edge["weight"] = weight[0]

            self.__insert_adb_docs(
                col,
                adb_documents[col],
                adb_edge,
                batch_size,
            )

        for col, doc_list in adb_documents.items():  # insert remaining documents
            if doc_list:
                logger.debug(f"Inserting last {len(doc_list)} documents into '{col}'")
                self.__db.collection(col).import_bulk(doc_list, on_duplicate="replace")

        logger.info(f"Created ArangoDB '{name}' Graph")
        return adb_graph

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

    def __fetch_adb_docs(self, col: str, query_options: Any) -> Result[Cursor]:
        """Fetches ArangoDB documents within a collection.

        :param col: The ArangoDB collection.
        :type col: str
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type query_options: Any
        :return: Result cursor.
        :rtype: arango.cursor.Cursor
        """
        aql = f"""
            FOR doc IN {col}
                RETURN doc
        """

        return self.__db.aql.execute(aql, **query_options)

    def __insert_adb_docs(
        self,
        col: str,
        col_docs: List[Json],
        doc: Json,
        batch_size: int,
    ) -> None:
        """Insert an ArangoDB document into a list. If the list exceeds
        batch_size documents, insert into the ArangoDB collection.

        :param col: The collection name
        :type col: str
        :param col_docs: The existing documents data belonging to the collection.
        :type col_docs: List[adbnx_adapter.typings.Json]
        :param doc: The current document to insert.
        :type doc: adbnx_adapter.typings.Json
        :param batch_size: The maximum number of documents to insert at once
        :type batch_size: int
        """
        col_docs.append(doc)

        if len(col_docs) >= batch_size:
            logger.debug(f"Inserting next {batch_size} batch documents into '{col}'")
            self.__db.collection(col).import_bulk(col_docs, on_duplicate="replace")
            col_docs.clear()
