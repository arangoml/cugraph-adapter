#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple, Union

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
        edge_attr: str = "weight",
        **query_options: Any,
    ) -> CUGMultiGraph:
        """Create a cuGraph graph from an ArangoDB metagraph.

        :param name: The cuGraph graph name.
        :type name: str
        :param metagraph: An object defining vertex & edge
            collections to import to cuGraph.
        :type metagraph: adbcug_adapter.typings.ADBMetagraph
        :param edge_attr: The weight attribute name of your ArangoDB edges.
            Defaults to 'weight'. If no weight attribute is present,
            will set the edge weight value to 0.
        :type edge_attr: str
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        :raise ValueError: If missing required keys in metagraph

        Here is an example entry for parameter **metagraph**:

        .. code-block:: python
        {
            "vertexCollections": {
                "account": {}, # cuGraph does not support node attributes
            },
            "edgeCollections": {
                "transaction": {}, # cuGraph does not support edge attributes
            },
        }
        """
        logger.debug(f"Starting arangodb_to_cugraph({name}, ...):")

        # Maps ArangoDB vertex IDs to cuGraph node IDs
        adb_map: Dict[str, Dict[str, Union[CUGId, str]]] = dict()
        cug_edges: List[Tuple[CUGId, CUGId, Any]] = []

        adb_v: Json
        for col, _ in metagraph["vertexCollections"].items():
            logger.debug(f"Preparing '{col}' vertices")
            for i, adb_v in enumerate(self.__fetch_adb_docs(col, query_options), 1):
                logger.debug(f'V{i}: {adb_v["_id"]}')

                adb_id: str = adb_v["_id"]
                self.__cntrl._prepare_arangodb_vertex(adb_v, col)
                cug_id: str = adb_v["_id"]

                adb_map[adb_id] = {"cug_id": cug_id, "collection": col}

        adb_e: Json
        for col, _ in metagraph["edgeCollections"].items():
            logger.debug(f"Preparing '{col}' edges")
            for i, adb_e in enumerate(self.__fetch_adb_docs(col, query_options), 1):
                logger.debug(f"E{i}: {adb_e['_id']}")

                from_node_id: CUGId = adb_map[adb_e["_from"]]["cug_id"]
                to_node_id: CUGId = adb_map[adb_e["_to"]]["cug_id"]
                cug_edges.append((from_node_id, to_node_id, adb_e.get(edge_attr, 0)))

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
        edge_attr: str = "weight",
        **query_options: Any,
    ) -> CUGMultiGraph:
        """Create a cuGraph graph from ArangoDB collections.
        :param name: The cuGraph graph name.
        :type name: str
        :param v_cols: A set of vertex collections to import to cuGraph.
        :type v_cols: Set[str]
        :param e_cols: A set of edge collections to import to cuGraph.
        :type e_cols: Set[str]
        :param edge_attr: The weight attribute name of your ArangoDB edges.
            Defaults to 'weight'. If no weight attribute is present,
            will set the edge weight value to 0.
        :type edge_attr: str
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        metagraph: ADBMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        return self.arangodb_to_cugraph(name, metagraph, edge_attr, **query_options)

    def arangodb_graph_to_cugraph(
        self, name: str, edge_attr: str = "weight", **query_options: Any
    ) -> CUGMultiGraph:
        """Create a cuGraph graph from an ArangoDB graph.
        :param name: The ArangoDB graph name.
        :type name: str
        :param edge_attr: The weight attribute name of your ArangoDB edges.
            Defaults to 'weight'. If no weight attribute is present,
            will set the edge weight value to 0.
        :type edge_attr: str
        :param query_options: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type query_options: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        graph = self.__db.graph(name)
        v_cols = graph.vertex_collections()
        e_cols = {e_d["edge_collection"] for e_d in graph.edge_definitions()}

        return self.arangodb_collections_to_cugraph(
            name, v_cols, e_cols, edge_attr, **query_options
        )

    def cugraph_to_arangodb(
        self,
        name: str,
        cug_graph: CUGGraph,
        edge_definitions: Optional[List[Json]] = None,
        keyify_nodes: bool = False,
        keyify_edges: bool = False,
        overwrite_graph: bool = False,
        edge_attr: str = "weight",
        **import_options: Dict[str, Any],
    ) -> ADBGraph:
        """Create an ArangoDB graph from a cuGraph graph, and a set of edge
        definitions.

        :param name: The ArangoDB graph name.
        :type name: str
        :param cug_graph: The existing cuGraph graph.
        :type cug_graph: cugraph.classes.graph.Graph
        :param edge_definitions: List of edge definitions, where each edge
            definition entry is a dictionary with fields "edge_collection",
            "from_vertex_collections" and "to_vertex_collections" (see below
            for example). Can be omitted if the graph already exists.
        :type edge_definitions: List[adbnx_adapter.typings.Json]
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
        :param edge_attr: If your cuGraph graph is weighted, you can specify
            the edge attribute name used to represent your cuGraph edge weight values
            once transferred into ArangoDB. Defaults to 'weight'.
        :type edge_attr: str
        :param overwrite_graph: Overwrites the graph if it already exists.
            Does not drop associated collections.
        :type overwrite_graph: bool
        :param import_options: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type import_options: Any
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

        if overwrite_graph:
            logger.debug("Overwrite graph flag is True. Deleting old graph.")
            self.__db.delete_graph(name, ignore_missing=True)

        if self.__db.has_graph(name):
            adb_graph = self.__db.graph(name)
        else:
            adb_graph = self.__db.create_graph(name, edge_definitions)

        adb_v_cols: List[str] = adb_graph.vertex_collections()
        adb_e_cols: List[str] = [
            e_d["edge_collection"] for e_d in adb_graph.edge_definitions()
        ]

        has_one_vcol = len(adb_v_cols) == 1
        has_one_ecol = len(adb_e_cols) == 1
        logger.debug(f"Is graph '{name}' homogeneous? {has_one_vcol and has_one_ecol}")

        cug_map = dict()  # Maps cuGraph node IDs to ArangoDB vertex IDs
        adb_documents: DefaultDict[str, List[Json]] = defaultdict(list)

        cug_id: CUGId
        cug_nodes = cug_graph.nodes().values_host
        logger.debug(f"Preparing {len(cug_nodes)} cugraph nodes")
        for i, cug_id in enumerate(cug_nodes, 1):
            logger.debug(f"N{i}: {cug_id}")

            col = (
                adb_v_cols[0]
                if has_one_vcol
                else self.__cntrl._identify_cugraph_node(cug_id, adb_v_cols)
            )

            if col not in adb_v_cols:
                msg = f"'{cug_id}' identified as '{col}', which is not in {adb_v_cols}"
                raise ValueError(msg)

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

            adb_documents[col].append({"_id": adb_v_id})

        from_node_id: CUGId
        to_node_id: CUGId
        logger.debug(f"Preparing {cug_graph.number_of_edges()} cugraph edges")
        for i, (from_node_id, to_node_id, *weight) in enumerate(
            cug_graph.view_edge_list().values_host, 1
        ):
            edge_str = f"({from_node_id}, {to_node_id})"
            logger.debug(f"E{i}: {edge_str}")

            from_n = cug_map[from_node_id]
            to_n = cug_map[to_node_id]

            col = (
                adb_e_cols[0]
                if has_one_ecol
                else self.__cntrl._identify_cugraph_edge(from_n, to_n, adb_e_cols)
            )

            if col not in adb_e_cols:
                msg = f"{edge_str} identified as '{col}', which is not in {adb_e_cols}"
                raise ValueError(msg)

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
                adb_edge[edge_attr] = weight[0]

            adb_documents[col].append(adb_edge)

        for col, doc_list in adb_documents.items():  # import documents into ArangoDB
            logger.debug(f"Inserting {len(doc_list)} documents into '{col}'")
            result = self.__db.collection(col).import_bulk(doc_list, **import_options)
            logger.debug(result)

        logger.info(f"Created ArangoDB '{name}' Graph")
        return adb_graph

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
