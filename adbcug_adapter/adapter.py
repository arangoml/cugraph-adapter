#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set, Tuple, Union

from arango.cursor import Cursor
from arango.database import StandardDatabase
from arango.graph import Graph as ADBGraph
from cudf import DataFrame, Series
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph
from rich.console import Group
from rich.live import Live
from rich.progress import Progress

from .abc import Abstract_ADBCUG_Adapter
from .controller import ADBCUG_Controller
from .typings import ADBMetagraph, CUGId, Json
from .utils import (
    get_bar_progress,
    get_export_spinner_progress,
    get_import_spinner_progress,
    logger,
)


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
        db: StandardDatabase,
        controller: ADBCUG_Controller = ADBCUG_Controller(),
        logging_lvl: Union[str, int] = logging.INFO,
    ):
        self.set_logging(logging_lvl)

        if issubclass(type(db), StandardDatabase) is False:
            msg = "**db** parameter must inherit from arango.database.StandardDatabase"
            raise TypeError(msg)

        if issubclass(type(controller), ADBCUG_Controller) is False:
            msg = "**controller** parameter must inherit from ADBCUG_Controller"
            raise TypeError(msg)

        self.__db = db
        self.__async_db = db.begin_async_execution(return_result=False)
        self.__cntrl = controller

        logger.info(f"Instantiated ADBCUG_Adapter with database '{db.name}'")

    @property
    def db(self) -> StandardDatabase:
        return self.__db  # pragma: no cover

    @property
    def cntrl(self) -> ADBCUG_Controller:
        return self.__cntrl  # pragma: no cover

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

    ###############################
    # Public: ArangoDB -> cuGraph #
    ###############################

    def arangodb_to_cugraph(
        self,
        name: str,
        metagraph: ADBMetagraph,
        edge_attr: str = "weights",
        default_edge_attr_value: int = 0,
        cug_graph: Optional[CUGMultiGraph] = None,
        **adb_export_kwargs: Any,
    ) -> CUGMultiGraph:
        """Create a cuGraph graph from an ArangoDB metagraph.

        :param name: The cuGraph graph name.
        :type name: str
        :param metagraph: An object defining vertex & edge
            collections to import to cuGraph.
        :type metagraph: adbcug_adapter.typings.ADBMetagraph
        :param edge_attr: The weight attribute name of your ArangoDB edges.
            Defaults to 'weights'. If no weight attribute is present,
            will set the edge weight value to **default_edge_attr_value**.
        :type edge_attr: str
        :param default_edge_attr_value: The default value set to the edge attribute
            if **edge_attr** is not present in the ArangoDB edge. Defaults to 0.
        :type default_edge_attr_value: int
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type adb_export_kwargs: Any
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
        logger.debug(f"--arangodb_to_cugraph('{name}')--")

        # This maps the ArangoDB vertex IDs to cuGraph node IDs
        adb_map: Dict[str, CUGId] = dict()

        # This stores the to-be-inserted cuGraph edges (coo format)
        cug_edges: List[Tuple[CUGId, CUGId, Any]] = []

        ######################
        # Vertex Collections #
        ######################

        for v_col, _ in metagraph["vertexCollections"].items():
            logger.debug(f"Preparing '{v_col}' vertices")

            # 1. Fetch ArangoDB vertices
            v_col_cursor, v_col_size = self.__fetch_adb_docs(v_col, **adb_export_kwargs)

            # 2. Process ArangoDB vertices
            self.__process_adb_cursor(
                "#8000FF",
                v_col_cursor,
                v_col_size,
                self.__process_adb_vertex,
                v_col,
                adb_map,
            )

        ####################
        # Edge Collections #
        ####################

        for e_col, _ in metagraph["edgeCollections"].items():
            logger.debug(f"Preparing '{e_col}' edges")

            # 1. Fetch ArangoDB edges
            e_col_cursor, e_col_size = self.__fetch_adb_docs(e_col, **adb_export_kwargs)

            # 2. Process ArangoDB edges
            self.__process_adb_cursor(
                "#9C46FF",
                e_col_cursor,
                e_col_size,
                self.__process_adb_edge,
                e_col,
                adb_map,
                cug_edges,
                edge_attr,
                default_edge_attr_value,
            )

        cug_graph = self.__create_cug_graph(cug_graph, cug_edges, edge_attr)

        logger.info(f"Created cuGraph '{name}' Graph")
        return cug_graph

    def arangodb_collections_to_cugraph(
        self,
        name: str,
        v_cols: Set[str],
        e_cols: Set[str],
        edge_attr: str = "weights",
        default_edge_attr_value: int = 0,
        **adb_export_kwargs: Any,
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
        :param default_edge_attr_value: The default value set to the edge attribute
            if **edge_attr** is not present in the ArangoDB edge. Defaults to 0.
        :type default_edge_attr_value: int
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type adb_export_kwargs: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        metagraph: ADBMetagraph = {
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        }

        return self.arangodb_to_cugraph(name, metagraph, edge_attr, **adb_export_kwargs)

    def arangodb_graph_to_cugraph(
        self,
        name: str,
        edge_attr: str = "weights",
        default_edge_attr_value: int = 0,
        **adb_export_kwargs: Any,
    ) -> CUGMultiGraph:
        """Create a cuGraph graph from an ArangoDB graph.
        :param name: The ArangoDB graph name.
        :type name: str
        :param edge_attr: The weight attribute name of your ArangoDB edges.
            Defaults to 'weight'. If no weight attribute is present,
            will set the edge weight value to 0.
        :type edge_attr: str
        :param default_edge_attr_value: The default value set to the edge attribute
            if **edge_attr** is not present in the ArangoDB edge. Defaults to 0.
        :type default_edge_attr_value: int
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.aql.AQL.execute
        :type adb_export_kwargs: Any
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        graph = self.__db.graph(name)
        v_cols = graph.vertex_collections()
        e_cols = {e_d["edge_collection"] for e_d in graph.edge_definitions()}

        return self.arangodb_collections_to_cugraph(
            name, v_cols, e_cols, edge_attr, **adb_export_kwargs
        )

    ###############################
    # Public: cuGraph -> ArangoDB #
    ###############################

    def cugraph_to_arangodb(
        self,
        name: str,
        cug_graph: CUGGraph,
        edge_definitions: Optional[List[Json]] = None,
        orphan_collections: Optional[List[str]] = None,
        overwrite_graph: bool = False,
        batch_size: Optional[int] = None,
        use_async: bool = False,
        src_series_key: str = "src",
        dst_series_key: str = "dst",
        edge_attr: str = "weights",
        **adb_import_kwargs: Any,
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
        :type edge_definitions: List[Dict[str, Any]]
        :param orphan_collections: A list of vertex collections that will be stored as
            orphans in the ArangoDB graph. Can be omitted if the graph already exists.
        :type orphan_collections: List[str]
        :param overwrite_graph: Overwrites the graph if it already exists.
            Does not drop associated collections.
        :type overwrite_graph: bool
        :param batch_size: If specified, runs the ArangoDB Data Ingestion
            process for every **batch_size** cuGraph nodes/edges within **cug_graph**.
            Defaults to `len(cug_nodes)` & `len(cug_edges)`.
        :type batch_size: int | None
        :param use_async: Performs asynchronous ArangoDB ingestion if enabled.
            Defaults to False.
        :type use_async: bool
        :param src_series_key: The cuGraph edge list source series key.
            Defaults to 'src'.
        :type src_series_key: str
        :param dst_series_key: The cuGraph edge list destination series key.
            Defaults to 'dst'.
        :type dst_series_key: str
        :param edge_attr: If your cuGraph graph is weighted, you can specify
            the edge attribute name used to represent your cuGraph edge weight values
            once transferred into ArangoDB. Defaults to 'weight'.
        :type edge_attr: str
        :param adb_import_kwargs: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :type adb_import_kwargs: Any
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
        logger.debug(f"--cugraph_to_arangodb('{name}')--")

        adb_graph = self.__create_adb_graph(
            name, overwrite_graph, edge_definitions, orphan_collections
        )

        adb_v_cols: List[str] = adb_graph.vertex_collections()  # type: ignore
        adb_e_cols: List[str] = [
            c["edge_collection"] for c in adb_graph.edge_definitions()  # type: ignore
        ]

        has_one_v_col = len(adb_v_cols) == 1
        has_one_e_col = len(adb_e_cols) == 1
        logger.debug(f"Is '{name}' homogeneous? {has_one_v_col and has_one_e_col}")

        # This maps cuGraph node IDs to ArangoDB vertex IDs
        cug_map: Dict[CUGId, str] = dict()

        # Stores to-be-inserted ArangoDB documents by collection name
        adb_docs: DefaultDict[str, List[Json]] = defaultdict(list)

        spinner_progress = get_import_spinner_progress("    ")

        #################
        # cuGraph Nodes #
        #################

        cug_id: CUGId

        cug_nodes = cug_graph.nodes().values_host
        node_batch_size = batch_size or len(cug_nodes)

        bar_progress = get_bar_progress("(CUG → ADB): Nodes", "#97C423")
        bar_progress_task = bar_progress.add_task("Nodes", total=len(cug_nodes))

        with Live(Group(bar_progress, spinner_progress)):
            for i, cug_id in enumerate(cug_nodes, 1):
                bar_progress.advance(bar_progress_task)

                # 1. Process cuGraph node
                self.__process_cug_node(
                    i,
                    cug_id,
                    cug_map,
                    adb_docs,
                    adb_v_cols,
                    has_one_v_col,
                )

                # 2. Insert batch of nodes
                if i % node_batch_size == 0:
                    self.__insert_adb_docs(
                        spinner_progress, adb_docs, use_async, **adb_import_kwargs
                    )

            # Insert remaining nodes
            self.__insert_adb_docs(
                spinner_progress, adb_docs, use_async, **adb_import_kwargs
            )

        #################
        # cuGraph Edges #
        #################

        from_node_id: CUGId
        to_node_id: CUGId

        cug_edges: DataFrame = cug_graph.view_edge_list()
        edge_batch_size = batch_size or len(cug_edges)

        bar_progress = get_bar_progress("(CUG → ADB): Edges", "#5E3108")
        bar_progress_task = bar_progress.add_task("Edges", total=len(cug_edges))

        cug_weights = cug_edges[edge_attr] if cug_graph.is_weighted() else None

        with Live(Group(bar_progress, spinner_progress)):
            for i in range(len(cug_edges)):
                bar_progress.advance(bar_progress_task)

                from_node_id = cug_edges[src_series_key][i]
                to_node_id = cug_edges[dst_series_key][i]

                # 1. Process cuGraph edge
                self.__process_cug_edge(
                    i,
                    from_node_id,
                    to_node_id,
                    cug_map,
                    adb_docs,
                    adb_e_cols,
                    has_one_e_col,
                    edge_attr,
                    cug_weights,
                )

                # 2. Insert batch of edges
                if i % edge_batch_size == 0:
                    self.__insert_adb_docs(
                        spinner_progress, adb_docs, use_async, **adb_import_kwargs
                    )

            # Insert remaining edges
            self.__insert_adb_docs(
                spinner_progress, adb_docs, use_async, **adb_import_kwargs
            )

        logger.info(f"Created ArangoDB '{name}' Graph")
        return adb_graph

    ################################
    # Private: ArangoDB -> cuGraph #
    ################################

    def __fetch_adb_docs(
        self,
        col: str,
        **adb_export_kwargs: Any,
    ) -> Tuple[Cursor, int]:
        """ArangoDB -> cuGraph: Fetches ArangoDB documents within a collection.

        :param col: The ArangoDB collection.
        :type col: str
        :param adb_export_kwargs: Keyword arguments to specify AQL query options when
            fetching documents from the ArangoDB instance.
        :type adb_export_kwargs: Any
        :return: The document cursor along with the total collection size.
        :rtype: Tuple[arango.cursor.Cursor, int]
        """
        col_size: int = self.__db.collection(col).count()  # type: ignore

        with get_export_spinner_progress(f"ADB Export: '{col}' ({col_size})") as p:
            p.add_task(col)

            cursor: Cursor = self.__db.aql.execute(  # type: ignore
                "FOR doc IN @@col RETURN doc",
                bind_vars={"@col": col},
                **{**adb_export_kwargs, **{"stream": True}},
            )

            return cursor, col_size

    def __process_adb_cursor(
        self,
        progress_color: str,
        cursor: Cursor,
        col_size: int,
        process_adb_doc: Callable[..., None],
        col: str,
        adb_map: Dict[str, CUGId],
        *args: Any,
    ) -> None:
        """ArangoDB -> cuGraph: Processes the ArangoDB Cursors for vertices and edges.

        :param progress_color: The progress bar color.
        :type progress_color: str
        :param cursor: The ArangoDB cursor for the current **col**.
        :type cursor: arango.cursor.Cursor
        :param process_adb_doc: The function to process the cursor data.
        :type process_adb_doc: Callable
        :param col: The ArangoDB collection for the current **cursor**.
        :type col: str
        :param col_size: The size of **col**.
        :type col_size: int
        :param adb_map: Maps ArangoDB vertex IDs to cuGraph node IDs.
        :type adb_map: Dict[str, adbcug_adapter.typings.CUGId]
        """

        progress = get_bar_progress(f"(ADB → CUG): '{col}'", progress_color)
        progress_task_id = progress.add_task(col, total=col_size)

        with Live(Group(progress)):
            while not cursor.empty():
                for doc in cursor.batch():  # type: ignore # false positive
                    progress.advance(progress_task_id)

                    process_adb_doc(doc, col, adb_map, *args)

                cursor.batch().clear()  # type: ignore # false positive
                if cursor.has_more():
                    cursor.fetch()

    def __process_adb_vertex(
        self,
        adb_v: Json,
        v_col: str,
        adb_map: Dict[str, CUGId],
    ) -> None:
        """ArangoDB -> cuGraph: Processes an ArangoDB vertex.

        :param adb_v: The ArangoDB vertex.
        :type adb_v: Dict[str, Any]
        :param v_col: The ArangoDB vertex collection.
        :type v_col: str
        :param adb_map: Maps ArangoDB vertex IDs to cuGraph node IDs.
        :type adb_map: Dict[str, adbcug_adapter.typings.CUGId]
        """
        adb_id: str = adb_v["_id"]
        self.__cntrl._prepare_arangodb_vertex(adb_v, v_col)
        cug_id: str = adb_v["_id"]

        adb_map[adb_id] = cug_id

    def __process_adb_edge(
        self,
        adb_e: Json,
        e_col: str,
        adb_map: Dict[str, CUGId],
        cug_edges: List[Tuple[CUGId, CUGId, Any]],
        edge_attr: str,
        default_edge_attr_value: int,
    ) -> None:
        """ArangoDB -> cuGraph: Processes an ArangoDB edge.

        :param adb_e: The ArangoDB edge.
        :type adb_e: Dict[str, Any]
        :param e_col: The ArangoDB edge collection.
        :type e_col: str
        :param adb_map: Maps ArangoDB vertex IDs to cuGraph node IDs.
        :type adb_map: Dict[str, adbcug_adapter.typings.CUGId]
        :param cug_edges: To-be-inserted cuGraph edges.
        :type cug_edges: List[Tuple[CUGId, CUGId, Any]]
        :param edge_attr: The weight attribute name of your ArangoDB edges.
        :type edge_attr: str
        :param default_edge_attr_value: The default value set to the edge attribute
            if **edge_attr** is not present in the ArangoDB edge. Defaults to 0.
        :type default_edge_attr_value: int
        """
        from_node_id: CUGId = adb_map[adb_e["_from"]]
        to_node_id: CUGId = adb_map[adb_e["_to"]]

        cug_edges.append(
            (
                from_node_id,
                to_node_id,
                adb_e.get(edge_attr, default_edge_attr_value),
            )
        )

    def __create_cug_graph(
        self,
        cug_graph: Optional[CUGMultiGraph],
        cug_edges: List[Tuple[CUGId, CUGId, Any]],
        edge_attr: str,
    ) -> CUGMultiGraph:
        """AragoDB -> cuGraph: Creates the cuGraph graph.

        :param cug_graph: An existing cuGraph graph.
        :type cug_graph: cugraph.classes.graph.Graph | None
        :param cug_edges: To-be-inserted cuGraph edges.
        :type cug_edges: List[Tuple[CUGId, CUGId, Any]]
        :param edge_attr: The weight attribute name of your ArangoDB edges.
        :type edge_attr: str
        :return: A Multi-Directed cuGraph Graph.
        :rtype: cugraph.structure.graph_classes.MultiDiGraph
        """
        df = DataFrame(cug_edges, columns=["src", "dst", edge_attr])

        cug_graph = cug_graph or CUGMultiGraph(directed=True)
        cug_graph.from_cudf_edgelist(
            df,
            source="src",
            destination="dst",
            edge_attr=edge_attr,
        )

        return cug_graph

    #################################
    # Private: cuGraph -> ArangoDB #
    #################################

    def __create_adb_graph(
        self,
        name: str,
        overwrite_graph: bool,
        edge_definitions: Optional[List[Json]] = None,
        orphan_collections: Optional[List[str]] = None,
    ) -> ADBGraph:
        """cuGraph -> ArangoDB: Creates the ArangoDB graph.

        :param name: The ArangoDB graph name.
        :type name: str
        :param overwrite_graph: Overwrites the graph if it already exists.
        :type overwrite_graph: bool
        :param edge_definitions: ArangoDB edge definitions.
        :type edge_definitions: List[Dict[str, Any]]
        :param orphan_collections: ArangoDB orphan collections.
        :type orphan_collections: List[str]
        :return: The ArangoDB Graph API wrapper.
        :rtype: arango.graph.Graph
        """
        if overwrite_graph:
            logger.debug("Overwrite graph flag is True. Deleting old graph.")
            self.__db.delete_graph(name, ignore_missing=True)

        if self.__db.has_graph(name):
            logger.debug(f"Graph {name} already exists")
            return self.__db.graph(name)

        else:
            logger.debug(f"Creating graph {name}")
            return self.__db.create_graph(  # type: ignore
                name,
                edge_definitions,
                orphan_collections,
            )

    def __process_cug_node(
        self,
        i: int,
        cug_id: CUGId,
        cug_map: Dict[CUGId, str],
        adb_docs: DefaultDict[str, List[Json]],
        adb_v_cols: List[str],
        has_one_v_col: bool,
    ) -> None:
        """cuGraph -> ArangoDB: Processes a cuGraph node.

        :param i: The node index.
        :type i: int
        :param cug_id: The cuGraph node ID.
        :type cug_id: adbcug_adapter.typings.CUGId
        :param cug_map: Maps cuGraph node IDs to ArangoDB vertex IDs.
        :type cug_map: Dict[adbcug_adapter.typings.CUGId, str]
        :param adb_docs: To-be-inserted ArangoDB documents.
        :type adb_docs: DefaultDict[str, List[Dict[str, Any]]]
        :param adb_v_cols: The ArangoDB vertex collections.
        :type adb_v_cols: List[str]
        :param has_one_v_col: True if the Graph has one Vertex collection.
        :type has_one_v_col: bool
        """
        logger.debug(f"N{i}: {cug_id}")

        col = (
            adb_v_cols[0]
            if has_one_v_col
            else self.__cntrl._identify_cugraph_node(cug_id, adb_v_cols)
        )

        if not has_one_v_col and col not in adb_v_cols:
            msg = f"'{cug_id}' identified as '{col}', which is not in {adb_v_cols}"
            raise ValueError(msg)

        key = self.__cntrl._keyify_cugraph_node(i, cug_id, col)

        adb_id = f"{col}/{key}"
        cug_node = {"_id": adb_id, "_key": key}

        cug_map[cug_id] = adb_id

        self.__cntrl._prepare_cugraph_node(cug_node, col)
        adb_docs[col].append(cug_node)

    def __process_cug_edge(
        self,
        i: int,
        from_node_id: CUGId,
        to_node_id: CUGId,
        cug_map: Dict[CUGId, str],
        adb_docs: DefaultDict[str, List[Json]],
        adb_e_cols: List[str],
        has_one_e_col: bool,
        edge_attr: str,
        cug_weights: Optional[Series] = None,
    ) -> None:
        """cuGraph -> ArangoDB: Processes a cuGraph edge.

        :param i: The edge index.
        :type i: int
        :param from_node_id: The cuGraph ID of the source node.
        :type from_node_id: adbcug_adapter.typings.CUGId
        :param to_node_id: The cuGraph ID of the target node.
        :type to_node_id: adbcug_adapter.typings.CUGId
        :param cug_map: Maps cuGraph node IDs to ArangoDB vertex IDs.
        :type cug_map: Dict[adbcug_adapter.typings.CUGId, str]
        :param adb_docs: To-be-inserted ArangoDB documents.
        :type adb_docs: DefaultDict[str, List[Dict[str, Any]]]
        :param adb_e_cols: The ArangoDB edge collections.
        :type adb_e_cols: List[str]
        :param has_one_e_col: True if the Graph has one Edge collection.
        :type has_one_e_col: bool
        :param edge_attr: The weight attribute name of your ArangoDB edges.
        :type edge_attr: str
        :param cug_weights: The cuGraph edge weights (if graph is weighted).
        :type cug_weights: Optional[cudf.Series]
        """
        edge_str = f"({from_node_id}, {to_node_id})"
        logger.debug(f"E{i}: {edge_str}")

        col = (
            adb_e_cols[0]
            if has_one_e_col
            else self.__cntrl._identify_cugraph_edge(
                from_node_id, to_node_id, cug_map, adb_e_cols
            )
        )

        if not has_one_e_col and col not in adb_e_cols:
            msg = f"{edge_str} identified as '{col}', which is not in {adb_e_cols}"
            raise ValueError(msg)

        key = self.__cntrl._keyify_cugraph_edge(
            i, from_node_id, to_node_id, cug_map, col
        )

        cug_edge = {
            "_key": key,
            "_from": cug_map[from_node_id],
            "_to": cug_map[to_node_id],
        }

        if cug_weights is not None:
            cug_edge[edge_attr] = cug_weights[i]

        self.__cntrl._prepare_cugraph_edge(cug_edge, col)
        adb_docs[col].append(cug_edge)

    def __insert_adb_docs(
        self,
        spinner_progress: Progress,
        adb_docs: DefaultDict[str, List[Json]],
        use_async: bool,
        **adb_import_kwargs: Any,
    ) -> None:
        """cuGraph -> ArangoDB: Insert the ArangoDB documents.

        :param spinner_progress: The spinner progress bar.
        :type spinner_progress: rich.progress.Progress
        :param adb_docs: To-be-inserted ArangoDB documents
        :type adb_docs: DefaultDict[str, List[Json]]
        :param use_async: Performs asynchronous ArangoDB ingestion if enabled.
        :type use_async: bool
        :param adb_import_kwargs: Keyword arguments to specify additional
            parameters for ArangoDB document insertion. Full parameter list:
            https://docs.python-arango.com/en/main/specs.html#arango.collection.Collection.import_bulk
        :param adb_import_kwargs: Any
        """
        if len(adb_docs) == 0:
            return

        db = self.__async_db if use_async else self.__db

        # Avoiding "RuntimeError: dictionary changed size during iteration"
        adb_cols = list(adb_docs.keys())

        for col in adb_cols:
            doc_list = adb_docs[col]

            action = f"ADB Import: '{col}' ({len(doc_list)})"
            spinner_progress_task = spinner_progress.add_task("", action=action)

            result = db.collection(col).import_bulk(doc_list, **adb_import_kwargs)
            logger.debug(result)

            del adb_docs[col]

            spinner_progress.stop_task(spinner_progress_task)
            spinner_progress.update(spinner_progress_task, visible=False)
