#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, List, Set, Tuple, Union

from arango.cursor import Cursor
from arango.database import Database
from arango.result import Result
from cudf import DataFrame
from cugraph import MultiGraph as cuGraphMultiGraph

from .abc import Abstract_ADBCUG_Adapter
from .controller import ADBCUG_Controller
from .typings import ArangoMetagraph, CuGId, Json
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

    def set_logging(self, level: Union[int, str]) -> None:
        logger.setLevel(level)

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
        logger.debug(f"Starting arangodb_to_cugraph({name}, ...):")
        self.__validate_attributes("graph", set(metagraph), self.METAGRAPH_ATRIBS)

        # Maps ArangoDB vertex IDs to cuGraph node IDs
        adb_map: Dict[str, Dict[str, Union[CuGId, str]]] = dict()
        cg_edges: List[Tuple[CuGId, CuGId]] = []

        adb_v: Json
        for col, atribs in metagraph["vertexCollections"].items():
            logger.debug(f"Preparing '{col}' vertices")
            for adb_v in self.__fetch_adb_docs(col, atribs, is_keep, query_options):
                adb_id: str = adb_v["_id"]
                cug_id = self.__cntrl._prepare_arangodb_vertex(adb_v, col)
                adb_map[adb_id] = {"cug_id": cug_id, "collection": col}

        adb_e: Json
        for col, atribs in metagraph["edgeCollections"].items():
            logger.debug(f"Preparing '{col}' edges")
            for adb_e in self.__fetch_adb_docs(col, atribs, is_keep, query_options):
                from_node_id: CuGId = adb_map[adb_e["_from"]]["cug_id"]
                to_node_id: CuGId = adb_map[adb_e["_to"]]["cug_id"]
                cg_edges.append((from_node_id, to_node_id))

        logger.debug(f"Inserting {len(cg_edges)} edges")
        srcs = [s for (s, _) in cg_edges]
        dsts = [d for (_, d) in cg_edges]
        cg_graph = cuGraphMultiGraph(directed=True)
        cg_graph.from_cudf_edgelist(DataFrame({"source": srcs, "destination": dsts}))

        logger.info(f"Created cuGraph '{name}' Graph")
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

        return self.arangodb_to_cugraph(name, metagraph, is_keep=True, **query_options)

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
