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
