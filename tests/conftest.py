import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from arango import ArangoClient
from arango.database import StandardDatabase
from cudf import DataFrame
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from adbcug_adapter import ADBCUG_Adapter
from adbcug_adapter.controller import ADBCUG_Controller
from adbcug_adapter.typings import CUGId, Json

PROJECT_DIR = Path(__file__).parent.parent

db: StandardDatabase
adbcug_adapter: ADBCUG_Adapter
custom_adbcug_adapter: ADBCUG_Adapter


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--dbName", action="store", default="_system")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="")


def pytest_configure(config: Any) -> None:
    con = {
        "url": config.getoption("url"),
        "username": config.getoption("username"),
        "password": config.getoption("password"),
        "dbName": config.getoption("dbName"),
    }

    print("----------------------------------------")
    print("URL: " + con["url"])
    print("Username: " + con["username"])
    print("Password: " + con["password"])
    print("Database: " + con["dbName"])
    print("----------------------------------------")

    global db
    db = ArangoClient(hosts=con["url"]).db(
        con["dbName"], con["username"], con["password"], verify=True
    )

    global adbcug_adapter, custom_adbcug_adapter
    adbcug_adapter = ADBCUG_Adapter(db, logging_lvl=logging.DEBUG)
    custom_adbcug_adapter = ADBCUG_Adapter(db, Custom_ADBCUG_Controller())

    arango_restore(con, "examples/data/fraud_dump")
    arango_restore(con, "examples/data/imdb_dump")

    # Create Fraud Detection Graph
    adbcug_adapter.db.delete_graph("fraud-detection", ignore_missing=True)
    adbcug_adapter.db.create_graph(
        "fraud-detection",
        edge_definitions=[
            {
                "edge_collection": "accountHolder",
                "from_vertex_collections": ["customer"],
                "to_vertex_collections": ["account"],
            },
            {
                "edge_collection": "transaction",
                "from_vertex_collections": ["account"],
                "to_vertex_collections": ["account"],
            },
        ],
    )


def arango_restore(con: Json, path_to_data: str) -> None:
    restore_prefix = "./assets/" if os.getenv("GITHUB_ACTIONS") else ""
    protocol = "http+ssl://" if "https://" in con["url"] else "tcp://"
    url = protocol + con["url"].partition("://")[-1]
    # A small hack to work around empty passwords
    password = f"--server.password {con['password']}" if con["password"] else ""

    subprocess.check_call(
        f'chmod -R 755 ./assets/arangorestore && {restore_prefix}arangorestore \
            -c none --server.endpoint {url} --server.database {con["dbName"]} \
                --server.username {con["username"]} {password} \
                    --input-directory "{PROJECT_DIR}/{path_to_data}"',
        cwd=f"{PROJECT_DIR}/tests",
        shell=True,
    )


def get_divisibility_graph() -> CUGGraph:
    edges = DataFrame(
        [
            (f"numbers/{j}", f"numbers/{i}", j / i)
            for i in range(1, 101)
            for j in range(1, 101)
            if j % i == 0
        ],
        columns=["src", "dst", "weight"],
    )

    cug_graph = CUGMultiGraph(directed=True)
    cug_graph.from_cudf_edgelist(
        edges, source="src", destination="dst", edge_attr="weight", renumber=False
    )

    return cug_graph


def get_bipartite_graph() -> CUGGraph:
    # This graph has nodes with valid ArangoDB collection names,
    # but has invalid node _key values
    edges = DataFrame(
        [("col_a/<valid> {key]???", "col_b/~!v!a!l!i!dk!e!y~")],
        columns=["src", "dst"],
    )

    cug_graph = CUGGraph()
    cug_graph.from_cudf_edgelist(edges, source="src", destination="dst", renumber=False)


class Custom_ADBCUG_Controller(ADBCUG_Controller):
    def _keyify_cugraph_node(self, cug_node_id: CUGId, col: str) -> str:
        return self._string_to_arangodb_key_helper(str(cug_node_id).split("/")[1])
