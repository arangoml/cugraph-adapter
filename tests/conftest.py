import logging
import subprocess
from pathlib import Path
from typing import Any, List

from arango import ArangoClient
from arango.database import StandardDatabase
from cudf import DataFrame
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from adbcug_adapter import ADBCUG_Adapter
from adbcug_adapter.controller import ADBCUG_Controller
from adbcug_adapter.typings import CUGId

PROJECT_DIR = Path(__file__).parent.parent

db: StandardDatabase
adbcug_adapter: ADBCUG_Adapter
bipartite_adbcug_adapter: ADBCUG_Adapter
divisibility_adbcug_adapter: ADBCUG_Adapter


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

    # # temporary workaround for build.yml purposes
    # con = get_temp_credentials()

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

    global adbcug_adapter, bipartite_adbcug_adapter, divisibility_adbcug_adapter
    adbcug_adapter = ADBCUG_Adapter(db, logging_lvl=logging.DEBUG)
    bipartite_adbcug_adapter = ADBCUG_Adapter(db, Custom_ADBCUG_Controller())
    divisibility_adbcug_adapter = ADBCUG_Adapter(db, Custom_ADBCUG_Controller())

    if db.has_graph("fraud-detection") is False:
        arango_restore(con, "examples/data/fraud_dump")
        db.create_graph(
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


def arango_restore(connection: Any, path_to_data: str) -> None:
    protocol = "http+ssl://" if "https://" in connection["url"] else "tcp://"
    url = protocol + connection["url"].partition("://")[-1]

    subprocess.check_call(
        f'chmod -R 755 ./tools/arangorestore && ./tools/arangorestore \
            -c none --server.endpoint {url} --server.database {connection["dbName"]} \
                --server.username {connection["username"]} \
                    --server.password "{connection["password"]}" \
                        --input-directory "{PROJECT_DIR}/{path_to_data}"',
        cwd=f"{PROJECT_DIR}/tests",
        shell=True,  # nosec
    )


def get_divisibility_graph() -> CUGGraph:
    edges = DataFrame(
        [
            (f"numbers/{j}", f"numbers/{i}", j / i)
            for i in range(1, 101)
            for j in range(1, 101)
            if j % i == 0
        ],
        columns=["src", "dst", "quotient"],
    )

    cug_graph = CUGMultiGraph(directed=True)
    cug_graph.from_cudf_edgelist(
        edges, source="src", destination="dst", edge_attr="quotient"
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
    cug_graph.from_cudf_edgelist(edges, source="src", destination="dst")
    return cug_graph


def get_drivers_graph() -> CUGGraph:
    edges = DataFrame(
        [("P-John", "C-BMW"), ("P-Mark", "C-Audi")],
        columns=["src", "dst"],
    )

    cug_graph = CUGGraph()
    cug_graph.from_cudf_edgelist(edges, source="src", destination="dst")
    return cug_graph


def get_likes_graph() -> CUGGraph:
    edges = DataFrame(
        [("P-John", "P-Emily", 1), ("P-Emily", "P-John", 0)],
        columns=["src", "dst", "likes"],
    )

    cug_graph = CUGGraph()
    cug_graph.from_cudf_edgelist(
        edges, source="src", destination="dst", edge_attr="likes"
    )
    return cug_graph

class Custom_ADBCUG_Controller(ADBCUG_Controller):
    def _identify_cugraph_node(self, cug_node_id: CUGId, adb_v_cols: List[str]) -> str:
        return str(cug_node_id).split("/")[0]

    def _keyify_cugraph_node(self, i: int, cug_node_id: CUGId, col: str) -> str:
        return self._string_to_arangodb_key_helper(str(cug_node_id).split("/")[1])
