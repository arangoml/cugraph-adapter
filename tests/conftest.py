import io
import json
import os
import subprocess
import urllib.request as urllib
import zipfile
from pathlib import Path
from typing import Any

from arango import ArangoClient
from arango.database import StandardDatabase

from adbcug_adapter.adapter import ADBCUG_Adapter
from adbcug_adapter.typings import Json, NxData, NxId

PROJECT_DIR = Path(__file__).parent.parent

con: Json
adbcug_adapter: ADBCUG_Adapter
db: StandardDatabase


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--protocol", action="store", default="http")
    parser.addoption("--host", action="store", default="localhost")
    parser.addoption("--port", action="store", default="8529")
    parser.addoption("--dbName", action="store", default="_system")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="openSesame")


def pytest_configure(config) -> None:
    global con
    con = {
        "protocol": config.getoption("protocol"),
        "hostname": config.getoption("host"),
        "port": config.getoption("port"),
        "username": config.getoption("username"),
        "password": config.getoption("password"),
        "dbName": config.getoption("dbName"),
    }

    print("----------------------------------------")
    print(f"{con['protocol']}://{con['hostname']}:{con['port']}")
    print("Username: " + con["username"])
    print("Password: " + con["password"])
    print("Database: " + con["dbName"])
    print("----------------------------------------")

    global adbcug_adapter
    adbcug_adapter = ADBCUG_Adapter(con)

    global db
    url = con["protocol"] + "://" + con["hostname"] + ":" + str(con["port"])
    client = ArangoClient(hosts=url)
    db = client.db(con["dbName"], con["username"], con["password"], verify=True)

    arango_restore(con, "examples/data/fraud_dump")
    arango_restore(con, "examples/data/imdb_dump")

    # Create Fraud Detection Graph
    db.delete_graph("fraud-detection", ignore_missing=True)
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


def arango_restore(con: Json, path_to_data: str) -> None:
    restore_prefix = "./assets/" if os.getenv("GITHUB_ACTIONS") else ""

    subprocess.check_call(
        f'chmod -R 755 ./assets/arangorestore && {restore_prefix}arangorestore \
            -c none --server.endpoint tcp://{con["hostname"]}:{con["port"]} \
                --server.username {con["username"]} --server.database {con["dbName"]} \
                    --server.password {con["password"]} \
                        --input-directory "{PROJECT_DIR}/{path_to_data}"',
        cwd=f"{PROJECT_DIR}/tests",
        shell=True,
    )
