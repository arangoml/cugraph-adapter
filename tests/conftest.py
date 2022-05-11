import json
import os
import subprocess
import time
from pathlib import Path

from arango import ArangoClient
from arango.database import StandardDatabase
from requests import post

from adbcug_adapter.adapter import ADBCUG_Adapter
from adbcug_adapter.typings import Json

PROJECT_DIR = Path(__file__).parent.parent

con: Json
adbcug_adapter: ADBCUG_Adapter
db: StandardDatabase


def pytest_sessionstart() -> None:
    global con
    con = get_oasis_crendetials()
    # con = {
    #     "username": "root",
    #     "password": "openSesame",
    #     "hostname": "localhost",
    #     "port": 8529,
    #     "protocol": "http",
    #     "dbName": "_system",
    # }
    print_connection_details(con)
    time.sleep(5)  # Enough for the oasis instance to be ready.

    global adbcug_adapter
    adbcug_adapter = ADBCUG_Adapter(con)

    global db
    url = "https://" + con["hostname"] + ":" + str(con["port"])
    client = ArangoClient(hosts=url)
    db = client.db(con["dbName"], con["username"], con["password"], verify=True)

    arango_restore(con, "examples/data/fraud_dump")
    arango_restore(con, "examples/data/imdb_dump")

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


def get_oasis_crendetials() -> Json:
    url = "https://tutorials.arangodb.cloud:8529/_db/_system/tutorialDB/tutorialDB"
    request = post(url, data=json.dumps("{}"))
    if request.status_code != 200:
        raise Exception("Error retrieving login data.")

    creds: Json = json.loads(request.text)
    return creds


def print_connection_details(con: Json) -> None:
    print("----------------------------------------")
    print("https://{}:{}".format(con["hostname"], con["port"]))
    print("Username: " + con["username"])
    print("Password: " + con["password"])
    print("Database: " + con["dbName"])
    print("----------------------------------------")


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
