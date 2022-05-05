from typing import List, Set

import pytest
from arango.graph import Graph as ArangoGraph
from cugraph import MultiGraph as cuGraphMultiGraph

from adbcug_adapter.adapter import ADBCUG_Adapter
from adbcug_adapter.typings import ArangoMetagraph, CuGId, Json

from .conftest import adbcug_adapter, db


def test_validate_attributes() -> None:
    bad_connection = {
        "dbName": "_system",
        "hostname": "localhost",
        "protocol": "http",
        "port": 8529,
        # "username": "root",
        # "password": "password",
    }

    with pytest.raises(ValueError):
        ADBCUG_Adapter(bad_connection)
