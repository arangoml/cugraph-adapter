from typing import Any, Dict, List, Set

import pytest
from cugraph import MultiGraph as cuGraphMultiGraph

from adbcug_adapter.adapter import ADBCUG_Adapter
from adbcug_adapter.typings import ArangoMetagraph, Json

from .conftest import adbcug_adapter, db


def test_validate_attributes() -> None:
    with pytest.raises(ValueError):
        bad_metagraph: Dict[str, Any] = dict()
        adbcug_adapter.arangodb_to_cugraph("graph_name", bad_metagraph)


def test_validate_constructor() -> None:
    bad_db: Dict[str, Any] = dict()

    with pytest.raises(TypeError):
        ADBCUG_Adapter(bad_db)  # type: ignore


@pytest.mark.parametrize(
    "adapter, name, metagraph",
    [
        (
            adbcug_adapter,
            "fraud-detection",
            {
                "vertexCollections": {
                    "account": {"Balance", "account_type", "customer_id", "rank"},
                    "bank": {"Country", "Id", "bank_id", "bank_name"},
                    "branch": {
                        "City",
                        "Country",
                        "Id",
                        "bank_id",
                        "branch_id",
                        "branch_name",
                    },
                    "Class": {"concrete", "label", "name"},
                    "customer": {"Name", "Sex", "Ssn", "rank"},
                },
                "edgeCollections": {
                    "accountHolder": {},
                    "Relationship": {
                        "label",
                        "name",
                        "relationshipType",
                    },
                    "transaction": {
                        "transaction_amt",
                        "sender_bank_id",
                        "receiver_bank_id",
                    },
                },
            },
        ),
        (
            adbcug_adapter,
            "IMDBGraph",
            {
                "vertexCollections": {"Users": {"Age", "Gender"}, "Movies": {}},
                "edgeCollections": {"Ratings": {"Rating"}},
            },
        ),
    ],
)
def test_adb_to_cg(
    adapter: ADBCUG_Adapter, name: str, metagraph: ArangoMetagraph
) -> None:
    cg_g = adapter.arangodb_to_cugraph(name, metagraph)
    assert_cugraph_data(cg_g, metagraph, True)


@pytest.mark.parametrize(
    "adapter, name, v_cols, e_cols",
    [
        (
            adbcug_adapter,
            "fraud-detection",
            {"account", "bank", "branch", "Class", "customer"},
            {"accountHolder", "Relationship", "transaction"},
        )
    ],
)
def test_adb_collections_to_cg(
    adapter: ADBCUG_Adapter, name: str, v_cols: Set[str], e_cols: Set[str]
) -> None:
    cg_g = adapter.arangodb_collections_to_cugraph(
        name,
        v_cols,
        e_cols,
    )
    assert_cugraph_data(
        cg_g,
        metagraph={
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        },
    )


@pytest.mark.parametrize(
    "adapter, name, edge_definitions",
    [(adbcug_adapter, "fraud-detection", None)],
)
def test_adb_graph_to_cg(
    adapter: ADBCUG_Adapter, name: str, edge_definitions: List[Json]
) -> None:
    # Re-create the graph if defintions are provided
    if edge_definitions:
        db.delete_graph(name, ignore_missing=True)
        db.create_graph(name, edge_definitions=edge_definitions)

    arango_graph = db.graph(name)
    v_cols = arango_graph.vertex_collections()
    e_cols = {col["edge_collection"] for col in arango_graph.edge_definitions()}

    cg_g = adapter.arangodb_graph_to_cugraph(name)
    assert_cugraph_data(
        cg_g,
        metagraph={
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        },
    )


def assert_cugraph_data(
    cg_g: cuGraphMultiGraph, metagraph: ArangoMetagraph, is_keep: bool = False
) -> None:

    adb_edge: Json
    df = cg_g.to_pandas_edgelist()
    for col, atribs in metagraph["edgeCollections"].items():
        for adb_edge in db.collection(col):
            # check if edge is present in edgelist
            index_list = df[
                (df["src"] == adb_edge["_from"]) & (df["dst"] == adb_edge["_to"])
            ].index.tolist()
            assert len(index_list) != 0
            # remove edge from edgelist
            df = df.drop(index=index_list[0])
