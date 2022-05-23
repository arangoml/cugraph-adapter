from typing import Any, Dict, List, Set

import pytest
from arango.graph import Graph as ADBGraph
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from adbcug_adapter import ADBCUG_Adapter
from adbcug_adapter.typings import ADBMetagraph, Json

from .conftest import adbcug_adapter, db, get_divisibility_graph


def test_validate_attributes() -> None:
    with pytest.raises(ValueError):
        bad_metagraph: Dict[str, Any] = dict()
        adbcug_adapter.arangodb_to_cugraph("graph_name", bad_metagraph)


def test_validate_constructor() -> None:
    bad_db: Dict[str, Any] = dict()

    class Bad_ADBCUG_Controller:
        pass

    with pytest.raises(TypeError):
        ADBCUG_Adapter(bad_db)

    with pytest.raises(TypeError):
        ADBCUG_Adapter(db, Bad_ADBCUG_Controller())  # type: ignore


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
def test_adb_to_cug(
    adapter: ADBCUG_Adapter, name: str, metagraph: ADBMetagraph
) -> None:
    cug_g = adapter.arangodb_to_cugraph(name, metagraph)
    assert_cugraph_data(cug_g, metagraph)


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
def test_adb_collections_to_cug(
    adapter: ADBCUG_Adapter, name: str, v_cols: Set[str], e_cols: Set[str]
) -> None:
    cug_g = adapter.arangodb_collections_to_cugraph(
        name,
        v_cols,
        e_cols,
    )
    assert_cugraph_data(
        cug_g,
        metagraph={
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        },
    )


@pytest.mark.parametrize(
    "adapter, name, edge_definitions",
    [(adbcug_adapter, "fraud-detection", None)],
)
def test_adb_graph_to_cug(
    adapter: ADBCUG_Adapter, name: str, edge_definitions: List[Json]
) -> None:
    # Re-create the graph if defintions are provided
    if edge_definitions:
        db.delete_graph(name, ignore_missing=True)
        db.create_graph(name, edge_definitions=edge_definitions)

    arango_graph = db.graph(name)
    v_cols = arango_graph.vertex_collections()
    e_cols = {col["edge_collection"] for col in arango_graph.edge_definitions()}

    cug_g = adapter.arangodb_graph_to_cugraph(name)
    assert_cugraph_data(
        cug_g,
        metagraph={
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        },
    )


@pytest.mark.parametrize(
    "adapter, name, cug_g, edge_definitions, batch_size, keyify_nodes",
    [
        (
            adbcug_adapter,
            "DivisibilityGraph",
            get_divisibility_graph(),
            [
                {
                    "edge_collection": "is_divisible_by",
                    "from_vertex_collections": ["numbers"],
                    "to_vertex_collections": ["numbers"],
                }
            ],
            100,
            True,
        )
    ],
)
def test_cug_to_adb(
    adapter: ADBCUG_Adapter,
    name: str,
    cug_g: CUGGraph,
    edge_definitions: List[Json],
    batch_size: int,
    keyify_nodes: bool,
) -> None:
    adb_g = adapter.cugraph_to_arangodb(
        name, cug_g, edge_definitions, batch_size, keyify_nodes
    )
    assert_arangodb_data(adapter, cug_g, adb_g, keyify_nodes)


def assert_arangodb_data(
    adapter: ADBCUG_Adapter,
    cug_g: CUGGraph,
    adb_g: ADBGraph,
    keyify_nodes: bool,
) -> None:
    cug_map = dict()
    adb_v_cols = set()
    adb_e_cols = set()

    edge_definitions = adb_g.edge_definitions()
    for e_d in edge_definitions:
        adb_e_cols.add(e_d["edge_collection"])

        from_collections = set(e_d["from_vertex_collections"])
        to_collections = set(e_d["to_vertex_collections"])
        for v_col in from_collections | to_collections:
            adb_v_cols.add(v_col)

    is_homogeneous = len(adb_v_cols | adb_e_cols) == 2
    homogenous_v_col = adb_v_cols.pop() if is_homogeneous else None
    homogenous_e_col = adb_e_cols.pop() if is_homogeneous else None

    for i, cug_id in enumerate(cug_g.nodes().values_host):
        col = homogenous_v_col or adapter.cntrl._identify_cugraph_node(
            cug_id, adb_v_cols
        )
        key = (
            adapter.cntrl._keyify_cugraph_node(cug_id, col) if keyify_nodes else str(i)
        )

        adb_v_id = col + "/" + key
        cug_map[cug_id] = {
            "cug_id": cug_id,
            "adb_id": adb_v_id,
            "adb_col": col,
            "adb_key": key,
        }

        assert adb_g.vertex_collection(col).has(key)

    for from_node_id, to_node_id, *weight in cug_g.view_edge_list().values_host:
        from_n = cug_map[from_node_id]
        to_n = cug_map[to_node_id]

        col = homogenous_e_col or adapter.cntrl._identify_cugraph_edge(
            from_n, to_n, adb_e_cols
        )
        adb_edges = adb_g.edge_collection(col).find(
            {
                "_from": from_n["adb_id"],
                "_to": to_n["adb_id"],
            }
        )

        assert len(adb_edges) > 0


def assert_cugraph_data(cug_g: CUGMultiGraph, metagraph: ADBMetagraph) -> None:

    adb_edge: Json
    df = cug_g.to_pandas_edgelist()
    for col, atribs in metagraph["edgeCollections"].items():
        for adb_edge in db.collection(col):
            # check if edge is present in edgelist
            index_list = df[
                (df["src"] == adb_edge["_from"]) & (df["dst"] == adb_edge["_to"])
            ].index.tolist()
            assert len(index_list) != 0
            # remove edge from edgelist
            df = df.drop(index=index_list[0])
