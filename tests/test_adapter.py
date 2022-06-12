from typing import Any, Dict, List, Set

import pytest
from arango.graph import Graph as ADBGraph
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from adbcug_adapter import ADBCUG_Adapter
from adbcug_adapter.typings import ADBMetagraph, Json

from .conftest import (
    adbcug_adapter,
    custom_adbcug_adapter,
    db,
    get_bipartite_graph,
    get_divisibility_graph,
)


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
    "adapter, name, cug_g, edge_definitions, \
        batch_size, keyify_nodes, keyify_edges, edge_attr, overwrite",
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
            False,
            "quotient",
            False,
        ),
        (
            adbcug_adapter,
            "DivisibilityGraph",
            get_divisibility_graph(),
            None,
            100,
            False,
            False,
            "quotient",
            True,
        ),
        (
            custom_adbcug_adapter,
            "SampleBipartiteGraph",
            get_bipartite_graph(),
            [
                {
                    "edge_collection": "to",
                    "from_vertex_collections": ["col_a"],
                    "to_vertex_collections": ["col_b"],
                }
            ],
            1,
            True,
            False,
            "",
            False,
        ),
    ],
)
def test_cug_to_adb(
    adapter: ADBCUG_Adapter,
    name: str,
    cug_g: CUGGraph,
    edge_definitions: List[Json],
    batch_size: int,
    keyify_nodes: bool,
    keyify_edges: bool,
    edge_attr: str,
    overwrite: bool,
) -> None:
    adb_g = adapter.cugraph_to_arangodb(
        name,
        cug_g,
        edge_definitions,
        batch_size,
        keyify_nodes,
        keyify_edges,
        edge_attr,
        overwrite,
    )
    assert_arangodb_data(adapter, cug_g, adb_g, keyify_nodes)


def assert_arangodb_data(
    adapter: ADBCUG_Adapter,
    cug_g: CUGGraph,
    adb_g: ADBGraph,
    keyify_nodes: bool,
) -> None:
    cug_map = dict()

    edge_definitions = adb_g.edge_definitions()
    adb_v_cols = adb_g.vertex_collections()
    adb_e_cols = [e_d["edge_collection"] for e_d in edge_definitions]

    has_one_vcol = len(adb_v_cols) == 1
    has_one_ecol = len(adb_e_cols) == 1

    for i, cug_id in enumerate(cug_g.nodes().values_host, 1):
        col = (
            adb_v_cols[0]
            if has_one_vcol
            else adapter.cntrl._identify_cugraph_node(cug_id, adb_v_cols)
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

        col = (
            adb_e_cols[0]
            if has_one_ecol
            else adapter.cntrl._identify_cugraph_edge(from_n, to_n, adb_e_cols)
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
