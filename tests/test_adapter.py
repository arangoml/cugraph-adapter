from typing import Any, Dict, List, Optional, Set

import pytest
from arango.graph import Graph as ADBGraph
from cugraph import Graph as CUGGraph
from cugraph import MultiGraph as CUGMultiGraph

from adbcug_adapter import ADBCUG_Adapter, ADBCUG_Controller
from adbcug_adapter.typings import ADBMetagraph, CUGId, Json

from .conftest import (
    adbcug_adapter,
    bipartite_adbcug_adapter,
    db,
    get_bipartite_graph,
    get_divisibility_graph,
    get_drivers_graph,
    get_likes_graph,
)


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
    "adapter, name, edge_definitions, orphan_collections",
    [(adbcug_adapter, "fraud-detection", None, None)],
)
def test_adb_graph_to_cug(
    adapter: ADBCUG_Adapter,
    name: str,
    edge_definitions: List[Json],
    orphan_collections: List[str],
) -> None:
    # Re-create the graph if defintions are provided
    if edge_definitions:
        db.delete_graph(name, ignore_missing=True)
        db.create_graph(name, edge_definitions, orphan_collections)

    arango_graph = db.graph(name)
    v_cols = arango_graph.vertex_collections()
    e_cols = {col["edge_collection"] for col in arango_graph.edge_definitions()}

    cug_g = adapter.arangodb_graph_to_cugraph(name, batch_size=10)
    assert_cugraph_data(
        cug_g,
        metagraph={
            "vertexCollections": {col: set() for col in v_cols},
            "edgeCollections": {col: set() for col in e_cols},
        },
    )


@pytest.mark.parametrize(
    "adapter, name, cug_g, edge_definitions, orphan_collections, \
        overwrite_graph, batch_size, edge_attr, adb_import_kwargs",
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
            None,
            False,
            50,
            "quotient",
            {"on_duplicate": "replace"},
        ),
        (
            adbcug_adapter,
            "DivisibilityGraph",
            get_divisibility_graph(),
            None,
            None,
            False,
            None,
            "quotient",
            {"overwrite": True},
        ),
        (
            bipartite_adbcug_adapter,
            "SampleBipartiteGraph",
            get_bipartite_graph(),
            [
                {
                    "edge_collection": "to",
                    "from_vertex_collections": ["col_a"],
                    "to_vertex_collections": ["col_b"],
                }
            ],
            None,
            True,
            1,
            None,
            {"overwrite": True},
        ),
    ],
)
def test_cug_to_adb(
    adapter: ADBCUG_Adapter,
    name: str,
    cug_g: CUGGraph,
    edge_definitions: Optional[List[Json]],
    orphan_collections: Optional[List[str]],
    overwrite_graph: bool,
    batch_size: int,
    edge_attr: Optional[str],
    adb_import_kwargs: Dict[str, Any],
) -> None:
    adb_g = adapter.cugraph_to_arangodb(
        name,
        cug_g,
        edge_definitions,
        orphan_collections,
        overwrite_graph,
        batch_size=batch_size,
        edge_attr=edge_attr,
        **adb_import_kwargs,
    )
    assert_arangodb_data(
        adapter,
        cug_g,
        adb_g,
        edge_attr,
    )


def test_cug_to_adb_invalid_collections() -> None:
    db.delete_graph("Drivers", ignore_missing=True, drop_collections=True)

    cug_g_1 = get_drivers_graph()
    e_d_1 = [
        {
            "edge_collection": "drives",
            "from_vertex_collections": ["Person"],
            "to_vertex_collections": ["Car"],
        }
    ]
    # Raise NotImplementedError on missing vertex collection identification
    with pytest.raises(NotImplementedError):
        adbcug_adapter.cugraph_to_arangodb("Drivers", cug_g_1, e_d_1)

    class Custom_ADBCUG_Controller(ADBCUG_Controller):
        def _identify_cugraph_node(
            self, cug_node_id: CUGId, adb_v_cols: List[str]
        ) -> str:
            return "invalid_vertex_collection"

    custom_adbcug_adapter = ADBCUG_Adapter(db, Custom_ADBCUG_Controller())

    # Raise ValueError on invalid vertex collection identification
    with pytest.raises(ValueError):
        custom_adbcug_adapter.cugraph_to_arangodb("Drivers", cug_g_1, e_d_1)

    db.delete_graph("Drivers", ignore_missing=True, drop_collections=True)
    db.delete_graph("Feelings", ignore_missing=True, drop_collections=True)

    cug_g_2 = get_likes_graph()
    e_d_2 = [
        {
            "edge_collection": "likes",
            "from_vertex_collections": ["Person"],
            "to_vertex_collections": ["Person"],
        },
        {
            "edge_collection": "dislikes",
            "from_vertex_collections": ["Person"],
            "to_vertex_collections": ["Person"],
        },
    ]

    # Raise NotImplementedError on missing edge collection identification
    with pytest.raises(NotImplementedError):
        adbcug_adapter.cugraph_to_arangodb("Feelings", cug_g_2, e_d_2)

    db.delete_graph("Feelings", ignore_missing=True, drop_collections=True)

    class Custom_ADBCUG_Controller(ADBCUG_Controller):
        def _identify_cugraph_node(
            self, cug_node_id: CUGId, adb_v_cols: List[str]
        ) -> str:
            return str(cug_node_id).split("/")[0]

        def _identify_cugraph_edge(
            self,
            from_node_id: CUGId,
            to_node_id: CUGId,
            cug_map: Dict[CUGId, str],
            adb_e_cols: List[str],
        ) -> str:
            return "invalid_edge_collection"

    custom_adbcug_adapter = ADBCUG_Adapter(db, Custom_ADBCUG_Controller())

    # Raise ValueError on invalid edge collection identification
    with pytest.raises(ValueError):
        custom_adbcug_adapter.cugraph_to_arangodb("Feelings", cug_g_2, e_d_2)

    db.delete_graph("Feelings", ignore_missing=True, drop_collections=True)


def assert_arangodb_data(
    adapter: ADBCUG_Adapter,
    cug_g: CUGGraph,
    adb_g: ADBGraph,
    edge_attr: Optional[str],
) -> None:
    cug_map = dict()

    adb_v_cols = adb_g.vertex_collections()
    adb_e_cols = [e_d["edge_collection"] for e_d in adb_g.edge_definitions()]

    has_one_vcol = len(adb_v_cols) == 1
    has_one_ecol = len(adb_e_cols) == 1

    for i, cug_id in enumerate(cug_g.nodes().values_host, 1):
        col = (
            adb_v_cols[0]
            if has_one_vcol
            else adapter.cntrl._identify_cugraph_node(cug_id, adb_v_cols)
        )
        key = adapter.cntrl._keyify_cugraph_node(i, cug_id, col)

        adb_v_id = f"{col}/{key}"
        cug_map[cug_id] = adb_v_id

        assert adb_g.vertex_collection(col).has(key)

    cug_edges = cug_g.view_edge_list()
    cug_weights = (
        cug_edges[edge_attr] if cug_g.is_weighted() and edge_attr is not None else None
    )

    for i in range(len(cug_edges)):
        from_node_id = cug_edges["src"][i]
        to_node_id = cug_edges["dst"][i]

        col = (
            adb_e_cols[0]
            if has_one_ecol
            else adapter.cntrl._identify_cugraph_edge(
                from_node_id, to_node_id, cug_map, adb_e_cols
            )
        )

        adb_edges = adb_g.edge_collection(col).find(
            {
                "_from": cug_map[from_node_id],
                "_to": cug_map[to_node_id],
            }
        )

        assert len(adb_edges) == 1
        if edge_attr:
            assert adb_edges.pop()[edge_attr] == cug_weights[i]


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
