# ArangoDB-cuGraph Adapter
[![build](https://github.com/arangoml/cugraph-adapter/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/arangoml/cugraph-adapter/actions/workflows/build.yml)
[![CodeQL](https://github.com/arangoml/cugraph-adapter/actions/workflows/analyze.yml/badge.svg?branch=master)](https://github.com/arangoml/cugraph-adapter/actions/workflows/analyze.yml)
[![Coverage Status](https://coveralls.io/repos/github/arangoml/cugraph-adapter/badge.svg?branch=master)](https://coveralls.io/github/arangoml/cugraph-adapter)
[![Last commit](https://img.shields.io/github/last-commit/arangoml/cugraph-adapter)](https://github.com/arangoml/cugraph-adapter/commits/master)

[![Conda version badge](https://img.shields.io/conda/v/arangodb/adbcug-adapter?color=3775A9&style=for-the-badge&logo=pypi&logoColor=FFD43B)](https://anaconda.org/arangodb/adbcug-adapter)
![Python version badge](https://img.shields.io/static/v1?color=3776AB&style=for-the-badge&logo=python&logoColor=FFD43B&label=python&message=3.7%20%7C%203.8%20%7C%203.9)

[![License](https://img.shields.io/github/license/arangoml/cugraph-adapter?color=9E2165&style=for-the-badge)](https://github.com/arangoml/cugraph-adapter/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/static/v1?style=for-the-badge&label=code%20style&message=black&color=black)](https://github.com/psf/black)
<!-- [![Downloads](https://img.shields.io/conda/dn/arangodb/adbcug-adapter?style=for-the-badge&color=282661&label=Downloads)](https://anaconda.org/arangodb/adbcug-adapter/badges/downloads.svg
) -->

<a href="https://www.arangodb.com/" rel="arangodb.com">![](https://raw.githubusercontent.com/arangoml/cugraph-adapter/master/examples/assets/logos/ArangoDB_logo.png)</a>
<a href="https://github.com/rapidsai/cugraph" rel="github.com/rapidsai/cugraph"><img src="https://raw.githubusercontent.com/arangoml/cugraph-adapter/master/examples/assets/logos/rapids_logo.png" width=30% height=30%></a>

The ArangoDB-cuGraph Adapter exports Graphs from ArangoDB, the multi-model database for graph & beyond, into RAPIDS cuGraph, a library of collective GPU-accelerated graph algorithms, and vice-versa.

## About RAPIDS cuGraph

While offering a similar API and set of graph algorithms to NetworkX, RAPIDS cuGraph library is GPU-based. Especially for large graphs, this results in a significant performance improvement of cuGraph compared to NetworkX. Please note that storing node attributes is currently not supported by cuGraph. In order to run cuGraph, an Nvidia-CUDA-enabled GPU is required.

## Installation

<u>Prerequisites</u>: A CUDA-capable GPU

#### Latest Release
```
conda install -c arangodb adbcug-adapter
```

#### Current State
```
conda install -c rapidsai -c nvidia -c numba -c conda-forge cugraph>=21.12 cudatoolkit>=11.2
pip install git+https://github.com/arangoml/cugraph-adapter.git
```

##  Quickstart

<a href="https://colab.research.google.com/github/arangoml/cugraph-adapter/blob/master/examples/ArangoDB_cuGraph_Adapter.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

```py
import cudf
import cugraph
from arango import ArangoClient # Python-Arango driver

from adbcug_adapter import ADBCUG_Adapter

# Let's assume that the ArangoDB "fraud detection" dataset is imported to this endpoint
db = ArangoClient(hosts="http://localhost:8529").db("_system", username="root", password="")

adbcug_adapter = ADBCUG_Adapter(db)
```

### ArangoDB to cuGraph
```py
# 1.1: ArangoDB to cuGraph via Graph name
cug_g = adbcug_adapter.arangodb_graph_to_cugraph("fraud-detection")

# 1.2: ArangoDB to cuGraph via Collection names
cug_g = adbcug_adapter.arangodb_collections_to_cugraph(
    "fraud-detection",
    {"account", "bank", "branch", "Class", "customer"},  #  Vertex collections
    {"accountHolder", "Relationship", "transaction"},  # Edge collections
)
```

### cuGraph to ArangoDB
```py
# 2.1: cuGraph Homogeneous graph to ArangoDB
edges = [("Person/A", "Person/B"), ("Person/B", "Person/C")]
cug_g = cugraph.MultiGraph(directed=True)
cug_g.from_cudf_edgelist(cudf.DataFrame(edges, columns=["src", "dst"]), source="src", destination="dst", renumber=False)

edge_definitions = [
    {
        "edge_collection": "knows",
        "from_vertex_collections": ["Person"],
        "to_vertex_collections": ["Person"],
    }
]

adb_g = adbcug_adapter.cugraph_to_arangodb("Knows", cug_g, edge_definitions) # Also try it with `keyify_nodes=True` !

# 2.2: cuGraph Heterogeneous graph to ArangoDB with ArangoDB node IDs
edges = []
for i in range(1, 101):
    for j in range(1, 101):
        if j % i == 0:
            # Notice that the cuGraph node IDs are following ArangoDB _id formatting standards (i.e `collection_name/node_key`)
            edges.append((f"numbers_j/{j}", f"numbers_i/{i}", j / i)) 

cug_g = cugraph.MultiGraph(directed=True)
cug_g.from_cudf_edgelist(cudf.DataFrame(edges, columns=["src", "dst", "quotient"]), source="src", destination="dst", edge_attr="quotient", renumber=False)

edge_definitions = [
    {
        "edge_collection": "is_divisible_by",
        "from_vertex_collections": ["numbers_j"],
        "to_vertex_collections": ["numbers_i"],
    }
]

adb_g = adbcug_adapter.cugraph_to_arangodb("Divisibility", cug_g, edge_definitions, keyify_nodes=True)

# 2.3 cuGraph Heterogeneous graph to ArangoDB with non-ArangoDB node IDs
edges = [
   ('student:101', 'lecture:101'), 
   ('student:102', 'lecture:102'), 
   ('student:103', 'lecture:103'), 
   ('student:103', 'student:101'), 
   ('student:103', 'student:102'),
   ('teacher:101', 'lecture:101'),
   ('teacher:102', 'lecture:102'),
   ('teacher:103', 'lecture:103'),
   ('teacher:101', 'teacher:102'),
   ('teacher:102', 'teacher:103')
]
cug_g = cugraph.MultiGraph(directed=True)
cug_g.from_cudf_edgelist(cudf.DataFrame(edges, columns=["src", "dst"]), source='src', destination='dst')

### Learn how this example is handled in Colab: https://colab.research.google.com/github/arangoml/cugraph-adapter/blob/master/examples/ArangoDB_cuGraph_Adapter.ipynb#scrollTo=nuVoCZQv6oyi
```

##  Development & Testing

Prerequisite: `arangorestore`, `CUDA-capable GPU`

1. `git clone https://github.com/arangoml/cugraph-adapter.git`
2. `cd cugraph-adapter`
3. (create virtual environment of choice)
4. `conda install -c rapidsai -c nvidia -c numba -c conda-forge cugraph>=21.12 cudatoolkit>=11.2`
5. `conda run pip install -e .[dev]`
6. (create an ArangoDB instance with method of choice)
7. `pytest --url <> --dbName <> --username <> --password <>`

**Note**: A `pytest` parameter can be omitted if the endpoint is using its default value:
```python
def pytest_addoption(parser):
    parser.addoption("--url", action="store", default="http://localhost:8529")
    parser.addoption("--dbName", action="store", default="_system")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="")
```
