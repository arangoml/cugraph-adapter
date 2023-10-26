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
pip install --extra-index-url=https://pypi.nvidia.com cudf-cu11 cugraph-cu11
pip install adbcug-adapter
```

#### Current State
```
pip install --extra-index-url=https://pypi.nvidia.com cudf-cu11 cugraph-cu11
pip install git+https://github.com/arangoml/cugraph-adapter.git
```

##  Quickstart

<a href="https://colab.research.google.com/github/arangoml/cugraph-adapter/blob/master/examples/ArangoDB_cuGraph_Adapter.ipynb" target="_parent"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>

```py
import cudf
import cugraph

from arango import ArangoClient
from adbcug_adapter import ADBCUG_Adapter, ADBCUG_Controller

# Connect to ArangoDB
db = ArangoClient().db()

# Instantiate the adapter
adbcug_adapter = ADBCUG_Adapter(db)
```

### ArangoDB to cuGraph
```py
#######################
# 1.1: via Graph name #
#######################

cug_g = adbcug_adapter.arangodb_graph_to_cugraph("fraud-detection")

#############################
# 1.2: via Collection names #
#############################

cug_g = adbcug_adapter.arangodb_collections_to_cugraph(
    "fraud-detection",
    {"account", "bank", "branch", "Class", "customer"},  #  Vertex collections
    {"accountHolder", "Relationship", "transaction"},  # Edge collections
)
```

### cuGraph to ArangoDB
```py

#################################
# 2.1: with a Homogeneous Graph #
#################################

edges = [("Person/A", "Person/B", 1), ("Person/B", "Person/C", -1)]
cug_g = cugraph.MultiGraph(directed=True)
cug_g.from_cudf_edgelist(cudf.DataFrame(edges, columns=["src", "dst", "weight"]), source="src", destination="dst", edge_attr="weight")

edge_definitions = [
    {
        "edge_collection": "knows",
        "from_vertex_collections": ["Person"],
        "to_vertex_collections": ["Person"],
    }
]

adb_g = adbcug_adapter.cugraph_to_arangodb("Knows", cug_g, edge_definitions, edge_attr="weight")

##############################################################
# 2.2: with a Homogeneous Graph & a custom ADBCUG Controller #
##############################################################

class Custom_ADBCUG_Controller(ADBCUG_Controller):
    """ArangoDB-cuGraph controller.

    Responsible for controlling how nodes & edges are handled when
    transitioning from ArangoDB to cuGraph & vice-versa.
    """

    def _prepare_cugraph_node(self, cug_node: dict, col: str) -> None:
        """Prepare a cuGraph node before it gets inserted into the ArangoDB
        collection **col**.

        :param cug_node: The cuGraph node object to (optionally) modify.
        :param col: The ArangoDB collection the node belongs to.
        """
        cug_node["foo"] = "bar"

    def _prepare_cugraph_edge(self, cug_edge: dict, col: str) -> None:
        """Prepare a cuGraph edge before it gets inserted into the ArangoDB
        collection **col**.

        :param cug_edge: The cuGraph edge object to (optionally) modify.
        :param col: The ArangoDB collection the edge belongs to.
        """
        cug_edge["bar"] = "foo"

adb_g = ADBCUG_Adapter(db, Custom_ADBCUG_Controller()).cugraph_to_arangodb("Knows", cug_g, edge_definitions)

###################################
# 2.3: with a Heterogeneous Graph #
###################################

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

# ...

# Learn how this example is handled in Colab:
# https://colab.research.google.com/github/arangoml/cugraph-adapter/blob/master/examples/ArangoDB_cuGraph_Adapter.ipynb#scrollTo=nuVoCZQv6oyi
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
