# (WIP) ArangoDB-cuGraph Adapter
[![build](https://github.com/arangoml/cugraph-adapter/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/arangoml/cugraph-adapter/actions/workflows/build.yml)
[![CodeQL](https://github.com/arangoml/cugraph-adapter/actions/workflows/analyze.yml/badge.svg?branch=master)](https://github.com/arangoml/cugraph-adapter/actions/workflows/analyze.yml)
[![Coverage Status](https://coveralls.io/repos/github/arangoml/cugraph-adapter/badge.svg?branch=master)](https://coveralls.io/github/arangoml/cugraph-adapter)
[![Last commit](https://img.shields.io/github/last-commit/arangoml/cugraph-adapter)](https://github.com/arangoml/cugraph-adapter/commits/master)

[![PyPI version badge](https://img.shields.io/pypi/v/adbcug-adapter?color=3775A9&style=for-the-badge&logo=pypi&logoColor=FFD43B)](https://pypi.org/project/adbcug-adapter/)
[![Python versions badge](https://img.shields.io/pypi/pyversions/adbcug-adapter?color=3776AB&style=for-the-badge&logo=python&logoColor=FFD43B)](https://pypi.org/project/adbcug-adapter/)

[![License](https://img.shields.io/github/license/arangoml/cugraph-adapter?color=9E2165&style=for-the-badge)](https://github.com/arangoml/cugraph-adapter/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/static/v1?style=for-the-badge&label=code%20style&message=black&color=black)](https://github.com/psf/black)
[![Downloads](https://img.shields.io/badge/dynamic/json?style=for-the-badge&color=282661&label=Downloads&query=total_downloads&url=https://api.pepy.tech/api/projects/adbcug-adapter)](https://pepy.tech/project/adbcug-adapter)

<a href="https://www.arangodb.com/" rel="arangodb.com">![](./examples/assets/logos/ArangoDB_logo.png)</a>
<a href="https://github.com/rapidsai/cugraph" rel="github.com/rapidsai/cugraph"><img src="./examples/assets/logos/rapids_logo.png" width=30% height=30%></a>

The ArangoDB-cuGraph Adapter exports Graphs from ArangoDB, a multi-model Graph Database, into RAPIDS cuGraph, a library of collective GPU-accelerated graph algorithms (vice-versa exports planned).

## About RAPIDS cuGraph

While offering a similar API and set of graph algorithms to NetworkX, RAPIDS cuGraph library is GPU-based. Especially for large graphs, this results in a significant performance improvement of cuGraph compared to NetworkX. Please note that storing node attributes is currently not supported by cuGraph. In order to run cuGraph, an Nvidia-CUDA-enabled GPU is required.

## Installation

```
conda install -c arangodb adbcug_adapter
```

##  Quickstart

For a more detailed walk-through, access the official notebook on Colab: **TODO**

```py
# Import the ArangoDB-cuGraph Adapter
from adbcug_adapter.adapter import ADBCUG_Adapter

# Store ArangoDB endpoint connection info
# Assumption: the ArangoDB "fraud detection" dataset is imported to this endpoint for example purposes
con = {
    "protocol": "http",
    "hostname": "localhost",
    "port": 8529,
    "username": "root",
    "password": "openSesame",
    "dbName": "_system",
}

# Instantiate the ADBCUG Adapter with connection credentials
adbcug_adapter = ADBCUG_Adapter(con)

# Convert ArangoDB to cuGraph via Graph
cug_fraud_graph = adbcug_adapter.arangodb_graph_to_cugraph("fraud-detection")

# Convert ArangoDB to cuGraph via Collection Names
cug_fraud_graph_2 = adbcug_adapter.arangodb_collections_to_cugraph(
        "fraud-detection", 
        {"account", "bank", "branch", "Class", "customer"}, # Specify vertex collections
        {"accountHolder", "Relationship", "transaction"} # Specify edge collections
)
```

##  Development & Testing (TODO - Rework as a conda environment)

Prerequisite: `arangorestore`, `conda`, `NVidia CUDA-enabled GPU`

1. `git clone https://github.com/arangoml/cugraph-adapter.git`
2. `cd cugraph-adapter`
3. (create virtual environment of choice)
4. `pip install -e . pytest`
5. (create an ArangoDB instance with method of choice)
6. `pytest --protocol <> --host <> --port <> --dbName <> --username <> --password <>`

**Note**: A `pytest` parameter can be omitted if the endpoint is using its default value:
```python
def pytest_addoption(parser):
    parser.addoption("--protocol", action="store", default="http")
    parser.addoption("--host", action="store", default="localhost")
    parser.addoption("--port", action="store", default="8529")
    parser.addoption("--dbName", action="store", default="_system")
    parser.addoption("--username", action="store", default="root")
    parser.addoption("--password", action="store", default="openSesame")
```