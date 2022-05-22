__all__ = ["Json", "ArangoMetagraph", "CUGId"]

from typing import Any, Dict, Set, Tuple, Union

Json = Dict[str, Any]
ArangoMetagraph = Dict[str, Dict[str, Set[str]]]

CUGId = Union[int, float, bool, str, Tuple[Any, ...]]
