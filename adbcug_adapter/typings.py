__all__ = ["Json", "ArangoMetagraph", "CuGId"]

from typing import Any, Dict, Set, Tuple, Union

Json = Dict[str, Any]
ArangoMetagraph = Dict[str, Dict[str, Set[str]]]

CuGId = Union[int, float, bool, str, Tuple[Any, ...]]
