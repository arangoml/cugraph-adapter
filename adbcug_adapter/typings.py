__all__ = ["Json", "ADBMetagraph", "CUGId"]

from typing import Any, Dict, Set, Tuple, Union

Json = Dict[str, Any]
ADBMetagraph = Dict[str, Dict[str, Set[str]]]

CUGId = Union[int, float, bool, str, Tuple[Any, ...]]
