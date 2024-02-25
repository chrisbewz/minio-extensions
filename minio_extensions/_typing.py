from typing import (
    Literal,
    Optional,
    List,
    TypeVar,
    Type,
    cast,
    Union,
    Dict,
    Any,
    Tuple
)

from typing_extensions import Annotated
from annotated_types import Ge, Gt


Indexable = Optional[int]
PosInt = Annotated[int, Ge(0)]
IntStr = Union[PosInt, str]

# Object tagging dupplicate handling options
TagDuplicateOptions = Literal["raise", "first", "last", "append"]

# Object versioning handling options
VersionLike = Union[Literal["first","latest"], "IntStr"]



