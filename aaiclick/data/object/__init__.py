"""
aaiclick.data.object - Core Object class and associated operations subpackage.
"""

from .object import GroupByQuery, LazyOperator, Object, View
from .transforms import cast, literal, split_by_char
from .url import create_object_from_url
