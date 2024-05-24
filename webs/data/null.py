from __future__ import annotations

from typing import Any
from typing import NoReturn
from typing import TypeVar

from webs.data.config import register
from webs.data.config import TransformerConfig

T = TypeVar('T')


@register(name='null')
class NullTransformerConfig(TransformerConfig):
    """Null transformer config."""

    def get_transformer(self) -> NullTransformer:
        """Create a transformer instance from the config."""
        return NullTransformer()


class NullTransformer:
    """Null transformer that does no transformations."""

    def is_identifier(self, obj: Any) -> bool:
        """Check if the object is an identifier instance.

        Always `False` in this implementation.
        """
        return False

    def transform(self, obj: T) -> T:
        """Transform the object into an identifier.

        Args:
            obj: Object to transform.

        Returns:
            Identifier object that can be usd to resolve `obj`.
        """
        return obj

    def resolve(self, identifier: Any) -> NoReturn:
        """Resolve an object from an identifier.

        Args:
            identifier: Identifier to an object.

        Returns:
            The resolved object.
        """
        raise NotImplementedError(
            f'{self.__class__.__name__} does not support identifiers',
        )
