try:
    from pymongo import ASCENDING, DESCENDING  # NOQA

    from motorengine.asyncio.connection import connect
    from motorengine.asyncio.connection import disconnect
    from motorengine.asyncio.connection import get_connection
    from motorengine.asyncio.document import Document  # NOQA

    from motorengine.fields import (  # NOQA
        BaseField, StringField, BooleanField, DateTimeField,
        UUIDField, ListField, EmbeddedDocumentField, ReferenceField, URLField,
        EmailField, IntField, FloatField, DecimalField, BinaryField,
        JsonField, ObjectIdField, DictField
    )

    from motorengine.asyncio.aggregation.base import Aggregation  # NOQA
    from motorengine.query_builder.node import Q, QNot  # NOQA

except ImportError:  # NOQA
    pass  # likely setup.py trying to import version
