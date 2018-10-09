try:
    from pymongo import ASCENDING, DESCENDING  # NOQA

    from motorengine.tornado.connection import connect
    from motorengine.tornado.connection import disconnect
    from motorengine.tornado.connection import get_connection
    from motorengine.tornado.document import Document  # NOQA

    from motorengine.fields import (  # NOQA
        BaseField, StringField, BooleanField, DateTimeField,
        UUIDField, ListField, EmbeddedDocumentField, ReferenceField, URLField,
        EmailField, IntField, FloatField, DecimalField, BinaryField,
        JsonField, ObjectIdField, DictField
    )

    from motorengine.tornado.aggregation.base import Aggregation  # NOQA
    from motorengine.query_builder.node import Q, QNot  # NOQA

except ImportError:  # NOQA
    pass  # likely setup.py trying to import version
