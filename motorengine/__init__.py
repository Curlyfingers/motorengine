#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = '1.1.3'

try:
    from pymongo import ASCENDING, DESCENDING  # NOQA

    from motorengine.fields import (  # NOQA
        BaseField, StringField, BooleanField, DateTimeField,
        UUIDField, ListField, EmbeddedDocumentField, ReferenceField, URLField,
        EmailField, IntField, FloatField, DecimalField, BinaryField,
        JsonField, ObjectIdField, DictField
    )

    from motorengine.query_builder.node import Q, QNot  # NOQA

except ImportError as e:  # NOQA
    # likely setup.py trying to import version
    import sys
    import traceback
    traceback.print_exception(*sys.exc_info())
