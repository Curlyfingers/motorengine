# -*- coding: utf-8 -*-

from motorengine.tornado.queryset import QuerySet
from motorengine.base.metaclasses import DocumentMetaClass as _DocumentMetaClass


class DocumentMetaClass(_DocumentMetaClass):
    query_set_class = QuerySet
