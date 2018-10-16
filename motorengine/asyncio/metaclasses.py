from motorengine.asyncio.queryset import QuerySet
from motorengine.base.metaclasses import DocumentMetaClass as _DocumentMetaClass


class DocumentMetaClass(_DocumentMetaClass):
    query_set_class = QuerySet
