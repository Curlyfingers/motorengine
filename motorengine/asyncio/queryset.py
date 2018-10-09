# -*- coding: utf-8 -*-

from easydict import EasyDict
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError


from motorengine.errors import UniqueKeyViolationError
from motorengine.errors import PartlyLoadedDocumentError
from motorengine.base.queryset import BaseQuerySet


class QuerySet(BaseQuerySet):
    def _get_connection_function(self):
        from motorengine.asyncio import get_connection
        return get_connection

    async def create(self, alias=None, **kwargs):
        document = self.__klass__(**kwargs)
        return await self.save(document=document, alias=alias)

    async def save(self, document, alias=None):
        if document.is_partly_loaded:
            msg = (
                'Partly loaded document {0} can\'t be saved. Document should '
                'be loaded without \'only\', \'exclude\' or \'fields\' '
                'QuerySet\'s modifiers'
            )
            raise PartlyLoadedDocumentError(
                msg.format(document.__class__.__name__)
            )

        self.update_field_on_save_values(document, document._id is not None)
        if self.validate_document(document):
            await self.ensure_index(alias=alias)
            return await self.save_document(document, alias=alias)

    async def save_document(self, document, alias=None):
        doc = document.to_son()

        if document._id is not None:
            try:
                await self.coll(alias).update({'_id': document._id}, doc)
            except DuplicateKeyError as e:
                raise UniqueKeyViolationError.from_pymongo(
                    str(e), self.__klass__
                )
        else:
            try:
                doc_id = await self.coll(alias).insert(doc)
            except DuplicateKeyError as e:
                raise UniqueKeyViolationError.from_pymongo(
                    str(e), self.__klass__
                )
            document._id = doc_id
        return document

    async def bulk_insert(self, documents, alias=None):
        is_valid = True
        docs_to_insert = []

        for document_index, document in enumerate(documents):
            self.update_field_on_save_values(
                document, document._id is not None
            )
            try:
                is_valid = is_valid and self.validate_document(document)
            except Exception as e:
                raise ValueError(
                    'Validation for document {} in the documents '
                    'you are saving failed with: {}'.format(
                        document_index,
                        e
                    )
                )

            if not is_valid:
                return

            docs_to_insert.append(document.to_son())

        if not is_valid:
            return

        doc_ids = await self.coll(alias).insert(docs_to_insert)

        for object_index, object_id in enumerate(doc_ids):
            documents[object_index]._id = object_id
        return documents

    async def update(self, definition, alias=None):

        definition = self.transform_definition(definition)

        update_filters = {}
        if self._filters:
            update_filters = self.get_query_from_filters(self._filters)

        update_arguments = dict(
            spec=update_filters,
            document={'$set': definition},
            multi=True,
        )
        res = await self.coll(alias).update(**update_arguments)

        return EasyDict({
            'count': int(res['n']),
            'updated_existing': res['updatedExisting']
        })

    async def delete(self, alias=None):
        return self.remove(alias=alias)

    async def remove(self, instance=None, alias=None):
        if instance is not None:
            if hasattr(instance, '_id') and instance._id:
                return await self.coll(alias).remove(instance._id)['n']
        else:
            if self._filters:
                remove_filters = self.get_query_from_filters(self._filters)
                return await self.coll(alias).remove(remove_filters)['n']
            else:
                return await self.coll(alias).remove()['n']

    async def get(self, _id=None, alias=None, **kwargs):
        from motorengine.asyncio import Q

        if _id is None and not kwargs:
            raise RuntimeError('Either an id or a filter must be provided to get')

        if _id is not None:
            if not isinstance(_id, ObjectId):
                _id = ObjectId(_id)

            filters = {
                '_id': _id
            }
        else:
            filters = Q(**kwargs)
            filters = self.get_query_from_filters(filters)

        instance = await self.coll(alias).find_one(
            filters, projection=self._loaded_fields.to_query(self.__klass__)
        )
        if instance is None:
            return
        else:
            doc = self.__klass__.from_son(
                instance,
                _is_partly_loaded=bool(self._loaded_fields),
                _reference_loaded_fields=self._reference_loaded_fields
            )
            if self.is_lazy:
                return doc
            else:
                await doc.load_references()
                return doc

    async def find_all(self, lazy=None, alias=None):
        to_list_arguments = {}
        if self._limit is not None:
            to_list_arguments['length'] = self._limit
        else:
            to_list_arguments['length'] = self.DEFAULT_LIMIT

        cursor = self._get_find_cursor(alias=alias)

        self._filters = {}

        docs = await cursor.to_list(**to_list_arguments)

        is_partly_loaded = bool(self._loaded_fields)

        result = []
        for doc in docs:
            obj = self.__klass__.from_son(
                doc,
                _reference_loaded_fields=self._reference_loaded_fields,
                _is_partly_loaded=is_partly_loaded
            )

            if (lazy is not None and not lazy) or not obj.is_lazy:
                await obj.load_references(obj._fields)

            result.append(obj)

        return result

    async def count(self, alias=None):
        cursor = self._get_find_cursor(alias=alias)
        self._filters = {}
        return await cursor.count()

    @property
    def aggregate(self):
        from motorengine.asyncio.aggregation import Aggregation
        return Aggregation(self)

    async def ensure_index(self, alias=None):
        fields_with_index = []
        for field_name, field in self.__klass__._fields.items():
            if field.unique or field.sparse:
                fields_with_index.append(field)

        created_indexes = []

        for field in fields_with_index:
            res = await self.coll(alias).ensure_index(
                field.db_field,
                unique=field.unique,
                sparse=field.sparse
            )
            created_indexes.append(res)

        return len(created_indexes)
