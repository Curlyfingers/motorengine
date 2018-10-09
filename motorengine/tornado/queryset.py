# -*- coding: utf-8 -*-

from easydict import EasyDict
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError


from motorengine.errors import UniqueKeyViolationError
from motorengine.errors import PartlyLoadedDocumentError
from motorengine.base.queryset import BaseQuerySet


class QuerySet(BaseQuerySet):
    def _get_connection_function(self):
        from motorengine.tornado.connection import get_connection
        return get_connection

    async def create(self, callback, alias=None, **kwargs):
        document = self.__klass__(**kwargs)
        await self.save(document=document, callback=callback, alias=alias)

    def handle_save(self, document, callback):
        def handle(*arguments, **kw):
            if len(arguments) > 1 and arguments[1]:
                if isinstance(arguments[1], (DuplicateKeyError, )):
                    raise UniqueKeyViolationError.from_pymongo(str(arguments[1]), self.__klass__)
                else:
                    raise arguments[1]

            document._id = arguments[0]
            callback(document)

        return handle

    def handle_update(self, document, callback):
        def handle(*arguments, **kw):
            if len(arguments) > 1 and arguments[1]:
                if isinstance(arguments[1], (DuplicateKeyError, )):
                    raise UniqueKeyViolationError.from_pymongo(str(arguments[1]), self.__klass__)
                else:
                    raise arguments[1]

            callback(document)

        return handle

    def save(self, document, callback, alias=None, upsert=False):
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
            self.ensure_index(
                callback=self.indexes_saved_before_save(document, callback, alias=alias, upsert=upsert), 
                alias=alias
            )

    def indexes_saved_before_save(self, document, callback, alias=None, upsert=False):
        def handle(*args, **kw):
            doc = document.to_son()

            if document._id is not None:
                self.coll(alias).update(
                    {'_id': document._id}, 
                    doc, 
                    callback=self.handle_update(document, callback),
                    upsert=upsert,
                )
            else:
                self.coll(alias).insert(doc, callback=self.handle_save(document, callback))

        return handle

    @staticmethod
    def handle_bulk_insert(documents, callback):
        def handle(*arguments, **kw):
            if len(arguments) > 1 and arguments[1]:
                raise arguments[1]

            for object_index, object_id in enumerate(arguments[0]):
                documents[object_index]._id = object_id
            callback(documents)

        return handle

    async def bulk_insert(self, documents, callback=None, alias=None):
        is_valid = True
        docs_to_insert = []

        for document_index, document in enumerate(documents):
            self.update_field_on_save_values(
                document, document._id is not None
            )
            try:
                is_valid = is_valid and self.validate_document(document)
            except Exception as e:
                raise ValueError('Validation for document {} in the documents you are saving failed with: {}'.format(
                    document_index,
                    e
                ))

            if not is_valid:
                return

            docs_to_insert.append(document.to_son())

        if not is_valid:
            return

        await self.coll(alias).insert(docs_to_insert, callback=self.handle_bulk_insert(documents, callback))

    @staticmethod
    def handle_update_documents(callback):
        def handle(*arguments, **kwargs):
            if len(arguments) > 1 and arguments[1]:
                raise arguments[1]

            callback(EasyDict({
                'count': int(arguments[0]['n']),
                'updated_existing': arguments[0]['updatedExisting']
            }))

        return handle

    async def update(self, definition, callback=None, alias=None):
        if callback is None:
            raise RuntimeError("The callback argument is required")

        definition = self.transform_definition(definition)

        update_filters = {}
        if self._filters:
            update_filters = self.get_query_from_filters(self._filters)

        update_arguments = dict(
            spec=update_filters,
            document={'$set': definition},
            multi=True,
            callback=self.handle_update_documents(callback)
        )
        await self.coll(alias).update(**update_arguments)

    async def delete(self, callback=None, alias=None):
        await self.remove(callback=callback, alias=alias)

    @staticmethod
    def handle_remove(callback):
        def handle(*args, **kw):
            callback(args[0]['n'])

        return handle

    async def remove(self, instance=None, callback=None, alias=None):
        if callback is None:
            raise RuntimeError('The callback argument is required')

        if instance is not None:
            if hasattr(instance, '_id') and instance._id:
                await self.coll(alias).remove(instance._id, callback=self.handle_remove(callback))
        else:
            if self._filters:
                remove_filters = self.get_query_from_filters(self._filters)
                await self.coll(alias).remove(remove_filters, callback=self.handle_remove(callback))
            else:
                await self.coll(alias).remove(callback=self.handle_remove(callback))

    @staticmethod
    def handle_auto_load_references(doc, callback):
        def handle(*args, **kw):
            if len(args) > 0:
                callback(doc)
                return

            callback(None)

        return handle

    def handle_get(self, callback):
        def handle(*args, **kw):
            instance = args[0]

            if instance is None:
                callback(None)
            else:
                doc = self.__klass__.from_son(
                    instance,
                    _is_partly_loaded=bool(self._loaded_fields),
                    _reference_loaded_fields=self._reference_loaded_fields
                )

                if self.is_lazy:
                    callback(doc)
                else:
                    doc.load_references(callback=self.handle_auto_load_references(doc, callback))

        return handle

    async def get(self, _id=None, callback=None, alias=None, **kwargs):
        from motorengine import Q

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

        await self.coll(alias).find_one(
            filters, projection=self._loaded_fields.to_query(self.__klass__),
            callback=self.handle_get(callback)
        )

    def handle_find_all_auto_load_references(self, callback, results):
        def handle(*arguments, **kwargs):
            self.current_count += 1
            if self.current_count == self.result_size:
                self.current_count = None
                self.result_size = None
                callback(results)

        return handle

    def handle_find_all(self, callback, lazy=None):
        def handle(*arguments, **kwargs):
            if arguments and len(arguments) > 1 and arguments[1]:
                raise arguments[1]

            result = []
            self.current_count = 0
            self.result_size = len(arguments[0])

            # if _loaded_fields is not empty then documents are partly loaded
            is_partly_loaded = bool(self._loaded_fields)

            for doc in arguments[0]:
                obj = self.__klass__.from_son(
                    doc,
                    # set projections for references (if any)
                    _reference_loaded_fields=self._reference_loaded_fields,
                    _is_partly_loaded=is_partly_loaded
                )

                result.append(obj)

            if not result:
                callback(result)
                return

            for doc in result:
                if (lazy is not None and not lazy) or not doc.is_lazy:
                    doc.load_references(
                        doc._fields,
                        callback=self.handle_find_all_auto_load_references(callback, result)
                    )
                else:
                    self.handle_find_all_auto_load_references(callback, result)()

        return handle

    async def find_all(self, callback, lazy=None, alias=None):
        to_list_arguments = dict(callback=self.handle_find_all(callback, lazy=lazy))

        if self._limit is not None:
            to_list_arguments['length'] = self._limit
        else:
            to_list_arguments['length'] = self.DEFAULT_LIMIT

        cursor = self._get_find_cursor(alias=alias)

        await cursor.to_list(**to_list_arguments)

    @staticmethod
    def handle_count(callback):
        def handle(*arguments, **kwargs):
            if arguments and len(arguments) > 1 and arguments[1]:
                raise arguments[1]
            callback(arguments[0])

        return handle

    async def count(self, callback, alias=None):
        cursor = self._get_find_cursor(alias=alias)
        await cursor.count(callback=self.handle_count(callback))

    @property
    def aggregate(self):
        from motorengine.tornado.aggregation import Aggregation
        return Aggregation(self)

    @staticmethod
    def handle_ensure_index(callback, created_indexes, total_indexes):
        def handle(*arguments, **kw):
            if len(arguments) > 1 and arguments[1]:
                raise arguments[1]

            created_indexes.append(arguments[0])

            if len(created_indexes) < total_indexes:
                return

            callback(total_indexes)

        return handle

    async def ensure_index(self, callback, alias=None):
        fields_with_index = []
        for field_name, field in self.__klass__._fields.items():
            if field.unique or field.sparse:
                fields_with_index.append(field)

        created_indexes = []

        for field in fields_with_index:
            await self.coll(alias).ensure_index(
                field.db_field,
                unique=field.unique,
                sparse=field.sparse,
                callback=self.handle_ensure_index(
                    callback,
                    created_indexes,
                    len(fields_with_index)
                ),
            )

        if not fields_with_index:
            callback(0)
