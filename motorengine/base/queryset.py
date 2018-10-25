# -*- coding: utf-8 -*-

from datetime import datetime

from six import with_metaclass

from abc import ABCMeta
from abc import abstractmethod

from motorengine import ASCENDING
from motorengine.query_builder.field_list import QueryFieldList
from motorengine.query_builder.transform import validate_fields
from motorengine.query_builder.node import Q, QCombination, QNot


class BaseQuerySet(with_metaclass(ABCMeta)):
    DEFAULT_LIMIT = 1000

    def __init__(self, klass):
        if klass.__abstract__ is True:
            raise Exception('Abstract model \'{}\' could not be used for retrieving data'.format(klass.__name__))
        self.__klass__ = klass
        self._filters = None
        self._limit = None
        self._skip = None
        self._order_fields = []
        self._loaded_fields = QueryFieldList()
        self._reference_loaded_fields = {}

        if klass.__inherit__ is True:
            child_classes = [klass.__hierarchy__,]
            child_classes.extend([
                child_class.__hierarchy__
                for child_class in klass.__child_classes__
            ])
            self._filters = Q({'_cls': {'$in': child_classes}})

    @property
    def is_lazy(self):
        return self.__klass__.__lazy__

    @abstractmethod
    def _get_connection_function(self):
        pass

    def _resolve_class(self, doc):
        from motorengine.base import classes_registry
        klass = doc.pop('_cls', None)
        if not klass:
            return self.__klass__
        return classes_registry.get(klass)


    def coll(self, alias=None):
        get_connection = self._get_connection_function()
        if alias is not None:
            conn = get_connection(alias=alias)
        elif self.__klass__.__alias__ is not None:
            conn = get_connection(alias=self.__klass__.__alias__)
        else:
            conn = get_connection()

        return conn[self.__klass__.__collection__]

    @abstractmethod
    async def create(self, *args, **kwargs):
        pass

    @abstractmethod
    async def save(self, *args, **kwargs):
        pass

    @abstractmethod
    async def update(self, *args, **kwargs):
        pass

    @abstractmethod
    async def delete(self, *args, **kwargs):
        pass

    @abstractmethod
    async def remove(self, *args, **kwargs):
        pass

    @abstractmethod
    async def bulk_insert(self, *args, **kwargs):
        pass

    @abstractmethod
    async def get(self, *args, **kwargs):
        pass

    @abstractmethod
    async def find_all(self, *args, **kwargs):
        pass

    @abstractmethod
    async def count(self, *args, **kwargs):
        pass

    @abstractmethod
    async def ensure_index(self, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def aggregate(self):
        pass

    def update_field_on_save_values(self, document, updating):
        from motorengine.fields.datetime_field import DateTimeField
        from motorengine.fields.embedded_document_field import (
            EmbeddedDocumentField
        )

        for field_name, field in document.__class__._fields.items():
            if isinstance(field, DateTimeField):
                if field.auto_now_on_insert and not updating:
                    setattr(document, field_name, datetime.now())
                elif field.auto_now_on_update:
                    setattr(document, field_name, datetime.now())

            if field.on_save is not None:
                setattr(document, field_name, field.on_save(document, updating))

            if isinstance(field, EmbeddedDocumentField):
                doc = getattr(document, field_name)
                if doc:
                    self.update_field_on_save_values(doc, updating)

    def validate_document(self, document):
        if not isinstance(document, self.__klass__):
            raise ValueError('This queryset for class \'{}\' can\'t save an instance of type \'{}\'.'.format(
                self.__klass__.__name__,
                document.__class__.__name__,
            ))

        return document.validate()

    @staticmethod
    def transform_definition(definition):
        from motorengine.fields.base_field import BaseField
        return {
            key.db_field if isinstance(key, BaseField) else key: value
            for key, value in definition.items()

        }

    def _check_valid_field_name_to_project(self, field_name, value):
        if '.' not in field_name and (
            field_name == '_id' or field_name in self.__klass__._fields
        ):
            return field_name, value

        from motorengine.fields.embedded_document_field import (
            EmbeddedDocumentField
        )
        from motorengine.fields.list_field import ListField
        from motorengine.fields.reference_field import ReferenceField

        tail = field_name
        head = []
        obj = self.__klass__
        while tail:
            parts = tail.split('.', 1)
            if len(parts) == 2:
                field_value, tail = parts
            else:
                field_value, tail = parts[0], None
            head.append(field_value)

            if not obj or field_value not in obj._fields:
                raise ValueError(
                    'Invalid field \'{}\': Field not found in \'{}\'.'.format(
                        field_name, self.__klass__.__name__
                    )
                )
            else:
                field = obj._fields[field_value]
                if isinstance(field, EmbeddedDocumentField):
                    obj = field.embedded_type
                elif isinstance(field, ListField):
                    if hasattr(field._base_field, 'embedded_type'):
                        obj = field.item_type
                    elif hasattr(field._base_field, 'reference_type'):
                        return self._fill_reference_loaded_fields(
                            head, tail, field_name, value
                        )
                    else:
                        obj = None
                elif isinstance(field, ReferenceField):
                    return self._fill_reference_loaded_fields(
                        head, tail, field_name, value
                    )
                else:
                    obj = None

        return field_name, value

    def _fill_reference_loaded_fields(self, head, tail, field_name, value):
        name = '.'.join(head)
        if tail:
            if name not in self._reference_loaded_fields:
                self._reference_loaded_fields[name] = {}
            self._reference_loaded_fields[name][tail] = value
            return name, QueryFieldList.ONLY
        else:
            return field_name, value

    def only(self, *fields):
        from motorengine.fields.base_field import BaseField

        only_fields = {}
        for field_name in fields:
            if isinstance(field_name, (BaseField, )):
                field_name = field_name.name

            only_fields[field_name] = QueryFieldList.ONLY

        return self.fields(True, **only_fields)

    def exclude(self, *fields):
        from motorengine.fields.base_field import BaseField

        exclude_fields = {}
        for field_name in fields:
            if isinstance(field_name, (BaseField, )):
                field_name = field_name.name

            exclude_fields[field_name] = QueryFieldList.EXCLUDE

        return self.fields(**exclude_fields)

    def fields(self, _only_called=False, **kwargs):
        from itertools import groupby
        from operator import itemgetter

        operators = ['slice']
        cleaned_fields = []
        for key, value in kwargs.items():
            parts = key.split('__')
            if parts[0] in operators:
                op = parts.pop(0)
                value = {'$' + op: value}

            key = '.'.join(parts)
            try:
                field_name, value = self._check_valid_field_name_to_project(
                    key, value
                )
            except ValueError as e:
                raise e

            cleaned_fields.append((field_name, value))

        fields = sorted(cleaned_fields, key=itemgetter(1))
        for value, group in groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            self._loaded_fields += QueryFieldList(
                fields, value=value, _only_called=_only_called)

        return self

    def all_fields(self):
        self._loaded_fields = QueryFieldList(
            always_include=self._loaded_fields.always_include)

        return self

    def get_query_from_filters(self, filters):
        if not filters:
            return {}

        query = filters.to_query(self.__klass__)
        return query

    def _get_find_cursor(self, alias):
        find_arguments = {}

        if self._order_fields:
            find_arguments['sort'] = self._order_fields

        if self._limit:
            find_arguments['limit'] = self._limit

        if self._skip:
            find_arguments['skip'] = self._skip

        query_filters = self.get_query_from_filters(self._filters)

        return self.coll(alias).find(
            query_filters, projection=self._loaded_fields.to_query(self.__klass__),
            **find_arguments
        )

    def filter(self, *arguments, **kwargs):
        if arguments and len(arguments) == 1 and isinstance(arguments[0], (Q, QNot, QCombination)):
            if self._filters:
                self._filters = self._filters & arguments[0]
            else:
                self._filters = arguments[0]
        else:
            validate_fields(self.__klass__, kwargs)
            if self._filters:
                self._filters = self._filters & Q(**kwargs)
            else:
                if arguments and len(arguments) == 1 and isinstance(arguments[0], dict):
                    self._filters = Q(arguments[0])
                else:
                    self._filters = Q(**kwargs)

        return self

    def filter_not(self, *arguments, **kwargs):
        from motorengine.query_builder.node import Q, QCombination, QNot

        if arguments and len(arguments) == 1 and isinstance(arguments[0], (Q, QCombination)):
            self.filter(QNot(arguments[0]))
        else:
            self.filter(QNot(Q(**kwargs)))

        return self

    def skip(self, skip):
        self._skip = skip
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    def order_by(self, field_name, direction=ASCENDING):
        from motorengine.fields.base_field import BaseField
        from motorengine.fields.list_field import ListField

        if isinstance(field_name, (ListField, )):
            raise ValueError(
                'Can\'t order by a list field. If you meant to order by the size of the list, '
                'please use either an Aggregation Pipeline query (look for Document.objects.aggregate) '
                'or create an IntField with the size of the list field in your Document.'
            )

        if isinstance(field_name, (BaseField, )):
            field_name = field_name.name

        if field_name not in self.__klass__._fields:
            raise ValueError('Invalid order by field \'{}\': Field not found in \'{}\'.'.format(
                field_name, self.__klass__.__name__)
            )

        field = self.__klass__._fields[field_name]
        self._order_fields.append((field.db_field, direction))
        return self
