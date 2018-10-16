# -*- coding: utf-8 -*-

from abc import ABCMeta

from motorengine.fields import BaseField
from motorengine.base.document import BaseDocument
from motorengine.errors import InvalidDocumentError


class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class DocumentMetaClass(ABCMeta):
    query_set_class = None

    def __new__(cls, name, bases, attrs):
        flattened_bases = cls._get_bases(bases)
        super_new = super(DocumentMetaClass, cls).__new__

        doc_fields = {}
        for base in flattened_bases[::-1]:
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)

        # Discover any document fields
        field_names = {}

        for field_name, doc_field in doc_fields.items():
            field_names[doc_field.db_field] = field_names.get(
                doc_field.db_field, 0) + 1

        for attr_name, attr_value in attrs.items():
            if not isinstance(attr_value, BaseField):
                continue
            if attr_value.__class__.__name__ == 'DynamicField':
                continue
            attr_value.name = attr_name
            if not attr_value.db_field:
                attr_value.db_field = attr_name
            doc_fields[attr_name] = attr_value

            # Count names to ensure no db_field redefinitions
            field_names[attr_value.db_field] = field_names.get(
                attr_value.db_field, 0) + 1

        # Set _fields and db_field maps
        attrs['_fields'] = doc_fields
        attrs['_db_field_map'] = {
            k: getattr(v, 'db_field', k)
            for k, v in doc_fields.items()
        }
        attrs['_fields_ordered'] = tuple(
            i[1] for i in sorted(
                (v.creation_counter, v.name)
                for v in doc_fields.values()
            )
        )
        attrs['_reverse_db_field_map'] = {
            v: k
            for k, v in attrs['_db_field_map'].items()
        }

        new_class = super_new(cls, name, bases, attrs)
        new_class.__hierarchy__ = None
        new_class.__child_classes__ = []

        if '__lazy__' not in attrs:
            new_class.__lazy__ = True

        if '__alias__' not in attrs:
            new_class.__alias__ = None

        if '__inherit__' not in attrs:
            new_class.__inherit__ = False

        if '__abstract__' not in attrs:
            new_class.__abstract__ = False

        if cls._is_base(flattened_bases):
            new_class.__abstract__ = True

        if '__collection__' not in attrs:
            new_class.__collection__ = cls._get_collection(new_class, flattened_bases)

        if new_class.__abstract__:
            new_class.__inherit__ = False
            new_class.__hierarchy__ = None
            new_class.__collection__ = None
            new_class.__child_classes__ = []

            duplicate_db_fields = [k for k, v in field_names.items() if v > 1]
            if duplicate_db_fields:
                raise InvalidDocumentError('Multiple db_fields defined for: {}'.format(', '.join(duplicate_db_fields)))

        if new_class.__inherit__:
            from motorengine.fields import StringField
            from motorengine.base import classes_registry

            cls._update_hierarchy(new_class, flattened_bases)

            if classes_registry.get(new_class.__hierarchy__):
                raise Exception('Model \'{}\' is already exists'.format(new_class.__hierarchy__))

            # TODO: refactor this part
            classes_registry[new_class.__hierarchy__] = new_class
            _cls_field = StringField()
            _cls_field.name = _cls_field.db_field = '_cls'
            _cls_field.default = new_class.__hierarchy__
            new_class._fields['_cls'] = _cls_field

        setattr(new_class, 'objects', classproperty(lambda *args, **kw: cls.query_set_class(new_class)))

        return new_class

    @staticmethod
    def _get_collection(new_class, flattened_bases):
        _flattened_bases = flattened_bases[::-1][2:]
        if not _flattened_bases:
            return new_class.__name__
        if not new_class.__abstract__ and not new_class.__inherit__:
            return new_class.__name__
        return _flattened_bases[0].__name__

    @staticmethod
    def _update_hierarchy(new_class, flattened_bases):
        _flattened_bases = flattened_bases[::-1][2:]
        if _flattened_bases:
            for base in _flattened_bases:
                base.__child_classes__.append(new_class)
        new_class.__hierarchy__ = '.'.join([cls.__name__ for cls in _flattened_bases] + [new_class.__name__])

    @staticmethod
    def _is_base(flattened_bases):
        if not len(flattened_bases) == 1:
            return False
        if not flattened_bases[0] is BaseDocument:
            return False
        return True

    @classmethod
    def _get_bases(cls, bases):
        if isinstance(bases, BasesTuple):
            return bases
        seen = []
        bases = cls.__get_bases(bases)
        unique_bases = (b for b in bases if not (b in seen or seen.append(b)))
        return BasesTuple(unique_bases)

    @classmethod
    def __get_bases(cls, bases):
        for base in bases:
            if base is object:
                continue
            yield base
            for child_base in cls.__get_bases(base.__bases__):
                yield child_base


class BasesTuple(tuple):
    """Special class to handle introspection of bases tuple in __new__"""
    pass
