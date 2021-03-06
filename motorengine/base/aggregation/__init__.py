# -*- coding: utf-8 -*-
from abc import ABCMeta
from abc import abstractmethod

from six import string_types
from six import with_metaclass

from motorengine import ASCENDING
from motorengine.query_builder.transform import update


class BaseAggregation(with_metaclass(ABCMeta)):
    def __init__(self, field, alias):
        self._field = field
        self.alias = alias

    @property
    def field(self):
        return self._field

    @abstractmethod
    def to_query(self, *args, **kwargs):
        pass


class PipelineOperation(object):
    def __init__(self, aggregation):
        self.aggregation = aggregation

    def to_query(self):
        return {}


class GroupBy(PipelineOperation):
    def __init__(self, aggregation, first_group_by, *groups):
        super(GroupBy, self).__init__(aggregation)
        self.first_group_by = first_group_by
        self.groups = groups

    def to_query(self):
        group_obj = {'$group': {'_id': {}}}

        for group in self.groups:
            if isinstance(group, BaseAggregation):
                group_obj['$group'].update(group.to_query(self.aggregation))
                continue

            if isinstance(group, string_types):
                field_name = group
            else:
                field_name = self.aggregation.get_field(group).db_field

            if self.first_group_by:
                group_obj['$group']['_id'][field_name] = "$%s" % field_name
            else:
                group_obj['$group']['_id'][field_name] = "$_id.%s" % field_name

        return group_obj


class Match(PipelineOperation):
    def __init__(self, aggregation, **filters):
        super(Match, self).__init__(aggregation)
        self.filters = filters

    def to_query(self):
        from motorengine import Q
        match_obj = {'$match': {}}

        query = self.aggregation.queryset.get_query_from_filters(Q(**self.filters))

        update(match_obj['$match'], query)

        return match_obj


class Unwind(PipelineOperation):
    def __init__(self, aggregation, field):
        super(Unwind, self).__init__(aggregation)
        self.field = self.aggregation.get_field(field)

    def to_query(self):
        return {'$unwind': '$%s' % self.field.db_field}


class OrderBy(PipelineOperation):
    def __init__(self, aggregation, field, direction):
        super(OrderBy, self).__init__(aggregation)
        self.field = self.aggregation.get_field(field)
        self.direction = direction

    def to_query(self):
        return {'$sort': {self.field.db_field: self.direction}}


class TopLevelAggregation(object):
    def __init__(self, queryset):
        self.first_group_by = True
        self.queryset = queryset
        self.pipeline = []
        self.ids = []
        self.raw_query = None

    @abstractmethod
    def fetch(self, *args, **kwargs):
        return

    @staticmethod
    def get_field_name(field):
        if isinstance(field, string_types):
            return field
        return field.db_field

    @staticmethod
    def get_field(field):
        return field

    def raw(self, steps):
        self.raw_query = steps
        return self

    def group_by(self, *args):
        self.pipeline.append(GroupBy(self, self.first_group_by, *args))
        self.first_group_by = False
        return self

    def match(self, **kw):
        self.pipeline.append(Match(self, **kw))
        return self

    def unwind(self, field):
        self.pipeline.append(Unwind(self, field))
        return self

    def order_by(self, field, direction=ASCENDING):
        self.pipeline.append(OrderBy(self, field, direction))
        return self

    @staticmethod
    def fill_ids(item):
        if '_id' not in item:
            return

        if isinstance(item['_id'], (dict,)):
            for id_name, id_value in list(item['_id'].items()):
                item[id_name] = id_value

    def get_instance(self, item):
        return self.queryset.__klass__.from_son(item)

    @classmethod
    def avg(cls, field, alias=None):
        from motorengine.base.aggregation.avg import AverageAggregation
        return AverageAggregation(field, alias)

    @classmethod
    def sum(cls, field, alias=None):
        from motorengine.base.aggregation.sum import SumAggregation
        return SumAggregation(field, alias)

    def to_query(self):
        if self.raw_query is not None:
            return self.raw_query

        query = []

        for pipeline_step in self.pipeline:
            query_steps = pipeline_step.to_query()
            if isinstance(query_steps, (tuple, set, list)):
                for step in query_steps:
                    query.append(step)
            else:
                query.append(query_steps)

        return query
