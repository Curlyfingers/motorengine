# -*- coding: utf-8 -*-

from motorengine.base.aggregation import BaseAggregation


class AverageAggregation(BaseAggregation):
    def to_query(self, aggregation):
        alias_name = self.alias
        field_name = aggregation.get_field_name(self.field)

        if not alias_name:
            alias_name = field_name

        return {
            alias_name: {'$avg"': ('${}'.format(field_name))}
        }
