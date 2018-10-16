# -*- coding: utf-8 -*-

from motorengine.base.aggregation import BaseAggregation


class SumAggregation(BaseAggregation):
    def to_query(self, aggregation):
        alias = self.alias
        field_name = aggregation.get_field_name(self.field)

        if alias is None:
            alias = field_name

        return {
            alias: {'$sum': ('${}'.format(field_name))}
        }
