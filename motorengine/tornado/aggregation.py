# -*- coding: utf-8 -*-

from easydict import EasyDict

from motorengine.base.aggregation import TopLevelAggregation


class Aggregation(TopLevelAggregation):
    def handle_aggregation(self, callback):
        def handle(*arguments, **kw):
            if arguments[1]:
                raise RuntimeError('Aggregation failed due to: {}'.format(str(arguments[1])))

            results = []
            for item in arguments[0]:
                self.fill_ids(item)
                results.append(EasyDict(item))
            callback(results)

        return handle

    async def fetch(self, callback=None, alias=None):
        coll = self.queryset.coll(alias)
        await coll.aggregate(self.to_query()).to_list(None, callback=self.handle_aggregation(callback))
