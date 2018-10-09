from easydict import EasyDict

from motorengine.base.aggregation import TopLevelAggregation


class Aggregation(TopLevelAggregation):
    async def fetch(self, alias=None):
        coll = self.queryset.coll(alias)
        results = []
        try:
            lst = await coll.aggregate(self.to_query(), cursor=False)
            for item in lst:
                self.fill_ids(item)
                results.append(EasyDict(item))
        except Exception as e:
            raise RuntimeError('Aggregation failed due to: {}'.format(str(e)))
        return results
