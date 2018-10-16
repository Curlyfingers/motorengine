from motorengine.base.document import BaseDocument
from motorengine.tornado.metaclasses import DocumentMetaClass


class Document(BaseDocument, metaclass=DocumentMetaClass):
    @classmethod
    async def ensure_index(cls, callback=None):
        await cls.objects.ensure_index(callback=callback)

    async def save(self, callback, alias=None, upsert=False):
        await self.objects.save(self, callback=callback, alias=alias, upsert=upsert)

    async def delete(self, callback, alias=None):
        await self.objects.remove(instance=self, callback=callback, alias=alias)

    def handle_load_reference(self, callback, references, reference_count, values_collection, field_name, fill_values_method=None):
        if fill_values_method is None:
            fill_values_method = self.fill_values_collection

        def handle(*args, **kw):
            fill_values_method(values_collection, field_name, args[0])

            if reference_count > 0:
                references.pop()

            if len(references) == 0:
                callback({
                    'loaded_reference_count': reference_count,
                    'loaded_values': values_collection
                })

        return handle

    async def load_references(self, fields=None, callback=None, alias=None):
        if callback is None:
            raise ValueError("Callback can't be None")

        references = self.find_references(document=self, fields=fields)
        reference_count = len(references)

        if not reference_count:
            callback({
                'loaded_reference_count': reference_count,
                'loaded_values': []
            })
            return

        for dereference_function, document_id, values_collection, field_name, fill_values_method in references:
            await dereference_function(
                document_id,
                callback=self.handle_load_reference(
                    callback=callback,
                    references=references,
                    reference_count=reference_count,
                    values_collection=values_collection,
                    field_name=field_name,
                    fill_values_method=fill_values_method
                )
            )
