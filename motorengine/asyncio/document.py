from motorengine.base.document import BaseDocument
from motorengine.asyncio.metaclasses import DocumentMetaClass


class Document(BaseDocument, metaclass=DocumentMetaClass):
    @classmethod
    async def ensure_index(cls):
        return await cls.objects.ensure_index()

    async def save(self, alias=None):
        return await self.objects.save(self, alias=alias)

    async def delete(self, alias=None):
        return await self.objects.remove(instance=self, alias=alias)

    async def load_references(self, fields=None, alias=None):
        references = self.find_references(document=self, fields=fields)
        reference_count = len(references)

        if not reference_count:
            return {
                'loaded_reference_count': reference_count,
                'loaded_values': []
            }

        for (
            dereference_function, document_id, values_collection,
            field_name, fill_values_method
        ) in references:
            doc = await dereference_function(document_id)
            if fill_values_method is None:
                fill_values_method = self.fill_values_collection

            fill_values_method(values_collection, field_name, doc)

        return {
            'loaded_reference_count': reference_count,
            'loaded_values': values_collection  # FIXME: wtf?
        }
