import json

import sqlalchemy as sa

from prefect.blocks.storage import S3StorageBlock
from prefect.orion import models
from prefect.orion.schemas.data import DataDocument
from prefect.blocks.system import JSON


async def get_block_content(session: sa.orm.Session, content: any, parse_doc: bool = False) -> str:
    from prefect.packaging.orion import OrionPackageManifest
    if isinstance(content, OrionPackageManifest):
        block_document = await models.block_documents.read_block_document_by_id(
            session=session,
            block_document_id=content.block_document_id)
        block = JSON._from_block_document(block_document)
        serialized_flow: str = json.loads(block.value["flow"])
        return serialized_flow['source']
    else:
        block_document = await models.block_documents.read_block_document_by_id(
            session=session,
            block_document_id=content['block_document_id'],
            include_secrets=True)
    storage_block = S3StorageBlock.parse_obj(block_document.data)
    flow_bytes = await storage_block.read(content['data'])
    flow_data = flow_bytes.decode('utf-8')
    if parse_doc:
        encode_data = json.loads(flow_data)
        datadoc = DataDocument(encoding=encode_data['encoding'], blob=encode_data['blob'])
        content = datadoc.decode()
        return content
    else:
        return flow_data
