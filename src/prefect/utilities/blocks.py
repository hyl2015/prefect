import json

import sqlalchemy as sa

from prefect.blocks.core import Block
from prefect.orion import models


async def get_flow_content(
    session: sa.orm.Session, entrypoint: str, block_document_id: str
) -> str:
    storage_document = await models.block_documents.read_block_document_by_id(
        session=session, block_document_id=block_document_id, include_secrets=True
    )
    storage_block = Block._from_block_document(storage_document)
    flow_storage_path = entrypoint.split(":")[0]
    flow_data_bytes = await storage_block.read_path(flow_storage_path)
    flow_data = flow_data_bytes.decode("utf-8")
    return flow_data
