"""
Routes for interacting with block objects.
"""
from typing import List, Optional, Union
from uuid import UUID

from fastapi import Body, Depends, HTTPException, responses, Path, Query, status
from fastapi.encoders import jsonable_encoder

from prefect.blocks.core import Block
from prefect.orion import models, schemas
from prefect.orion.api import dependencies
from prefect.orion.database.dependencies import provide_database_interface
from prefect.orion.database.interface import OrionDBInterface
from prefect.orion.utilities.server import OrionRouter
from prefect.results import PersistedResult, PersistedResultBlob, LiteralResult
from prefect.utilities.blocks import get_flow_content

router = OrionRouter(prefix="/block_documents", tags=["Block documents"])


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_block_document(
        block_document: schemas.actions.BlockDocumentCreate,
        db: OrionDBInterface = Depends(provide_database_interface),
) -> schemas.core.BlockDocument:
    """
    Create a new block document.
    """
    async with db.session_context(begin_transaction=True) as session:
        if block_document.name is not None:
            exists = (
                await models.block_documents.block_document_with_unique_values_exists(
                    session=session,
                    block_type_id=block_document.block_type_id,
                    name=block_document.name,
                )
            )
            if exists:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    detail="Block already exists",
                )

        return await models.block_documents.create_block_document(
            session=session, block_document=block_document
        )


@router.post("/filter")
async def read_block_documents(
        limit: int = dependencies.LimitBody(),
        block_documents: Optional[schemas.filters.BlockDocumentFilter] = None,
        block_types: Optional[schemas.filters.BlockTypeFilter] = None,
        block_schemas: Optional[schemas.filters.BlockSchemaFilter] = None,
        include_secrets: bool = Body(
            False, description="Whether to include sensitive values in the block document."
        ),
        offset: int = Body(0, ge=0),
        db: OrionDBInterface = Depends(provide_database_interface),
) -> List[schemas.core.BlockDocument]:
    """
    Query for block documents.
    """
    async with db.session_context() as session:
        result = await models.block_documents.read_block_documents(
            session=session,
            block_document_filter=block_documents,
            block_type_filter=block_types,
            block_schema_filter=block_schemas,
            include_secrets=include_secrets,
            offset=offset,
            limit=limit,
        )

    return result


@router.post("/count")
async def count_block_documents(
        block_documents: Optional[schemas.filters.BlockDocumentFilter] = None,
        block_schemas: Optional[schemas.filters.BlockSchemaFilter] = None,
        db: OrionDBInterface = Depends(provide_database_interface),
) -> int:
    """
    Count deployments.
    """
    async with db.session_context() as session:
        return await models.block_documents.count_block_documents(
            session=session,
            block_document_filter=block_documents,
            block_schema_filter=block_schemas
        )


@router.get("/{id:uuid}")
async def read_block_document_by_id(
        block_document_id: UUID = Path(
            ..., description="The block document id", alias="id"
        ),
        include_secrets: bool = Query(
            False, description="Whether to include sensitive values in the block document."
        ),
        db: OrionDBInterface = Depends(provide_database_interface),
) -> schemas.core.BlockDocument:
    async with db.session_context() as session:
        block_document = await models.block_documents.read_block_document_by_id(
            session=session,
            block_document_id=block_document_id,
            include_secrets=include_secrets,
        )
    if not block_document:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Block document not found")
    return block_document


@router.delete("/{id:uuid}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_block_document(
        block_document_id: UUID = Path(
            ..., description="The block document id", alias="id"
        ),
        db: OrionDBInterface = Depends(provide_database_interface),
):
    async with db.session_context(begin_transaction=True) as session:
        result = await models.block_documents.delete_block_document(
            session=session, block_document_id=block_document_id
        )
    if not result:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail="Block document not found"
        )


@router.patch("/{id:uuid}", status_code=status.HTTP_204_NO_CONTENT)
async def update_block_document_data(
        block_document: schemas.actions.BlockDocumentUpdate,
        block_document_id: UUID = Path(
            ..., description="The block document id", alias="id"
        ),
        db: OrionDBInterface = Depends(provide_database_interface),
):
    try:
        async with db.session_context(begin_transaction=True) as session:
            result = await models.block_documents.update_block_document(
                session=session,
                block_document_id=block_document_id,
                block_document=block_document,
            )
    except ValueError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST)

    if not result:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail="Block document not found"
        )


@router.post("/decode_flow_data")
async def read_flow_data(
        block_document_id: str = Body(None),
        entrypoint: str = Body(None),
        db: OrionDBInterface = Depends(provide_database_interface),
):
    async with db.session_context(begin_transaction=True) as session:
        flow_data = await get_flow_content(session, entrypoint, block_document_id)
    return responses.JSONResponse(
        content={"content": flow_data},
    )


@router.post("/decode_blob")
async def read_s3_blob(
        blob_data: Union[PersistedResult, LiteralResult] = None,
        db: OrionDBInterface = Depends(provide_database_interface),
):
    if isinstance(blob_data, LiteralResult):
        return responses.JSONResponse(
            content={
                "content": blob_data.json()
            },
        )
    async with db.session_context(begin_transaction=True) as session:
        filesystem_document = await models.block_documents.read_block_document_by_id(
            session=session,
            block_document_id=blob_data.storage_block_id,
            include_secrets=True,
        )

    filesystem = Block._from_block_document(filesystem_document)
    content = await filesystem.read_path(blob_data.storage_key)
    serialized_result = PersistedResultBlob.parse_raw(content)
    obj = serialized_result.serializer.loads(serialized_result.data)
    return responses.JSONResponse(
        content={
            "content": jsonable_encoder(obj)
        },
    )
