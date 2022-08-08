"""
Routes for interacting with Deployment objects.
"""
import json
import os.path
import uuid
from datetime import datetime, timedelta
from inspect import getdoc
from tempfile import NamedTemporaryFile
from typing import List, Optional
from uuid import UUID

import pendulum
from fastapi import Body, Depends,Form, UploadFile, HTTPException, Path, Response, status

import prefect.orion.api.dependencies as dependencies
import prefect.orion.models as models
import prefect.orion.schemas as schemas
from prefect.blocks.core import Block
from prefect.infrastructure import (
    DockerContainer,
    KubernetesJob,
    Process,
)
from prefect.orion.database.dependencies import provide_database_interface
from prefect.orion.database.interface import OrionDBInterface
from prefect.orion.exceptions import ObjectNotFoundError
from prefect.orion.utilities.schemas import DateTimeTZ
from prefect.orion.utilities.server import OrionRouter
from prefect.utilities.callables import parameter_schema
from prefect.utilities.importtools import import_object

router = OrionRouter(prefix="/deployments", tags=["Deployments"])


@router.get("/tags")
async def read_deployment_tags(
        db: OrionDBInterface = Depends(provide_database_interface),
) -> list[str]:
    async with db.session_context(begin_transaction=True) as session:
        tags = await models.deployments.read_deployment_tags(session=session)
    return tags


@router.get("/infrastructures")
async def read_all_infrastructures() -> list[dict]:
    return [Process().dict(), DockerContainer().dict(), KubernetesJob().dict()]


@router.post("/")
async def create_deployment(
    deployment: schemas.actions.DeploymentCreate,
    response: Response,
    db: OrionDBInterface = Depends(provide_database_interface),
) -> schemas.core.Deployment:
    """
    Gracefully creates a new deployment from the provided schema. If a deployment with
    the same name and flow_id already exists, the deployment is updated.

    If the deployment has an active schedule, flow runs will be scheduled.
    When upserting, any scheduled runs from the existing deployment will be deleted.
    """

    # hydrate the input model into a full model
    deployment = schemas.core.Deployment(**deployment.dict())

    async with db.session_context(begin_transaction=True) as session:
        # check to see if relevant blocks exist, allowing us throw a useful error message
        # for debugging
        if deployment.infrastructure_document_id is not None:
            infrastructure_block = (
                await models.block_documents.read_block_document_by_id(
                    session=session,
                    block_document_id=deployment.infrastructure_document_id,
                )
            )
            if not infrastructure_block:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Error creating deployment. Could not find infrastructure block with id: {deployment.infrastructure_document_id}. This usually occurs when applying a deployment specification that was built against a different Prefect database / workspace.",
                )

        if deployment.storage_document_id is not None:
            infrastructure_block = (
                await models.block_documents.read_block_document_by_id(
                    session=session,
                    block_document_id=deployment.storage_document_id,
                )
            )
            if not infrastructure_block:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Error creating deployment. Could not find storage block with id: {deployment.storage_document_id}. This usually occurs when applying a deployment specification that was built against a different Prefect database / workspace.",
                )

        now = pendulum.now()
        model = await models.deployments.create_deployment(
            session=session, deployment=deployment
        )

    if model.created >= now:
        response.status_code = status.HTTP_201_CREATED

    return model


@router.patch("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def update_deployment(
    deployment: schemas.actions.DeploymentUpdate,
    deployment_id: str = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
):
    async with db.session_context(begin_transaction=True) as session:
        result = await models.deployments.update_deployment(
            session=session, deployment_id=deployment_id, deployment=deployment
        )
    if not result:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Deployment not found.")


@router.get("/name/{flow_name}/{deployment_name}")
async def read_deployment_by_name(
    flow_name: str = Path(..., description="The name of the flow"),
    deployment_name: str = Path(..., description="The name of the deployment"),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> schemas.core.Deployment:
    """
    Get a deployment using the name of the flow and the deployment.
    """
    async with db.session_context() as session:
        deployment = await models.deployments.read_deployment_by_name(
            session=session, name=deployment_name, flow_name=flow_name
        )
    if not deployment:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Deployment not found")
    return deployment


@router.get("/{id}")
async def read_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> schemas.core.Deployment:
    """
    Get a deployment by id.
    """
    async with db.session_context() as session:
        deployment = await models.deployments.read_deployment(
            session=session, deployment_id=deployment_id
        )
    if not deployment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )
    return deployment


@router.post("/filter")
async def read_deployments(
    limit: int = dependencies.LimitBody(),
    offset: int = Body(0, ge=0),
    flows: schemas.filters.FlowFilter = None,
    flow_runs: schemas.filters.FlowRunFilter = None,
    task_runs: schemas.filters.TaskRunFilter = None,
    deployments: schemas.filters.DeploymentFilter = None,
    sort: schemas.sorting.DeploymentSort = Body(
        schemas.sorting.DeploymentSort.CREATED_DESC
    ),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> List[schemas.responses.DeploymentResponse]:
    """
    Query for deployments.
    """
    async with db.session_context() as session:
        return await models.deployments.read_deployments(
            session=session,
            offset=offset,
            limit=limit,
            flow_filter=flows,
            flow_run_filter=flow_runs,
            task_run_filter=task_runs,
            deployment_filter=deployments,
            sort=sort,
        )


@router.post("/count")
async def count_deployments(
    flows: schemas.filters.FlowFilter = None,
    flow_runs: schemas.filters.FlowRunFilter = None,
    task_runs: schemas.filters.TaskRunFilter = None,
    deployments: schemas.filters.DeploymentFilter = None,
    db: OrionDBInterface = Depends(provide_database_interface),
) -> int:
    """
    Count deployments.
    """
    async with db.session_context() as session:
        return await models.deployments.count_deployments(
            session=session,
            flow_filter=flows,
            flow_run_filter=flow_runs,
            task_run_filter=task_runs,
            deployment_filter=deployments,
        )


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
):
    """
    Delete a deployment by id.
    """
    async with db.session_context(begin_transaction=True) as session:
        result = await models.deployments.delete_deployment(
            session=session, deployment_id=deployment_id
        )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )


@router.post("/{id}/schedule")
async def schedule_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    start_time: DateTimeTZ = Body(None, description="The earliest date to schedule"),
    end_time: DateTimeTZ = Body(None, description="The latest date to schedule"),
    min_time: timedelta = Body(
        None,
        description="Runs will be scheduled until at least this long after the `start_time`",
    ),
    min_runs: int = Body(None, description="The minimum number of runs to schedule"),
    max_runs: int = Body(None, description="The maximum number of runs to schedule"),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> None:
    """
    Schedule runs for a deployment. For backfills, provide start/end times in the past.

    This function will generate the minimum number of runs that satisfy the min
    and max times, and the min and max counts. Specifically, the following order
    will be respected:

        - Runs will be generated starting on or after the `start_time`
        - No more than `max_runs` runs will be generated
        - No runs will be generated after `end_time` is reached
        - At least `min_runs` runs will be generated
        - Runs will be generated until at least `start_time + min_time` is reached
    """
    async with db.session_context(begin_transaction=True) as session:
        await models.deployments.schedule_runs(
            session=session,
            deployment_id=deployment_id,
            start_time=start_time,
            min_time=min_time,
            end_time=end_time,
            min_runs=min_runs,
            max_runs=max_runs,
        )


@router.post("/{id}/set_schedule_active")
async def set_schedule_active(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> None:
    """
    Set a deployment schedule to active. Runs will be scheduled immediately.
    """
    async with db.session_context(begin_transaction=True) as session:
        deployment = await models.deployments.read_deployment(
            session=session, deployment_id=deployment_id
        )
        if not deployment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
            )
        deployment.is_schedule_active = True


@router.post("/{id}/set_schedule_inactive")
async def set_schedule_inactive(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> None:
    """
    Set a deployment schedule to inactive. Any auto-scheduled runs still in a Scheduled
    state will be deleted.
    """
    async with db.session_context(begin_transaction=False) as session:
        deployment = await models.deployments.read_deployment(
            session=session, deployment_id=deployment_id
        )
        if not deployment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
            )
        deployment.is_schedule_active = False
        # commit here to make the inactive schedule "visible" to the scheduler service
        await session.commit()

        # delete any auto scheduled runs
        await models.deployments._delete_scheduled_runs(
            session=session,
            deployment_id=deployment_id,
            db=db,
            auto_scheduled_only=True,
        )

        await session.commit()


@router.post("/{id}/create_flow_run")
async def create_flow_run_from_deployment(
    flow_run: schemas.actions.DeploymentFlowRunCreate,
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
    response: Response = None,
) -> schemas.core.FlowRun:
    """
    Create a flow run from a deployment.

    Any parameters not provided will be inferred from the deployment's parameters.
    If tags are not provided, the deployment's tags will be used.

    If no state is provided, the flow run will be created in a PENDING state.
    """
    async with db.session_context(begin_transaction=True) as session:
        # get relevant info from the deployment
        deployment = await models.deployments.read_deployment(
            session=session, deployment_id=deployment_id
        )

        if not deployment:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
            )

        parameters = deployment.parameters
        parameters.update(flow_run.parameters or {})
        if not flow_run.work_queue_name:
            flow_run.work_queue_name = deployment.work_queue_name

        # hydrate the input model into a full flow run / state model
        flow_run = schemas.core.FlowRun(
            **flow_run.dict(
                exclude={
                    "parameters",
                    "tags",
                    "infrastructure_document_id",
                }
            ),
            flow_id=deployment.flow_id,
            deployment_id=deployment.id,
            parameters=parameters,
            tags=set(flow_run.tags) if flow_run.tags else deployment.tags,
            infrastructure_document_id=(
                flow_run.infrastructure_document_id
                or deployment.infrastructure_document_id
            )
        )

        if not flow_run.state:
            flow_run.state = schemas.states.Pending()

        now = pendulum.now("UTC")
        model = await models.flow_runs.create_flow_run(
            session=session, flow_run=flow_run
        )
        if model.created >= now:
            response.status_code = status.HTTP_201_CREATED
        return model


# DEPRECATED
@router.get("/{id}/work_queue_check", deprecated=True)
async def work_queue_check_for_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> List[schemas.core.WorkQueue]:
    """
    Get list of work-queues that are able to pick up the specified deployment.

    This endpoint is intended to be used by the UI to provide users warnings
    about deployments that are unable to be executed because there are no work
    queues that will pick up their runs, based on existing filter criteria. It
    may be deprecated in the future because there is not a strict relationship
    between work queues and deployments.
    """
    try:
        async with db.session_context() as session:
            work_queues = await models.deployments.check_work_queues_for_deployment(
                session=session, deployment_id=deployment_id
            )
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )
    return work_queues


@router.post("/upload")
async def upload_deployment(
    script_file: UploadFile,
    response: Response,
    name: str = Form(None),
    flow_fn_name: str = Form(None),
    infrastructure_id: UUID = Form(None),
    storage_id: UUID = Form(None),
    schedule: Optional[str] = Form(None),
    queue_name: Optional[str] = Form(None),
    is_schedule_active: bool = Form(default=True),
    tags: Optional[str] = Form(None),
    parameters: Optional[str] = Form(None),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> UUID | None:
    """
    Gracefully creates a new deployment from the provided schema. If a deployment with
    the same name and flow_id already exists, the deployment is updated.

    If the deployment has an active schedule, flow runs will be scheduled.
    When upserting, any scheduled runs from the existing deployment will be deleted.
    """
    from prefect.deployments import Deployment

    deployment_id = None
    tmp_dir = datetime.now().strftime("%Y-%m")
    with NamedTemporaryFile(
        mode="wb",
        prefix=f"flow-{uuid.uuid4()}",
        suffix=".py",
    ) as tmpfile:
        contents = await script_file.read()
        tmpfile.write(contents)
        tmpfile.flush()
        spec_data = {
            "name": name,
        }
        flow_path = f"{tmpfile.name}:{flow_fn_name}"
        flow = import_object(flow_path)
        flow_parameter_schema = parameter_schema(flow)
        description = getdoc(flow)
        entrypoint = (
            f"{tmp_dir}/{os.path.basename(tmpfile.name)}:{flow_fn_name}"
        )

        spec_data["parameter_openapi_schema"] = flow_parameter_schema
        spec_data["entrypoint"] = entrypoint
        spec_data["flow_name"] = flow.name
        spec_data["description"] = description

        if schedule:
            spec_data["schedule"] = json.loads(schedule)

        if tags:
            spec_data["tags"] = json.loads(tags)

        if parameters:
            spec_data["parameters"] = json.loads(parameters)
        async with db.session_context(begin_transaction=True) as session:

            storage_block = await models.block_documents.read_block_document_by_id(
                session=session, block_document_id=storage_id, include_secrets=True
            )
            print('storage_block',storage_block)
            if not storage_block:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Storage block not found",
                )
            template = Block._from_block_document(storage_block)
            storage = template.copy(
                exclude={"_block_document_id", "_block_document_name", "_is_anonymous"}
            )
            await storage.write_path(
                f"{tmp_dir}/{os.path.basename(tmpfile.name)}", contents
            )
            spec_data['storage'] = storage
            try:
                deployment = Deployment(**spec_data)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Provided file did not conform to deployment spec: {exc!r}",
                )
            flow = schemas.core.Flow(name=deployment.flow_name)
            flow_model = await models.flows.create_flow(session=session, flow=flow)

        deployment_create = schemas.actions.DeploymentCreate(
            flow_id=flow_model.id,
            work_queue_name=queue_name,
            name=deployment.name,
            version=deployment.version,
            schedule=deployment.schedule,
            parameters=dict(deployment.parameters or {}),
            description=deployment.description,
            tags=list(deployment.tags or []),
            entrypoint=entrypoint,
            storage_document_id=storage_id,
            infrastructure_document_id=infrastructure_id,
            is_schedule_active=is_schedule_active,
            parameter_openapi_schema=flow_parameter_schema.dict(),
        )
        resp_dep = await create_deployment(
            deployment_create, response=response, db=db
        )
        deployment_id = resp_dep.id

    return deployment_id
