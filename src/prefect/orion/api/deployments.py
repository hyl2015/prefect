"""
Routes for interacting with Deployment objects.
"""

import datetime
from tempfile import NamedTemporaryFile
from typing import List
from uuid import UUID
from typing import Optional

import pendulum
import yaml
import json
import sqlalchemy as sa
from fastapi import (
    Body,
    Depends,
    HTTPException,
    Path,
    Response,
    status,
    Form,
    UploadFile,
)
from prefect.blocks.storage import S3StorageBlock

import prefect.orion.api.dependencies as dependencies
import prefect.orion.models as models
import prefect.orion.schemas as schemas
from prefect.orion.database.dependencies import provide_database_interface
from prefect.orion.database.interface import OrionDBInterface
from prefect.orion.exceptions import ObjectNotFoundError
from prefect.orion.utilities.server import OrionRouter
from prefect.utilities.inspect import export_tasks

router = OrionRouter(prefix="/deployments", tags=["Deployments"])


async def _create_deployment(
    deployment: schemas.core.Deployment, db: OrionDBInterface, session: sa.orm.Session
) -> schemas.core.Deployment:
    model = await models.deployments.create_deployment(
        session=session, deployment=deployment
    )

    # this deployment might have already scheduled runs (if it's being upserted)
    # so we delete them all here; if the upserted deployment has an active schedule
    # then its runs will be rescheduled.
    delete_query = sa.delete(db.FlowRun).where(
        db.FlowRun.deployment_id == model.id,
        db.FlowRun.state_type == schemas.states.StateType.SCHEDULED.value,
        db.FlowRun.auto_scheduled.is_(True),
    )
    await session.execute(delete_query)
    return model


@router.post("/upload")
async def upload_deployment(
    script_file: UploadFile,
    name: str = Form(),
    schedule: Optional[str] = Form(None),
    flow_runner: Optional[str] = Form(None),
    is_schedule_active: bool = Form(default=True),
    tags: Optional[str] = Form(None),
    parameters: Optional[str] = Form(None),
    session: sa.orm.Session = Depends(dependencies.get_session),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> UUID | None:
    """
    Gracefully creates a new deployment from the provided schema. If a deployment with
    the same name and flow_id already exists, the deployment is updated.

    If the deployment has an active schedule, flow runs will be scheduled.
    When upserting, any scheduled runs from the existing deployment will be deleted.
    """
    from prefect.deployments import load_deployments_from_yaml, Deployment, DataDocument
    from prefect.cli.deployment import _deployment_to_manifest

    deployment_id = None
    with NamedTemporaryFile(
        mode="wb",
        prefix="flow",
        suffix=".py",
    ) as tmpfile, NamedTemporaryFile(
        mode="wt",
        prefix="deployment",
        suffix=".yml",
    ) as tmp_yaml:
        contents = await script_file.read()
        tmpfile.write(contents)
        tmpfile.flush()
        spec_data = {"name": name, "flow": {"path": tmpfile.name}}
        if schedule:
            spec_data["schedule"] = json.loads(schedule)

        if tags:
            spec_data["tags"] = json.loads(tags)

        if flow_runner:
            spec_data["flow_runner"] = json.loads(flow_runner)

        if parameters:
            spec_data["parameters"] = json.loads(parameters)
        tmp_yaml.write(yaml.dump(spec_data))
        tmp_yaml.flush()
        registry = load_deployments_from_yaml(tmp_yaml.name)
        valid_deployments = registry.get_instances(Deployment)
        if valid_deployments:
            deployment = valid_deployments[0]
            manifest = await _deployment_to_manifest(deployment)
            if "image" in manifest.__fields__:
                flow_runner = deployment.flow_runner.copy(
                    update={"image": manifest.image}
                )
            else:
                flow_runner = deployment.flow_runner
            flow_data = DataDocument.encode("package-manifest", manifest)
            flow = schemas.core.Flow(name=manifest.flow_name)
            flow_model = await models.flows.create_flow(session=session, flow=flow)
            deployment_model = await _create_deployment(
                schemas.core.Deployment(
                    **{
                        "is_schedule_active": is_schedule_active,
                        "flow_id": flow_model.id,
                        "name": deployment.name or manifest.flow_name,
                        "flow_data": flow_data,
                        "schedule": deployment.schedule,
                        "parameters": deployment.parameters,
                        "tags": deployment.tags,
                        "flow_runner": flow_runner.to_settings(),
                    }
                ),
                db,
                session,
            )
            deployment_id = deployment_model.id

    return deployment_id


@router.post("/")
async def create_deployment(
    deployment: schemas.actions.DeploymentCreate,
    response: Response,
    session: sa.orm.Session = Depends(dependencies.get_session),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> schemas.core.Deployment:
    """
    Gracefully creates a new deployment from the provided schema. If a deployment with
    the same name and flow_id already exists, the deployment is updated.

    If the deployment has an active schedule, flow runs will be scheduled.
    When upserting, any scheduled runs from the existing deployment will be deleted.
    """

    # hydrate the input model into a full model
    now = pendulum.now()
    deployment = schemas.core.Deployment(**deployment.dict())
    model = await _create_deployment(deployment, db, session)
    if model.created >= now:
        response.status_code = status.HTTP_201_CREATED
    return model


@router.get("/name/{flow_name}/{deployment_name}")
async def read_deployment_by_name(
    flow_name: str = Path(..., description="The name of the flow"),
    deployment_name: str = Path(..., description="The name of the deployment"),
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> schemas.core.Deployment:
    """
    Get a deployment using the name of the flow and the deployment.
    """
    deployment = await models.deployments.read_deployment_by_name(
        session=session, name=deployment_name, flow_name=flow_name
    )
    if not deployment:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Deployment not found")
    return deployment


@router.get("/{id}/inspect")
async def inspect_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    session: sa.orm.Session = Depends(dependencies.get_session),
):
    """
    Get a deployment by id.
    """
    export_data = {"imports": [], "tasks": [], "flows": []}
    deployment = await models.deployments.read_deployment(
        session=session, deployment_id=deployment_id
    )
    if not deployment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )
    flow_data = deployment.flow_data
    if flow_data:
        flow_content = flow_data.decode()
        block_document = await models.block_documents.read_block_document_by_id(
            session=session, block_document_id=flow_content["block_document_id"]
        )
        storage_block = S3StorageBlock.parse_obj(block_document.data)
        flow_bytes = await storage_block.read(flow_content["data"])
        with NamedTemporaryFile(
            mode="wb",
            prefix="flow",
            suffix=".py",
        ) as tmpfile:
            tmpfile.write(flow_bytes)
            tmpfile.flush()
            export_data = export_tasks(tmpfile.name)
    return export_data


@router.get("/tags")
async def read_deployment_tags(
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> list[str]:
    tags = await models.deployments.read_deployment_tags(session=session)
    return tags


@router.get("/{id}")
async def read_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> schemas.core.Deployment:
    """
    Get a deployment by id.
    """
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
    sort: schemas.sorting.DeploymentSort = Body(
        schemas.sorting.DeploymentSort.CREATED_TIME_DESC
    ),
    limit: int = dependencies.LimitBody(),
    offset: int = Body(0, ge=0),
    flows: schemas.filters.FlowFilter = None,
    flow_runs: schemas.filters.FlowRunFilter = None,
    task_runs: schemas.filters.TaskRunFilter = None,
    deployments: schemas.filters.DeploymentFilter = None,
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> List[schemas.responses.DeploymentResponse]:
    """
    Query for deployments.
    """
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
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> int:
    """
    Count deployments.
    """
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
    session: sa.orm.Session = Depends(dependencies.get_session),
    db: OrionDBInterface = Depends(provide_database_interface),
):
    """
    Delete a deployment by id.
    """
    result = await models.deployments.delete_deployment(
        session=session, deployment_id=deployment_id
    )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )

    # if the delete succeeded, delete any scheduled runs
    delete_query = sa.delete(db.FlowRun).where(
        db.FlowRun.deployment_id == deployment_id,
        db.FlowRun.state_type == schemas.states.StateType.SCHEDULED.value,
    )
    await session.execute(delete_query)


@router.post("/{id}/schedule")
async def schedule_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    start_time: datetime.datetime = Body(
        None, description="The earliest date to schedule"
    ),
    end_time: datetime.datetime = Body(None, description="The latest date to schedule"),
    max_runs: int = Body(None, description="The maximum number of runs to schedule"),
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> None:
    """
    Schedule runs for a deployment. For backfills, provide start/end times in the past.
    """
    await models.deployments.schedule_runs(
        session=session,
        deployment_id=deployment_id,
        start_time=start_time,
        end_time=end_time,
        max_runs=max_runs,
    )


@router.post("/{id}/set_schedule_active")
async def set_schedule_active(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    session: sa.orm.Session = Depends(dependencies.get_session),
) -> None:
    """
    Set a deployment schedule to active. Runs will be scheduled immediately.
    """
    deployment = await models.deployments.read_deployment(
        session=session, deployment_id=deployment_id
    )
    if not deployment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )
    deployment.is_schedule_active = True
    await session.flush()


@router.post("/{id}/set_schedule_inactive")
async def set_schedule_inactive(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    session: sa.orm.Session = Depends(dependencies.get_session),
    db: OrionDBInterface = Depends(provide_database_interface),
) -> None:
    """
    Set a deployment schedule to inactive. Any auto-scheduled runs still in a Scheduled
    state will be deleted.
    """
    deployment = await models.deployments.read_deployment(
        session=session, deployment_id=deployment_id
    )
    if not deployment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )
    deployment.is_schedule_active = False
    await session.flush()

    # delete any future scheduled runs that were auto-scheduled
    delete_query = sa.delete(db.FlowRun).where(
        db.FlowRun.deployment_id == deployment.id,
        db.FlowRun.state_type == schemas.states.StateType.SCHEDULED.value,
        db.FlowRun.auto_scheduled.is_(True),
    )
    await session.execute(delete_query)


@router.post("/{id}/create_flow_run")
async def create_flow_run_from_deployment(
    flow_run: schemas.actions.DeploymentFlowRunCreate,
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    session: sa.orm.Session = Depends(dependencies.get_session),
    response: Response = None,
) -> schemas.core.FlowRun:
    """
    Create a flow run from a deployment.

    Any parameters not provided will be inferred from the deployment's parameters.
    If tags are not provided, the deployment's tags will be used.

    If no state is provided, the flow run will be created in a PENDING state.
    """
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

    # Do not allow both infrastructure and flow runner to be set
    if flow_run.infrastructure_document_id:
        infrastructure = flow_run.infrastructure_document_id
        flow_runner = None
    elif flow_run.flow_runner:
        infrastructure = None
        flow_runner = flow_run.flow_runner
    else:
        infrastructure = deployment.infrastructure_document_id
        flow_runner = deployment.flow_runner

    # hydrate the input model into a full flow run / state model
    flow_run = schemas.core.FlowRun(
        **flow_run.dict(
            exclude={
                "parameters",
                "tags",
                "runner_type",
                "runner_config",
                "flow_runner",
                "infrastructure_document_id",
            }
        ),
        flow_id=deployment.flow_id,
        deployment_id=deployment.id,
        parameters=parameters,
        tags=set(flow_run.tags) if flow_run.tags else deployment.tags,
        flow_runner=flow_runner,
        infrastructure_document_id=infrastructure,
    )

    if not flow_run.state:
        flow_run.state = schemas.states.Pending()

    now = pendulum.now("UTC")
    model = await models.flow_runs.create_flow_run(session=session, flow_run=flow_run)
    if model.created >= now:
        response.status_code = status.HTTP_201_CREATED
    return model


@router.get("/{id}/work_queue_check")
async def work_queue_check_for_deployment(
    deployment_id: UUID = Path(..., description="The deployment id", alias="id"),
    session: sa.orm.Session = Depends(dependencies.get_session),
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
        work_queues = await models.deployments.check_work_queues_for_deployment(
            session=session, deployment_id=deployment_id
        )
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found"
        )
    return work_queues
