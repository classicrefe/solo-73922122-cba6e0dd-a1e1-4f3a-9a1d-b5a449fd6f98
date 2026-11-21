"""FastAPI application exposing the repartition-planning workflow."""

from contextlib import asynccontextmanager
from typing import Annotated, Optional

from fastapi import Body, Depends, FastAPI, Path, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import __version__
from .config import Settings
from .db import Database
from .errors import ApiError
from .models import (
    Day,
    HealthOut,
    PlanOut,
    PlanRequest,
    PublicationOut,
    PublishRequest,
    SourceManifestIn,
    SourceManifestOut,
)
from .service import RepartitionService

API_DESCRIPTION = """
Repartition the daily passage-detail partitions of the toll-road data lake while
reusing as many existing Parquet files as possible.

### Workflow

1. **Register a source manifest** for a day (`POST /days/{day}/source-manifests`).
   Station ids are sorted automatically. Each station may declare the
   `current_range` of the existing Parquet file it belongs to. Versions are
   allocated per day and increase monotonically — a new registration supersedes
   the previous version.
2. **Plan** (`POST /days/{day}/plans`) with `partition_count` and
   `max_partition_bytes`. Stations are split into that many *consecutive*
   partitions in station-id order; a station is never split across partitions.
   A partition reuses an existing file only when its station range is exactly
   equal to a declared current range. Among all feasible layouts the service
   returns the one with the fewest rewritten bytes (ties broken by the
   lexicographically smallest partition boundaries).
3. **Confirm** (`POST /days/{day}/plans/{plan_id}/confirm`).
4. **Publish** (`POST /days/{day}/plans/{plan_id}/publish`). The published
   payload is what the rewrite job reads (`GET /days/{day}/publication`).

### Guarantees

* A publication exists at most once per source version, even under concurrency.
* Retrying a plan request or a publication returns the original record.
* Infeasible requests return `422` and publish nothing.
* Registering a newer source version stops the old rewrite manifest from being
  served (`410`) and blocks confirmation/publication of plans made against the
  old version.
"""


def create_app(settings: Optional[Settings] = None, db: Optional[Database] = None) -> FastAPI:
    settings = settings or Settings.from_env()
    database = db or Database(settings.database_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.initialize()
        app.state.service = RepartitionService(
            database,
            max_stations_per_manifest=settings.max_stations_per_manifest,
            max_dp_cells=settings.max_dp_cells,
        )
        yield

    app = FastAPI(
        title="Toll Passage Repartition Planner",
        version=__version__,
        description=API_DESCRIPTION,
        lifespan=lifespan,
    )

    def get_service(request: Request) -> RepartitionService:
        return request.app.state.service

    ServiceDep = Annotated[RepartitionService, Depends(get_service)]
    DayPath = Annotated[str, Path(pattern=r"^\d{4}-\d{2}-\d{2}$", description="Partition day, YYYY-MM-DD")]

    @app.exception_handler(ApiError)
    async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.as_detail()})

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = []
        for err in exc.errors():
            cleaned = {k: v for k, v in err.items() if k != "ctx"}
            errors.append(cleaned)
        return JSONResponse(
            status_code=422,
            content={
                "detail": {
                    "code": "validation_error",
                    "message": "request validation failed",
                    "errors": errors,
                }
            },
        )

    @app.get("/health", response_model=HealthOut, tags=["meta"])
    def health() -> HealthOut:
        return HealthOut(status="ok", version=__version__)

    # ------------------------------------------------------------- manifests
    @app.post(
        "/days/{day}/source-manifests",
        response_model=dict,
        status_code=201,
        tags=["source manifests"],
        summary="Register the next source manifest version for a day",
    )
    def register_manifest(
        day: DayPath,
        service: ServiceDep,
        body: SourceManifestIn = Body(
            ...,
            openapi_examples={
                "two_ranges_and_one_new_station": {
                    "summary": "Two existing Parquet ranges plus a brand-new station",
                    "value": {
                        "version": 3,
                        "stations": [
                            {"station_id": 1001, "estimated_bytes": 120_000_000,
                             "current_range": {"start_station": 1001, "end_station": 1020}},
                            {"station_id": 1002, "estimated_bytes": 95_000_000,
                             "current_range": {"start_station": 1001, "end_station": 1020}},
                            {"station_id": 1021, "estimated_bytes": 80_000_000,
                             "current_range": {"start_station": 1021, "end_station": 1040}},
                            {"station_id": 1022, "estimated_bytes": 70_000_000,
                             "current_range": {"start_station": 1021, "end_station": 1040}},
                            {"station_id": 1041, "estimated_bytes": 30_000_000},
                        ],
                    },
                }
            },
        ),
    ) -> dict:
        return service.register_manifest(day, body)

    @app.get(
        "/days/{day}/source-manifests",
        response_model=dict,
        tags=["source manifests"],
        summary="List source manifest versions for a day",
    )
    def list_manifests(day: DayPath, service: ServiceDep) -> dict:
        return service.list_manifests(day)

    @app.get(
        "/days/{day}/source-manifest",
        response_model=SourceManifestOut,
        tags=["source manifests"],
        summary="Get the current source manifest",
    )
    def current_manifest(day: DayPath, service: ServiceDep) -> SourceManifestOut:
        return service.get_current_manifest(day)

    # ----------------------------------------------------------------- plans
    @app.post(
        "/days/{day}/plans",
        response_model=PlanOut,
        tags=["plans"],
        summary="Plan a minimum-rewrite repartition",
        responses={
            422: {"description": "No feasible layout (reason code included)"},
        },
    )
    def create_plan(day: DayPath, body: PlanRequest, service: ServiceDep) -> PlanOut:
        plan, _created = service.create_plan(day, body)
        return plan

    @app.get(
        "/days/{day}/plans",
        response_model=dict,
        tags=["plans"],
        summary="List plans for a day",
    )
    def list_plans(day: DayPath, service: ServiceDep) -> dict:
        return service.list_plans(day)

    @app.get(
        "/days/{day}/plans/{plan_id}",
        response_model=PlanOut,
        tags=["plans"],
        summary="Fetch a stored plan",
    )
    def get_plan(
        day: DayPath,
        service: ServiceDep,
        plan_id: Annotated[int, Path(ge=1)],
    ) -> PlanOut:
        return service.get_plan(day, plan_id)

    @app.post(
        "/days/{day}/plans/{plan_id}/confirm",
        response_model=PlanOut,
        tags=["plans"],
        summary="Confirm a plan so it may be published",
    )
    def confirm_plan(
        day: DayPath,
        service: ServiceDep,
        plan_id: Annotated[int, Path(ge=1)],
        body: PublishRequest = Body(
            ..., openapi_examples={"default": {"value": {"operator": "ops-zhang"}}}
        ),
    ) -> PlanOut:
        return service.confirm_plan(day, plan_id, body.operator)

    # ---------------------------------------------------------- publications
    @app.post(
        "/days/{day}/plans/{plan_id}/publish",
        response_model=PublicationOut,
        status_code=201,
        tags=["publications"],
        summary="Publish the confirmed plan for the rewrite job",
        responses={
            200: {"description": "Retry of the same plan: original publication receipt returned"},
            409: {"description": "Plan not confirmed / superseded / other plan already published"},
        },
    )
    def publish(
        day: DayPath,
        service: ServiceDep,
        response: Response,
        plan_id: Annotated[int, Path(ge=1)],
        body: PublishRequest = Body(
            ..., openapi_examples={"default": {"value": {"operator": "ops-zhang"}}}
        ),
    ) -> PublicationOut:
        publication, created = service.publish(day, plan_id, body.operator)
        if not created:
            response.status_code = 200
        return publication

    @app.get(
        "/days/{day}/publication",
        response_model=PublicationOut,
        tags=["publications"],
        summary="Read the published rewrite manifest served to the rewrite job",
        responses={410: {"description": "Superseded by a newer source version"}},
    )
    def get_publication(day: DayPath, service: ServiceDep) -> PublicationOut:
        return service.get_publication(day)

    return app


app = create_app()
