import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from uglyData.api.models import User
from elasticapm.contrib.starlette import ElasticAPM, make_apm_client
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from uglyData import __version__ as version
from uglyData.api.auth import get_current_user
from uglyData.api.routers import (
    audit_trail,
    bonds,
    columns,
    custom_indices,
    drivers,
    ecoreleases,
    events,
    exchanges,
    families,
    instruments,
    log,
    market,
    products,
    spreads,
    subfamilies,
    tags,
    users,
    minio,
)
from uglyData.api.service import DB
from typing import Annotated
import typer
import uvicorn

LOG = logging.getLogger(__name__)

LOG.debug("Starting app")

APP_NAME = "UglyData API"
APP_DESCRIPTION = "UglyData API"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # connect to the db
    if "API_DB_CONN_INFO" not in os.environ:
        raise KeyError("API_DB_CONN_INFO not set in environment")
    await DB.connect(os.environ["API_DB_CONN_INFO"])
    yield
    # Close the db connection
    await DB.close()


cli = typer.Typer(help=APP_DESCRIPTION)


@cli.command()
def run(
    host: Annotated[
        str, typer.Option(envvar="API_HOST", help="Api host.")
    ] = "10.66.40.13",
    port: Annotated[int, typer.Option(envvar="API_PORT", help="Api port.")] = 15555,
    bucket: Annotated[
        str,
        typer.Option(envvar="MINIO_BUCKET", help="Minio bucket name."),
    ] = "foo",
    access_key: Annotated[
        str,
        typer.Option(envvar="MINIO_ACCESS_KEY", help="Minio access key."),
    ] = "minioadmin",
    secret_key: Annotated[
        str,
        typer.Option(envvar="MINIO_SECRET_KEY", help="Minio secret key."),
    ] = "minioadmin",
    s3_endpoint: Annotated[
        str,
        typer.Option(envvar="MINIO_S3_ENDPOINT", help="Minio S3 endpoint."),
    ] = "http://localhost:9000",
    api_db_conn_info: Annotated[
        str,
        typer.Option(envvar="API_DB_CONN_INFO", help="Database connection info."),
    ] = "service=dbusermain",
):
    """Run the UglyData API server."""
    app = FastAPI(
        title="Arfima Data API",
        description="API for Arfima Database",
        version=version,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # change this to our apps
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    apm_config = {
        "SERVICE_NAME": "fastapi",
        "SERVER_URL": "http://10.66.10.9:8200",
        "ENVIRONMENT": "dev",
        "GLOBAL_LABELS": "platform=Demo, application=demo_testing",
        "ELASTIC_APM_AUTO_LOG_STACKS": True,
        "CAPTURE_BODY": "all",
        "SERVER_TIMEOUT": "10s",
        "ENABLED": False,
    }

    apm = make_apm_client(apm_config)

    app.add_middleware(ElasticAPM, client=apm)

    app_v1 = FastAPI(
        title="Arfima Data API",
        description="API for Arfima Database",
        version=version,
    )
    app_v1.include_router(products.router)
    app_v1.include_router(instruments.router)
    app_v1.include_router(market.router)
    app_v1.include_router(drivers.router)
    app_v1.include_router(custom_indices.router)
    app_v1.include_router(exchanges.router)
    app_v1.include_router(families.router)
    app_v1.include_router(subfamilies.router)
    app_v1.include_router(users.router)
    app_v1.include_router(log.router)
    app_v1.include_router(events.router)
    app_v1.include_router(spreads.router)
    app_v1.include_router(bonds.router)
    app_v1.include_router(ecoreleases.router)
    app_v1.include_router(tags.router)
    app_v1.include_router(minio.router)

    app_v1.include_router(audit_trail.router)

    app_v1.include_router(columns.router)

    app.mount("/api/v1", app_v1)

    @app.get("/")
    async def read_root():
        return RedirectResponse(url="/api/v1/redoc")

    @app.get("/api/v1")
    async def read_root_v1():
        return RedirectResponse(url="/api/v1/redoc")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/health/db")
    async def health_db():
        try:
            await DB.conn.fetchval("SELECT 1")
            return {"status": "ok"}
        except Exception as e:
            LOG.error(e)
            return {"status": "error", "message": str(e)}

    @app_v1.get("/user")
    async def get_user(user: User = Depends(get_current_user)):  # noqa: B008
        """Get information about the current user."""
        return user

    uvicorn.run(app, host=host, port=port)

    os.environ["BUCKET"] = bucket
    os.environ["ACCESS_KEY"] = access_key
    os.environ["SECRET_KEY"] = secret_key
    os.environ["S3_ENDPOINT"] = s3_endpoint
    os.environ["API_DB_CONN_INFO"] = api_db_conn_info


if __name__ == "__main__":
    cli(standalone_mode=False)
