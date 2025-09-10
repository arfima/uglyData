from fastapi import APIRouter, Path, Depends, Query

from uglyData.api.models import User, LogRecord
from ..dependencies import (
    get_asset,
    get_all_assets,
    limit_params,
)
from ..auth import get_current_user

router = APIRouter(prefix="/log", tags=["log"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        "dtime",
        "action_type",
        "user_name",
        "schema_name",
        "table_name",
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


def categories(
    action_type: str = Query(None, description="Type of the action."),
    username: str = Query(None, description="Username of the user."),
    table_name: str = Query(None, description="Name of the table."),
) -> dict:
    return {
        "action_type": action_type,
        "username": username,
        "table_name": table_name,
    }


@router.get("")
@router.get("/")
async def get_all_log(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(categories),
) -> list[LogRecord]:
    """Get all log in the database."""
    params, filters = prepare_params(params, **categories)

    return await get_all_assets(
        table="info.frontend_log", filters=filters, user=user, **params
    )


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(categories),
):
    params, filters = prepare_params(params, **categories)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.frontend_log",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{id}")
async def get_exchange_info(
    id: str = Path(description="Id of the record log"),
    user: User = Depends(get_current_user),
) -> LogRecord:
    """Get information about a single exchange."""
    return await get_asset(table="info.frontend_log", values={"id": id}, user=user)
