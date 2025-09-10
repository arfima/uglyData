"""Custom Instruments api routers."""

# ruff: noqa: B008
from fastapi import (
    APIRouter,
    Path,
    Depends,
)

from uglyData.api.models import CustomIndex, User
from ..dependencies import (
    get_asset,
    get_all_assets,
    limit_params,
)
from ..auth import get_current_user

router = APIRouter(prefix="/custom_indices", tags=["custom_indices"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("custom_index", 3),
        ("class_name", 2),
        ("description", 1),
        ("tags", 2),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_cdx(
    params=Depends(limit_params), user: User = Depends(get_current_user)
) -> list[CustomIndex]:
    """Get all custom indices in the database."""
    params, filters = prepare_params(params)
    return await get_all_assets(
        table="info.custom_instr_tags", auth_table="market", user=user, **params
    )


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
):
    """Get the number of custom indices."""
    params, filters = prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.custom_instr_tags",
        user=user,
        auth_table="market",
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{custom_index}")
async def get_exchange_info(
    custom_index: str = Path(description="Name of the custom_index."),
    user: User = Depends(get_current_user),
) -> CustomIndex:
    """Get information about a single custom index."""
    return await get_asset(
        table="info.custom_instr_tags",
        auth_table="market",
        values={"custom_index": custom_index},
        user=user,
    )
