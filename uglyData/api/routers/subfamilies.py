from fastapi import APIRouter, Body, Path, Depends, status, BackgroundTasks, Query

from uglyData.api.models import Subfamily, User
from ..dependencies import (
    get_asset,
    get_all_assets,
    post_asset,
    put_asset,
    limit_params,
    delete_asset,
)
from ..auth import get_current_user

router = APIRouter(prefix="/subfamilies", tags=["subfamilies"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        "subfamily",
        "family",
        "description",
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


def categories(
    family: list[str] | str = Query(None, description="Name of the family"),
) -> dict:
    return {
        "family": family,
    }


@router.get("")
@router.get("/")
async def get_all_subfamilies(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(categories),
) -> list[Subfamily]:
    """Get all subfamilies in the database."""
    params, filters = prepare_params(params, **categories)

    return await get_all_assets(
        table="info.subfamilies", filters=filters, user=user, **params
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
        table="info.subfamilies",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{subfamily}")
async def get_family_info(
    subfamily: str = Path(description="Name of the family"),
    user: User = Depends(get_current_user),
) -> Subfamily:
    """Get information about a single family."""
    return await get_asset(
        table="info.subfamilies", values={"subfamily": subfamily}, user=user
    )


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_subfamily(
    background_tasks: BackgroundTasks,
    subfamily: Subfamily = Body(...),
    user: User = Depends(get_current_user),
) -> Subfamily:
    """Add a family to the database."""
    return await post_asset(
        table="info.subfamilies",
        asset=subfamily,
        user=user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_subfamily(
    background_tasks: BackgroundTasks,
    family: Subfamily = Body(...),
    user: User = Depends(get_current_user),
) -> Subfamily:
    """Update a subfamily in the database."""
    return await put_asset(
        table="info.subfamilies",
        asset=family,
        pkeys=["subfamily"],
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_subfamily(
    background_tasks: BackgroundTasks,
    subfamily: Subfamily = Body(...),
    user: User = Depends(get_current_user),
):
    return await delete_asset(
        table="info.subfamilies",
        asset=subfamily,
        pkeys=["subfamily"],
        user=user,
        background_tasks=background_tasks,
    )
