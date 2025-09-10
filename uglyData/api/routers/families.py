from fastapi import (
    APIRouter,
    Body,
    Path,
    Depends,
    status,
    BackgroundTasks,
)

from uglyData.api.models import Family, User
from ..dependencies import (
    get_asset,
    get_all_assets,
    post_asset,
    put_asset,
    delete_asset,
    limit_params,
)
from ..auth import get_current_user

router = APIRouter(prefix="/families", tags=["families"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        "family",
        "description",
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_families(
    params=Depends(limit_params), user: User = Depends(get_current_user)
) -> list[Family]:
    """Get all families in the database."""

    params, filters = prepare_params(params)
    return await get_all_assets(table="info.families", user=user, **params)


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
):
    params, filters = prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.families",
        user=user,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{family}")
async def get_family_info(
    family: str = Path(description="Name of the family"),
    user: User = Depends(get_current_user),
) -> Family:
    """Get information about a single family."""
    return await get_asset(table="info.families", values={"family": family}, user=user)


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_family(
    background_tasks: BackgroundTasks,
    family: Family = Body(...),
    user: User = Depends(get_current_user),
) -> Family:
    """Add a family to the database."""
    return await post_asset(
        table="info.families",
        asset=family,
        user=user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_family(
    background_tasks: BackgroundTasks,
    family: Family = Body(...),
    user: User = Depends(get_current_user),
) -> Family:
    """Update a family in the database."""
    return await put_asset(
        table="info.families",
        asset=family,
        pkeys=["family"],
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_family(
    background_tasks: BackgroundTasks,
    family: Family = Body(...),
    user: User = Depends(get_current_user),
):
    return await delete_asset(
        table="info.families",
        asset=family,
        pkeys=["family"],
        user=user,
        background_tasks=background_tasks,
    )
