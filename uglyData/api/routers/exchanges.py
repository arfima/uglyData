from fastapi import (
    APIRouter,
    Body,
    Path,
    Depends,
    status,
    BackgroundTasks,
)

from uglyData.api.models import Exchange, User
from ..dependencies import (
    get_asset,
    get_all_assets,
    post_asset,
    put_asset,
    limit_params,
    delete_asset,
)
from ..auth import get_current_user

router = APIRouter(prefix="/exchanges", tags=["exchanges"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("mic", 3),
        ("arfimaname", 2),
        ("description", 1),
        ("url", 1),
        ("comment", 1),
        ("tt_ticker", 1),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_exchanges(
    params=Depends(limit_params), user: User = Depends(get_current_user)
) -> list[Exchange]:
    """Get all exchanges in the database."""
    params, filters = prepare_params(params)
    return await get_all_assets(table="info.exchanges", user=user, **params)


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
):
    params, filters = prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.exchanges",
        user=user,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{mic}")
async def get_exchange_info(
    mic: str = Path(description="MIC of the exchange."),
    user: User = Depends(get_current_user),
) -> Exchange:
    """Get information about a single exchange."""
    return await get_asset(table="info.exchanges", values={"mic": mic}, user=user)


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_exchange(
    background_tasks: BackgroundTasks,
    exchange: Exchange = Body(...),
    user: User = Depends(get_current_user),
) -> Exchange:
    """Add a exchange to the database."""
    return await post_asset(
        table="info.exchanges",
        asset=exchange,
        user=user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_exchange(
    background_tasks: BackgroundTasks,
    exchange: Exchange = Body(...),
    user: User = Depends(get_current_user),
) -> Exchange:
    """Update a exchange in the database."""
    return await put_asset(
        table="info.exchanges",
        asset=exchange,
        pkeys=["mic"],
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_exchange(
    background_tasks: BackgroundTasks,
    exchange: Exchange = Body(...),
    user: User = Depends(get_current_user),
):
    return await delete_asset(
        table="info.exchanges",
        asset=exchange,
        pkeys=["mic"],
        user=user,
        background_tasks=background_tasks,
    )
