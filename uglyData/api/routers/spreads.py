"""Spread api routers."""

# ruff: noqa: B008
from uglyData.api.models import CompleteSpread, Spread, User
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Path,
    status,
)

from ..auth import get_current_user
from ..dependencies import (
    delete_asset,
    get_all_assets,
    get_asset,
    limit_params,
    post_asset,
    write_log,
)
from ..service import DB

router = APIRouter(prefix="/spreads", tags=["spreads"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("arfima_name", 3),
        ("violet_name", 1),
        ("auto_scalper_name", 1),
        ("violet_display_name", 1),
        ("violet_portfolio_name", 1),
        ("master_portfolio_name", 1),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_spreads(
    user: User = Depends(get_current_user),
    params=Depends(limit_params),
) -> list[CompleteSpread]:
    """Get all spreads in the database."""
    params, filters = prepare_params(params)

    return await get_all_assets(
        table="info.spreads_view",
        auth_table="info.spreads",
        filters=filters,
        user=user,
        **params,
    )


@router.get("/count")
async def get_count(
    user: User = Depends(get_current_user),
    params=Depends(limit_params),
):
    params, filters = prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.spreads_view",
        auth_table="info.spreads",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{name}")
async def get_spread(
    name: str = Path(description="Name of the spread"),
    user: User = Depends(get_current_user),
) -> CompleteSpread:
    """Get a spread from the database."""
    return await get_asset(
        table="info.spreads_view",
        auth_table="info.spreads",
        values={"arfima_name": name},
        user=user,
    )


async def check_spread_legs(spread: Spread):
    legs = spread.legs
    legs_instr = [leg["instrument"] for leg in legs]
    dtype = spread.dtype
    sql = f"""
        SELECT instrument
        FROM info.instruments_etal
        WHERE dtype = '{dtype}' and instrument  
        in ({",".join([f"'{instr}'" for instr in legs_instr])})
    """
    data = [instr[0] for instr in await DB.conn.fetch(sql)]
    not_found = [instr for instr in legs_instr if instr not in data]
    if len(data) != len(legs_instr):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Instrument(s) not found for dtype '{dtype}': "
            + ", ".join(not_found),
        )


async def insert_spread(spread: dict, user: User):
    executions = spread.pop("executions")

    await post_asset(table="info.spreads", user=user, asset=spread, enable_log=False)

    for exec in executions:
        legs = exec.pop("legs")
        if "execution_id" in exec and exec["execution_id"] is None:
            exec.pop("execution_id")
        exec = await post_asset(
            table="info.spreads_executions",
            auth_table="info.spreads",
            user=user,
            asset=exec,
            enable_log=False,
        )
        for leg in legs:
            leg["execution_id"] = exec["execution_id"]

        await post_asset(
            table="info.spreads_legs",
            auth_table="info.spreads",
            user=user,
            asset=legs,
            enable_log=False,
        )


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_spread(
    background_tasks: BackgroundTasks,
    spread: Spread = Body(...),
    user: User = Depends(get_current_user),
) -> Spread:
    """Add a spread to the database."""
    spread_dict = spread.model_dump(exclude_unset=True)
    async with DB.conn.transaction():
        await insert_spread(spread_dict, user=user)

    background_tasks.add_task(write_log, user, "info.spreads", "CREATE", None, spread)
    return spread


@router.put("")
@router.put("/", status_code=status.HTTP_200_OK)
async def update_spread(
    background_tasks: BackgroundTasks,
    spread: Spread = Body(...),
    user: User = Depends(get_current_user),
) -> Spread:
    """Update a spread in the database."""
    spread_dict = spread.model_dump(exclude_unset=False)

    async with DB.conn.transaction():
        old_spread = await get_asset(
            table="info.spreads_view",
            auth_table="info.spreads",
            values={"arfima_name": spread.arfima_name},
        )

        await delete_asset(
            table="info.spreads",
            auth_table="info.spreads",
            user=user,
            asset=spread_dict,
            pkeys=["arfima_name"],
            enable_log=False,
        )
        await insert_spread(spread_dict, user=user)

    background_tasks.add_task(
        write_log, user, "info.spreads", "UPDATE", old_spread, spread
    )
    return spread


@router.delete("")
@router.delete("/")
async def delete_spread(
    background_tasks: BackgroundTasks,
    spread: Spread = Body(...),
    user: User = Depends(get_current_user),
):
    spread_dict = spread.model_dump(exclude_unset=True)

    await delete_asset(
        table="info.spreads",
        user=user,
        asset=spread_dict,
        pkeys=["arfima_name"],
        enable_log=True,
        background_tasks=background_tasks,
    )

    return spread
