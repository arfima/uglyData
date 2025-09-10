from fastapi import (
    APIRouter,
    Body,
    Query,
    Path,
    Depends,
    BackgroundTasks,
)
from ..dependencies import (
    get_all_assets,
    get_asset,
    post_asset,
    put_asset,
    limit_params,
    delete_asset,
)
from uglyData.api.models import (
    Instrument,
    User,
    CompleteInstrument,
    InstrumentDeliverable,
    CheapestDeliverable,
)
from ..auth import get_current_user
from typing import Annotated, List


router = APIRouter(prefix="/instruments", tags=["instruments"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("instrument", 3),
        ("product", 2),
        ("product_type", 1),
        ("refinitiv_ticker", 1),
        ("bloomberg_ticker", 1),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


def market_data_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("instrument", 5),
        ("dtype", 4),
        ("start", 1),
        ("end", 1),
        ("product", 4),
        ("product_type", 4),
        ("description", 4),
        ("exchange", 4),
        ("family", 4),
        ("subfamily", 4),
    ]

    filters = {k: v for k, v in kwargs.items() if v}
    return params, filters


def categories(
    product: str = Query(None, description="Name of the product"),
    product_type: str = Query(None, description="Type of the product"),
) -> dict:
    return {
        "product": product,
        "product_type": product_type,
    }


def market_data_categories(
    product: list[str] | str = Query(None, description="Name of the product"),
    product_type: list[str] | str = Query(None, description="Type of the product"),
    exchange: list[str] | str = Query(None, description="Exchange of the product"),
    family: list[str] | str = Query(None, description="Family of the product"),
    subfamily: list[str] | str = Query(None, description="Subfamily of the product"),
    dtype: list[str] | str = Query(None, description="Data type of the instrument"),
) -> dict:
    return {
        "product": product,
        "product_type": product_type,
        "exchange": exchange,
        "family": family,
        "subfamily": subfamily,
        "dtype": dtype,
    }


@router.get("")
@router.get("/")
async def get_all_instruments(
    params: list = Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(categories),
) -> list[CompleteInstrument]:
    """Get all instruments in the database."""

    params, filters = prepare_params(params, **categories)
    return await get_all_assets(
        table="info.complete_instruments",
        auth_table="info.instruments",
        filters=filters,
        user=user,
        **params,
    )


@router.get("/list/")
async def get_instruments(
    instrument: Annotated[list[str] | None, Query()],
    params: list = Depends(limit_params),
    user: User = Depends(get_current_user),
) -> list[CompleteInstrument]:
    """Get all instruments in the database."""
    return await get_all_assets(
        table="info.complete_instruments",
        auth_table="info.instruments",
        filters={"instrument": instrument},
        user=user,
    )


@router.get("/all")
async def get_all_instruments_etal(
    params: dict = Depends(limit_params),
    user: User = Depends(get_current_user),
):
    """Get all instruments in the database + drivers."""

    params["search_columns"] = ["instrument"]

    return await get_all_assets(
        table="info.instruments_etal",
        auth_table="info.instruments",
        user=user,
        **params,
    )


@router.get("/marketData")
async def get_all_instruments_market_data(
    params: dict = Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(market_data_categories),
):
    """Get all instruments in the database."""

    params, filters = market_data_params(params, **categories)

    return await get_all_assets(
        table="primarydata.instruments_info",
        auth_table="info.instruments",
        user=user,
        filters=filters,
        **params,
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
        table="info.instruments",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/marketData/count")
async def get_count_market_data(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(market_data_categories),
):
    params, filters = market_data_params(params, **categories)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="primarydata.instruments_info",
        auth_table="info.instruments",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{instrument}")
async def get_instrument(
    instrument: str = Path(description="Name of the instrument"),
) -> CompleteInstrument:
    """Get information about a single instrument."""
    return await get_asset(
        table="info.complete_instruments",
        auth_table="info.instruments",
        values={"instrument": instrument},
    )


@router.post("", status_code=201)
@router.post("/", status_code=201)
async def add_instrument(
    background_tasks: BackgroundTasks,
    instrument: Instrument = Body(...),
    user: User = Depends(get_current_user),
):
    """Add a instrument to the database"""
    asset = await post_asset(
        table="info.instruments",
        asset=instrument,
        user=user,
        background_tasks=background_tasks,
    )
    return asset


@router.put("")
@router.put("/")
async def update_instrument(
    background_tasks: BackgroundTasks,
    instrument: Instrument = Body(...),
    user: User = Depends(get_current_user),
):
    """Update a instrument in the database"""
    return await put_asset(
        table="info.instruments",
        asset=instrument,
        pkeys="instrument",
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_instrument(
    background_tasks: BackgroundTasks,
    instrument: Instrument = Body(...),
    user: User = Depends(get_current_user),
):
    return await delete_asset(
        table="info.instruments",
        asset=instrument,
        pkeys=["instrument"],
        user=user,
        background_tasks=background_tasks,
    )


@router.get("/dtype/{dtype}")
async def get_all_instruments_etal_by_dtype(
    dtype: str = Path(),
    params: dict = Depends(limit_params),
    user: User = Depends(get_current_user),
):
    """Get all instruments in the database."""

    params["search_columns"] = ["instrument"]

    return await get_all_assets(
        table="info.instruments_etal",
        auth_table="info.instruments",
        filters={"dtype": dtype},
        user=user,
        **params,
    )


@router.get("/deliverables/")
async def get_deliverables_for_instrument(
    instrument: Annotated[list[str] | None, Query()],
    params: dict = Depends(limit_params),
    user: User = Depends(get_current_user),
):
    """Get all deliverables for a specific instrument."""

    return await get_all_assets(
        table="info.deliverables",
        auth_table="info.instruments",
        filters={"instrument": instrument},
        user=user,
    )


@router.get("/last_cheapest/")
async def get_cheapest_deliverable_for_instrument(
    instrument: Annotated[list[str] | None, Query()],
    user: User = Depends(get_current_user),
    params: dict = Depends(limit_params),
) -> List[InstrumentDeliverable]:
    return await get_all_assets(
        table="info.instrument_last_cheapest",
        auth_table="info.instruments",
        filters={"instrument": instrument},
        user=user,
    )


@router.get("/cheapest_fixed/")
async def get_cheapest_deliverable_for_instrument(
    instrument: Annotated[list[str] | None, Query()],
    user: User = Depends(get_current_user),
    params: dict = Depends(limit_params),
) -> List[InstrumentDeliverable]:
    return await get_all_assets(
        table="info.instrument_cheapest_fixed",
        auth_table="info.instruments",
        filters={"instrument": instrument},
        user=user,
    )


@router.get("/cheapests/")
async def get_cheapest_deliverable_for_instrument(
    instrument: Annotated[list[str] | None, Query()],
    user: User = Depends(get_current_user),
    params: dict = Depends(limit_params),
) -> List[CheapestDeliverable]:
    return await get_all_assets(
        table="primarydata.base_cheapest",
        auth_table="info.instruments",
        filters={"instrument": instrument},
        user=user,
    )
