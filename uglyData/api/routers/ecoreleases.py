"""Endpoints concerning the eco realeses."""

from fastapi import (
    APIRouter,
    Path,
    Depends,
)
from ..dependencies import (
    get_all_assets,
    get_asset,
    limit_params,
)
from uglyData.api.models import User, EcoRelease
from ..auth import get_current_user


router = APIRouter(prefix="/ecoreleases", tags=["ecoreleases"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("instrument", 3),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_ecoreleases(
    params: dict = Depends(limit_params),
    user: User = Depends(get_current_user),
) -> list[EcoRelease]:
    """Get all ecoreleases in the database."""
    params, filters = prepare_params(params)
    return await get_all_assets(
        table="info.ecoreleases",
        auth_table="info.instruments",
        filters=filters,
        user=user,
        **params,
    )


@router.get("/{ecorelease}")
async def get_ecorelease(
    ecorelease: str = Path(description="Ticker of the ecorelease"),
) -> EcoRelease:
    """Get information about a single ecorelease."""
    return await get_asset(
        table="info.ecoreleases",
        auth_table="info.instruments",
        values={"instrument": ecorelease},
    )
