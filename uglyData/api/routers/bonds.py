from fastapi import (
    APIRouter,
    Query,
    Path,
    Depends,
)
from ..dependencies import (
    get_all_assets,
    get_asset,
    limit_params,
)
from uglyData.api.models import User, Bond
from ..auth import get_current_user


router = APIRouter(prefix="/bonds", tags=["bonds"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("isin", 3),
        ("bond_name", 2),
        ("currency", 1),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


def categories(
    issuer: str = Query(None, description="Country issuing the bonds"),
    coupon_type: str = Query(None, description="Type of the coupon"),
) -> dict:
    return {
        "issuer": issuer,
        "coupon_type": coupon_type,
    }


@router.get("")
@router.get("/")
async def get_all_bonds(
    params: dict = Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(categories),
) -> list[Bond]:
    """Get all bonds in the database."""

    params, filters = prepare_params(params, **categories)
    return await get_all_assets(
        table="info.bonds",
        auth_table="info.instruments",
        filters=filters,
        user=user,
        **params,
    )


@router.get("/{bond}")
async def get_bond(
    bond: str = Path(description="ISIN of the bond"),
) -> Bond:
    """Get information about a single bond."""
    return await get_asset(
        table="info.bonds",
        auth_table="info.instruments",
        values={"isin": bond},
    )
