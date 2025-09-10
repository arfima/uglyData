"""Product api routers."""

# ruff: noqa: B008
from typing import Annotated

from uglyData.api.models import CompleteProduct, Product, User
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    Path,
    Query,
    status,
)

from ..auth import get_current_user
from ..dependencies import (
    delete_asset,
    get_all_assets,
    get_asset,
    limit_params,
    post_asset,
    put_asset,
)
from ..service import DB

router = APIRouter(prefix="/products", tags=["products"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("product", 5),
        ("product_type", 4),
        ("description", 3),
        ("tt_ticker", 3),
        ("exchange_ticker", 3),
        ("bloomberg_ticker", 3),
        ("exchange", 3),
        ("family", 3),
        ("subfamily", 3),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


def product_categories(
    product: str = Query(None, description="Name of the product."),
    product_type: list[str] = Query(None, description="Type of the product."),
    family: list[str] = Query(None, description="Family of the product."),
    subfamily: list[str] = Query(None, description="Subfamily of the product."),
    exchange: list[str] = Query(None, description="Exchange of the product."),
) -> dict:
    return {
        "product": product,
        "product_type": product_type,
        "family": family,
        "subfamily": subfamily,
        "exchange": exchange,
    }


@router.get("")
@router.get("/")
async def get_all_products(
    params=Depends(limit_params),
    user: User = None,  # Depends(get_current_user),
    categories: dict = Depends(product_categories),
) -> list[CompleteProduct]:
    """Get all products in the database."""
    params, filters = prepare_params(params, **categories)

    return await get_all_assets(
        table="info.product_tags",
        auth_table="info.products",
        user=user,
        filters=filters,
        **params,
    )


@router.get("/list/")
async def get_products(
    product: Annotated[list[str] | None, Query()],
    params: list = Depends(limit_params),
    user: User = Depends(get_current_user),
) -> list[CompleteProduct]:
    """Get all instruments in the database."""
    return await get_all_assets(
        table="info.product_tags",
        auth_table="info.products",
        filters={"product": product},
        user=user,
    )


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(product_categories),
):
    params, filters = prepare_params(params, **categories)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.products",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/product_types")
async def get_product_types(
    user: User = Depends(get_current_user),
):
    """Get all products in the database."""
    values = await DB.get_categories(table="info.products", category="product_type")
    # return {"product_types": [val[0] for val in values]}
    return [{"product_type": val[0]} for val in values]


@router.get("/yield_types")
async def get_yield_types(
    user: User = Depends(get_current_user),
):
    """Return the possible yield_types."""
    values = await DB.get_enum(my_enum="info.yield_types")
    return [{"yield_type": val[0]} for val in values]


@router.get("/{product_type}/{product}")
async def get_product_info(
    product_type: str = Path(description="Type of the product."),
    product: str = Path(description="Name of the product."),
    # user: User = Depends(get_current_user),
) -> Product:
    """Get information about a single product."""
    return await get_asset(
        table="info.products",
        values={"product": product, "product_type": product_type},
        # user=user,
    )


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_product(
    background_tasks: BackgroundTasks,
    product: Product = Body(...),
    user: User = Depends(get_current_user),
) -> Product:
    """Add a product to the database."""
    return await post_asset(
        table="info.products",
        asset=product,
        user=user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_product(
    background_tasks: BackgroundTasks,
    product: Product = Body(...),
    user: User = Depends(get_current_user),
) -> Product:
    """Update a product in the database."""
    return await put_asset(
        table="info.products",
        asset=product,
        pkeys=["product", "product_type"],
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_product(
    background_tasks: BackgroundTasks,
    product: Product = Body(...),
    user: User = Depends(get_current_user),
):
    return await delete_asset(
        table="info.products",
        asset=product,
        pkeys=["product", "product_type"],
        user=user,
        background_tasks=background_tasks,
    )
