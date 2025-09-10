"""Router for tags."""

from fastapi import (
    APIRouter,
    Body,
    Path,
    status,
    Depends,
    BackgroundTasks,
    HTTPException,
)
from uglyData.api.models import (
    TagBase,
    Tag,
    TagInstrument,
    TagStrategy,
    TagCustomInstruments,
    TagProduct,
    User,
)
from ..auth import get_current_user
from ..dependencies import (
    post_asset,
    put_asset,
    get_asset,
    get_all_assets,
    limit_params,
    delete_asset,
    write_log,
)
from ..service import DB

router = APIRouter(prefix="/tags", tags=["tags"])


def _prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("tag", 3),
        ("description", 1),
        ("products", 1),
        ("instruments", 1),
        ("strategy_filters", 1),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_tags(
    user: User = Depends(get_current_user),  # noqa: B008
    params=Depends(limit_params),  # noqa: B008
) -> list[Tag]:
    """Get all tags in the database."""
    params, filters = _prepare_params(params)

    return await get_all_assets(
        table="info.tags_view",
        auth_table="info.tags",
        filters=filters,
        user=user,
        **params,
    )


@router.get("/count")
async def get_count(
    user: User = Depends(get_current_user),  # noqa: B008
    params=Depends(limit_params),  # noqa: B008
):
    """Return the number of tags."""
    params, filters = _prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.tags_view",
        auth_table="info.tags",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{name}")
async def get_tag(
    name: str = Path(description="Name of the tag"),
    user: User = Depends(get_current_user),  # noqa: B008
) -> Tag:
    """Get a tag from the database."""
    return await get_asset(
        table="info.tags_view",
        auth_table="info.tags",
        values={"tag": name},
        user=user,
    )


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_tag(
    background_tasks: BackgroundTasks,
    tag: Tag = Body(...),  # noqa: B008
    user: User = Depends(get_current_user),  # noqa: B008
) -> Tag:
    """Add a tag to the database."""
    async with DB.conn.transaction():
        base_tag = TagBase(
            tag=tag.tag,
        )

        await post_asset(table="info.tags", user=user, asset=base_tag, enable_log=False)

        if tag.instruments:
            instrs = [
                TagInstrument(tag=tag.tag, instrument=inst).model_dump()
                for inst in tag.instruments
            ]

            await post_asset(
                table="info.tag_instruments",
                auth_table="info.tags",
                user=user,
                asset=instrs,
                enable_log=False,
            )

        if tag.strategy_filters:
            strat_filters = [
                TagStrategy(tag=tag.tag, strategy_filter=strat_filter).model_dump()
                for strat_filter in tag.strategy_filters
            ]

            await post_asset(
                table="info.tag_strategy_filters",
                auth_table="info.tags",
                user=user,
                asset=strat_filters,
                enable_log=False,
            )

        if tag.custom_instrument_filters:
            cis_filters = [
                TagCustomInstruments(
                    tag=tag.tag, custom_instrument_filter=cis_filter
                ).model_dump()
                for cis_filter in tag.custom_instrument_filters
            ]

            await post_asset(
                table="info.tag_custom_filters",
                auth_table="info.tags",
                user=user,
                asset=cis_filters,
                enable_log=False,
            )

        if tag.products:
            prods = [
                TagProduct(
                    tag=tag.tag,
                    product=prod_prodtype["product"],
                    product_type=prod_prodtype["product_type"],
                ).model_dump()
                for prod_prodtype in tag.products
            ]

            await post_asset(
                table="info.tag_products",
                auth_table="info.tags",
                user=user,
                asset=prods,
                enable_log=False,
            )

    background_tasks.add_task(write_log, user, "info.tags", "CREATE", None, tag)

    return tag


@router.put("")
@router.put("/", status_code=status.HTTP_200_OK)
async def update_tag(
    background_tasks: BackgroundTasks,
    tag: Tag = Body(...),  # noqa: B008
    user: User = Depends(get_current_user),  # noqa: B008
) -> Tag:
    """Update a tag in the database."""
    async with DB.conn.transaction():
        base_tag = TagBase(
            tag=tag.tag,
        )

        if tag.instruments:
            instrs = [
                TagInstrument(tag=tag.tag, instrument=inst).model_dump()
                for inst in tag.instruments
            ]

            old_instrs = await put_asset(
                table="info.tag_instruments",
                auth_table="info.tags",
                user=user,
                asset=instrs,
                pkeys=["tag"],
                enable_log=False,
            )
        else:
            try:
                old_instrs = await delete_asset(
                    table="info.tag_instruments",
                    auth_table="info.tags",
                    user=user,
                    asset=tag,
                    pkeys=["tag"],
                    enable_log=False,
                )
                if not isinstance(old_instrs, list):
                    old_instrs = [old_instrs]
            except HTTPException:
                # By the default delete_asset raises an exception if the asset is not
                # found but in this case we don't care if the asset is not found because
                # tag instruments can not exist in the tag instruments table when instruments = null
                old_instrs = None

        if tag.strategy_filters:
            strat_filters = [
                TagStrategy(tag=tag.tag, strategy_filter=strat_filter).model_dump()
                for strat_filter in tag.strategy_filters
            ]

            old_strat_filters = await put_asset(
                table="info.tag_strategy_filters",
                auth_table="info.tags",
                user=user,
                asset=strat_filters,
                pkeys=["tag"],
                enable_log=False,
            )
        else:
            try:
                old_strat_filters = await delete_asset(
                    table="info.tag_strategy_filters",
                    auth_table="info.tags",
                    user=user,
                    asset=tag,
                    pkeys=["tag"],
                    enable_log=False,
                )
                if not isinstance(old_strat_filters, list):
                    old_strat_filters = [old_strat_filters]
            except HTTPException:
                # By the default delete_asset raises an exception if the asset is not
                # found but in this case we don't care if the asset is not found
                old_strat_filters = None

        if tag.custom_instrument_filters:
            cis_filters = [
                TagCustomInstruments(
                    tag=tag.tag, custom_instrument_filter=cis_filter
                ).model_dump()
                for cis_filter in tag.custom_instrument_filters
            ]

            old_cis_filters = await put_asset(
                table="info.tag_custom_filters",
                auth_table="info.tags",
                user=user,
                asset=cis_filters,
                pkeys=["tag"],
                enable_log=False,
            )
        else:
            try:
                old_cis_filters = await delete_asset(
                    table="info.tag_custom_filters",
                    auth_table="info.tags",
                    user=user,
                    asset=tag,
                    pkeys=["tag"],
                    enable_log=False,
                )
                if not isinstance(old_cis_filters, list):
                    old_cis_filters = [old_cis_filters]
            except HTTPException:
                # By the default delete_asset raises an exception if the asset is not
                # found but in this case we don't care if the asset is not found
                old_cis_filters = None

        if tag.products:
            prods = [
                TagProduct(
                    tag=tag.tag,
                    product=prod_prodtype["product"],
                    product_type=prod_prodtype["product_type"],
                ).model_dump()
                for prod_prodtype in tag.products
            ]

            old_prods = await put_asset(
                table="info.tag_products",
                auth_table="info.tags",
                user=user,
                asset=prods,
                pkeys=["tag"],
                enable_log=False,
            )
        else:
            try:
                old_prods = await delete_asset(
                    table="info.tag_products",
                    auth_table="info.tags",
                    user=user,
                    asset=tag,
                    pkeys=["tag"],
                    enable_log=False,
                )
                if not isinstance(old_prods, list):
                    old_prods = [old_prods]
            except HTTPException:
                # By the default delete_asset raises an exception if the asset is not
                # found but in this case we don't care if the asset is not found
                old_prods = None

        old_tag = Tag(
            tag=base_tag.tag,
            products=[{k: v for k, v in pr.items() if k != "tag"} for pr in old_prods]
            if old_prods
            else None,
            instruments=[ti["instrument"] for ti in old_instrs] if old_instrs else None,
            strategy_filters=[sf["strategy_filter"] for sf in old_strat_filters]
            if old_strat_filters
            else None,
        )

        background_tasks.add_task(write_log, user, "info.tags", "UPDATE", old_tag, tag)
    return tag


@router.delete("")
@router.delete("/")
async def delete_driver(
    background_tasks: BackgroundTasks,
    tag: Tag = Body(...),  # noqa: B008
    user: User = Depends(get_current_user),  # noqa: B008
):
    """Delete a tag from the db."""
    async with DB.conn.transaction():
        if tag.instruments:
            instrs = [
                TagInstrument(tag=tag.tag, instrument=inst).model_dump()
                for inst in tag.instruments
            ]

            await delete_asset(
                table="info.tag_instruments",
                auth_table="info.tags",
                user=user,
                asset=instrs,
                pkeys=["tag"],
                enable_log=False,
            )

        if tag.strategy_filters:
            strat_filters = [
                TagStrategy(tag=tag.tag, strategy_filter=strat_filter).model_dump()
                for strat_filter in tag.strategy_filters
            ]

            await delete_asset(
                table="info.tag_strategy_filters",
                auth_table="info.tags",
                user=user,
                asset=strat_filters,
                pkeys=["tag"],
                enable_log=False,
            )

        if tag.custom_instrument_filters:
            cis_filters = [
                TagCustomInstruments(
                    tag=tag.tag, custom_instrument_filter=cis_filter
                ).model_dump()
                for cis_filter in tag.custom_instrument_filters
            ]

            await delete_asset(
                table="info.tag_custom_filters",
                auth_table="info.tags",
                user=user,
                asset=cis_filters,
                pkeys=["tag"],
                enable_log=False,
            )

        if tag.products:
            prods = [
                TagProduct(
                    tag=tag.tag,
                    product=prod_prodtype["product"],
                    product_type=prod_prodtype["product_type"],
                ).model_dump()
                for prod_prodtype in tag.products
            ]

            await delete_asset(
                table="info.tag_products",
                auth_table="info.tags",
                user=user,
                asset=prods,
                pkeys=["tag"],
                enable_log=False,
            )

        await delete_asset(
            table="info.tags",
            user=user,
            asset=tag,
            pkeys=["tag"],
            enable_log=False,
        )

    background_tasks.add_task(write_log, user, "info.tags", "DELETE", tag, None)
    return tag
