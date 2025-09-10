from fastapi import (
    APIRouter,
    Body,
    Path,
    status,
    Depends,
    BackgroundTasks,
    HTTPException,
    Query,
)
from uglyData.api.models import Driver, User, DriverLeg, BaseDriver
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

router = APIRouter(prefix="/drivers", tags=["drivers"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("driver", 3),
        ("description", 1),
        ("tags", 1),
        ("legs", 1),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


def drivers_categories(
    tags: list[str] = Query(None, description="Tags to filter by"),
    store: bool = Query(None, description="Filter by store"),
) -> dict:
    return {
        "tags": tags,
        "store": store,
    }


@router.get("")
@router.get("/")
async def get_all_drivers(
    user: User = Depends(get_current_user),
    params=Depends(limit_params),
    categories: dict = Depends(drivers_categories),
) -> list[Driver]:
    """Get all drivers in the database."""

    params, filters = prepare_params(params, **categories)

    return await get_all_assets(
        table="info.drivers_view",
        auth_table="info.drivers",
        filters=filters,
        user=user,
        **params,
    )


@router.get("/count")
async def get_count(
    user: User = Depends(get_current_user),
    params=Depends(limit_params),
    categories: dict = Depends(drivers_categories),
):
    params, filters = prepare_params(params, **categories)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.drivers_view",
        auth_table="info.drivers",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{name}")
async def get_driver(
    name: str = Path(description="Name of the driver"),
    user: User = Depends(get_current_user),
) -> Driver:
    """Get a driver from the database."""
    return await get_asset(
        table="info.drivers_view",
        auth_table="info.drivers",
        values={"driver": name},
        user=user,
    )


async def check_driver_legs(driver: Driver):
    legs = driver.legs
    legs_instr = [leg["instrument"] for leg in legs]
    dtype = driver.dtype
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


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_driver(
    background_tasks: BackgroundTasks,
    driver: Driver = Body(...),
    user: User = Depends(get_current_user),
) -> Driver:
    """Add a driver to the database."""
    if driver.legs:
        await check_driver_legs(driver)

    async with DB.conn.transaction():
        base_driver = BaseDriver(
            driver=driver.driver,
            dtype=driver.dtype,
            description=driver.description,
            tags=driver.tags,
        )
        await post_asset(
            table="info.drivers", user=user, asset=base_driver, enable_log=False
        )

        if driver.legs:
            legs = [
                DriverLeg(driver=driver.driver, **leg).model_dump()
                for leg in driver.legs
            ]
            await post_asset(
                table="info.drivers_legs",
                auth_table="info.drivers",
                user=user,
                asset=legs,
                enable_log=False,
            )

    background_tasks.add_task(write_log, user, "info.drivers", "CREATE", None, driver)

    return driver


@router.put("")
@router.put("/", status_code=status.HTTP_200_OK)
async def update_driver(
    background_tasks: BackgroundTasks,
    driver: Driver = Body(...),
    user: User = Depends(get_current_user),
) -> Driver:
    """Update a driver in the database."""
    if driver.legs:
        await check_driver_legs(driver)

    async with DB.conn.transaction():
        base_driver = BaseDriver(
            driver=driver.driver,
            dtype=driver.dtype,
            description=driver.description,
            tags=driver.tags,
        )
        old_driver = await put_asset(
            table="info.drivers",
            user=user,
            asset=base_driver,
            pkeys=["driver"],
            enable_log=False,
        )

        if driver.legs:
            legs = [
                DriverLeg(driver=driver.driver, **leg).model_dump()
                for leg in driver.legs
            ]
            old_legs = await put_asset(
                table="info.drivers_legs",
                auth_table="info.drivers",
                user=user,
                asset=legs,
                pkeys=["driver"],
                enable_log=False,
            )

        else:
            try:
                old_legs = await delete_asset(
                    table="info.drivers_legs",
                    auth_table="info.drivers",
                    user=user,
                    asset=driver,
                    pkeys=["driver"],
                    enable_log=False,
                )
            except HTTPException:
                # By the default delete_asset raises an exception if the asset is not
                # found but in this case we don't care if the asset is not found because
                # driver legs can not exist in the legs table when legs = null
                old_legs = None
        old_driver = Driver(**old_driver, legs=old_legs)

        background_tasks.add_task(
            write_log, user, "info.drivers", "UPDATE", old_driver, driver
        )
    return driver


@router.delete("")
@router.delete("/")
async def delete_driver(
    background_tasks: BackgroundTasks,
    driver: Driver = Body(...),
    user: User = Depends(get_current_user),
):
    async with DB.conn.transaction():
        if driver.legs:
            legs = [
                DriverLeg(driver=driver.driver, **leg).model_dump()
                for leg in driver.legs
            ]
            await delete_asset(
                table="info.drivers_legs",
                auth_table="info.drivers",
                user=user,
                asset=legs,
                pkeys=["driver"],
                enable_log=False,
            )

        await delete_asset(
            table="info.drivers",
            user=user,
            asset=driver,
            pkeys=["driver"],
            enable_log=False,
        )

    background_tasks.add_task(write_log, user, "info.drivers", "DELETE", driver, None)
    return driver
