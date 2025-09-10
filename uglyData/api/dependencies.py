import pandas as pd
from fastapi import Query, HTTPException, BackgroundTasks
from pydantic import BaseModel
from pydantic import Json
from .service import DB
from .exceptions import (
    AssetNotFound,
    AssetAlreadyExists,
    FieldNotValid,
    ForeignKeyViolationError,
)
from uglyData.api.models import AccessLevel, User
from contextlib import contextmanager
import decimal
import orjson

DEFAULT_LIMIT = 1000
MIN_LIMIT = 1000
MAX_LIMIT = 100000

START_TIMESTAMP_DESCR = (
    "Start timestamp, can be a string date or a unix timestamp (UTC) in milliseconds"
)
END_TIMESTAMP_DESCR = START_TIMESTAMP_DESCR.replace("Start", "End")
LIMIT_TICKS_DESCR = "Maximum number of ticks to return"
LIMIT_BARS_DESCR = LIMIT_TICKS_DESCR.replace("ticks", "bars")


async def ticks_parameters(
    instrument: str = Query(description="Name of the instrument"),
    start: str | int = Query(description=START_TIMESTAMP_DESCR, default=None),
    end: str | int = Query(default=None, description=END_TIMESTAMP_DESCR),
    limit: int = Query(
        default=DEFAULT_LIMIT, ge=MIN_LIMIT, le=MAX_LIMIT, description=LIMIT_TICKS_DESCR
    ),
):
    return {
        "instrument": instrument,
        "start": start,
        "end": end,
        "limit": limit,
    }


async def audit_parameters(
    start: str | int = Query(description=START_TIMESTAMP_DESCR, default=None),
    end: str | int = Query(default=None, description=END_TIMESTAMP_DESCR),
    req: Json = Query(description="Request of the audit trail"),
):
    return {"start": start, "end": end, "req": req}


async def bars_parameters(
    instrument: str = Query(description="Name of the instrument"),
    start: str | int = Query(default=None, description=START_TIMESTAMP_DESCR),
    end: str | int = Query(default=None, description=END_TIMESTAMP_DESCR),
    freq: str = Query(default=None, description="Bar interval (5s, 1m, 1h, 1d)"),
    limit: int = Query(
        default=DEFAULT_LIMIT, ge=MIN_LIMIT, le=MAX_LIMIT, description=LIMIT_TICKS_DESCR
    ),
):
    return {
        "instrument": instrument,
        "start": start,
        "end": end,
        "freq": freq,
        "limit": limit,
    }


def limit_params(
    limit: int = Query(
        default=500, description="Limit the number of results.", ge=1, le=10000
    ),
    page: int = Query(default=1, description="page the results.", ge=1),
    searchQuery: str = Query(default=None, description="Search query"),
    sorting: list[str] = Query(default=None, description="Sorting"),
) -> dict:
    return {
        "limit": limit,
        "offset": limit * (page - 1),
        "search_query": searchQuery,
        "sorting": sorting,
    }


def check_permissions(user: User, table: str, level: AccessLevel):
    """Check if the user has the required permission."""
    asset_name = table.split(".")[-1]
    if getattr(user, asset_name) < level:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to access this resource.",
        )


@contextmanager
def request_handler(
    user: str,
    table: str,
    access_level: AccessLevel,
    asset=None,
    auth_table: str = None,
):
    if user:
        check_permissions(user, auth_table if auth_table else table, access_level)
    try:
        yield
    except (FieldNotValid, ForeignKeyViolationError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except AssetNotFound:
        raise HTTPException(status_code=404, detail="Item not found")
    except AssetAlreadyExists:
        raise HTTPException(status_code=409, detail="Item already exists")
    except Exception as e:
        if asset and isinstance(asset, BaseModel):
            asset_ref = "with " + asset.__class__.__name__
        else:
            asset_ref = ""
        error_cls = e.__class__.__name__
        error = f"Error {asset_ref}: {error_cls}({e})"
        raise HTTPException(status_code=500, detail=error)


async def post_asset(
    table: str,
    asset: dict | BaseModel,
    user: User,
    auth_table: str = None,
    enable_log=True,
    background_tasks: BackgroundTasks = None,
    discard_duplicates: bool = False,
):
    """Add an asset to the database."""
    with request_handler(
        user=user,
        table=table,
        access_level=AccessLevel.WRITE,
        asset=asset,
        auth_table=auth_table,
    ):
        if isinstance(asset, BaseModel):
            asset = asset.model_dump()
        added = await DB.add_asset(
            asset=asset, table=table, discard_duplicates=discard_duplicates
        )
        if enable_log:
            if background_tasks is None:
                raise ValueError("background_tasks must be provided")
            background_tasks.add_task(write_log, user, table, "CREATE", None, asset)
        return added


async def put_asset(
    table: str,
    asset,
    pkeys: str | list[str],
    user: User,
    auth_table: str = None,
    enable_log=True,
    background_tasks: BackgroundTasks = None,
):
    """Add an asset to the database."""
    with request_handler(
        user=user,
        table=table,
        access_level=AccessLevel.WRITE,
        asset=asset,
        auth_table=auth_table,
    ):
        if isinstance(asset, BaseModel):
            asset = asset.model_dump()
        old_asset = await DB.update_asset(asset=asset, table=table, pkeys=pkeys)
        if enable_log:
            if background_tasks is None:
                raise ValueError("background_tasks must be provided")
            background_tasks.add_task(
                write_log, user, table, "UPDATE", old_asset, asset
            )
        return asset


async def delete_asset(
    table: str,
    asset,
    user: User,
    pkeys: str | list[str],
    auth_table: str = None,
    enable_log=True,
    background_tasks: BackgroundTasks = None,
):
    with request_handler(
        user=user,
        table=table,
        access_level=AccessLevel.ADMIN,
        asset=asset,
        auth_table=auth_table,
    ):
        if isinstance(asset, BaseModel):
            asset = asset.model_dump()
        deleted = await DB.delete_asset(asset=asset, table=table, pkeys=pkeys)
        if enable_log:
            if background_tasks is None:
                raise ValueError("background_tasks must be provided")
            background_tasks.add_task(write_log, user, table, "DELETE", asset, None)
        return deleted if len(deleted) > 1 else deleted[0]


async def get_asset(
    table: str, values: dict, user: User = None, auth_table: str = None
):
    """Get an asset from the database.

    Parameters
    ----------
    table: str
        Name of the db table to get from.
    values: dict[str,Any]
        Dictionary with columns names as keys and the expected value as values.
    user: User, optional
        User object for authentication. Default is None.
    auth_table: str, optional
        Name of the table to use for checking authentication. When None, request_handler uses table.

    Returns
    -------
    dict
        The result of the SQL query as a dictionary. A single row of the table.

    """
    with request_handler(
        user=user,
        table=table,
        access_level=AccessLevel.READ,
        asset=values,
        auth_table=auth_table,
    ):
        return await DB.get_asset(table=table, values=values)


async def get_all_assets(
    table: str,
    filters: dict = None,
    limit: int = None,
    offset: int = None,
    search_query: str = None,
    sorting: list[str] = None,
    user: User = None,
    auth_table: str = None,
    *args,
    **kwargs,
):
    """Get all assets from the database.

    Parameters
    ----------
    table: str
        Name of the db table to get from.
    filters: dict[str,Any], optional
        Dictionary with columns names as keys and the expected value as values.
    limit: int, optional
        Limit to be applied to the sql query. f"LIMIT {offset}" will be added to the query.
    offset: int, optional
        Offset to be applied to the sql query. f"OFFSET {offset}" will be added to the query.
    search_query: str, optional
        String that will be compared to all columns cited in search_column:list[str] (search_column should be added as an argument of the function).
        "WHERE" + [f"and {col_name} LIKE {search_query}" for col in search_column]
    sorting: list[str], optional
        List of sorting conditions to be applied in order. Each of format "col_name:order" with order in ["asc","desc"],
        "ORDER bY" + [f"{col_name} {order}," for col,order in sorting.split(':')] will be added to the query.
    user: User, optional
        User object for authentication. Default is None.
    auth_table: str, optional
        Name of the table to use for checking authentication. When None, request_handler uses table.

    Returns
    -------
    list[dict]
        The result of the SQL query as a dictionary. A list of dictionaries, each representing a row in the db.

    """
    with request_handler(
        user=user,
        table=table,
        access_level=AccessLevel.READ,
        asset=filters,
        auth_table=auth_table,
    ):
        return await DB.get_all_assets(
            table=table,
            filters=filters,
            limit=limit,
            offset=offset,
            search_query=search_query,
            sorting=sorting,
            *args,
            **kwargs,
        )


def default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError


def parse_json(data: BaseModel | dict) -> str:
    """Parse a json object."""
    if isinstance(data, BaseModel):
        data = data.model_dump()
    return orjson.dumps(data, default=default).decode("utf-8")


async def write_log(
    user: User, table: str, action: str, old_data: dict, new_data: dict
):
    """Write a log entry."""

    values = {
        "dtime": str(pd.Timestamp.now()),
        "action_type": action,
        "user_name": user.username if user else None,
        "schema_name": table.split(".")[0],
        "table_name": table.split(".")[1],
        "old_data": parse_json(old_data) if old_data else None,
        "new_data": parse_json(new_data) if new_data else None,
    }

    columns, values = map(tuple, zip(*values.items()))

    await DB.conn.insert(table="info.frontend_log", columns=columns, values=values)
