from fastapi import (
    APIRouter,
    Depends,
    Path,
    Query,
    status,
    Body,
    BackgroundTasks,
    HTTPException,
)

from uglyData.api.models import Column, User, LoadRequest
from ..dependencies import (
    get_all_assets,
    limit_params,
    get_asset,
    post_asset,
    delete_asset,
    put_asset,
)
from ..auth import get_current_user
from ..service import DB
from ..builders import BuilderFactory

router = APIRouter(prefix="/columns", tags=["columns"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        ("column_name", 5),
        ("description", 4),
        ("field_tt", 3),
        ("field_bloomberg", 3),
        ("field_refinitiv", 3),
        ("field_wb", 3),
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


TABLES = {
    "eod": "base_eod",
    "intrade": "base_intrade",
    "intraquote": "base_intraquote",
    "quotes": "refinitiv_t2tquote",
    "trades": "refinitiv_t2trade",
    "intraday": ["base_intrade", "base_intraquote"],
    "t2t": ["refinitiv_t2tquote", "refinitiv_t2trade"],
    "tt_audittrail": "tradingdata.tt_audittrail_view",
}


async def get_columns_by_dtype(dtype: str):
    """Return a list of columns for a given data type."""
    table = TABLES.get(dtype, None)
    if table is None:
        return HTTPException(status_code=404, detail=f"Data type '{dtype}' not found")

    if isinstance(table, list):
        cols = []
        for t in table:
            for col in await DB.conn.get_columns(t, schema="primarydata"):
                cols.append(col) if col not in cols else None

    else:
        cols = await DB.conn.get_columns(table, schema="primarydata")
    return cols


@router.get("")
@router.get("/")
async def get_all_columns(
    dtype: str = Query(None, description="Type of the column."),
    params=Depends(limit_params),
    user: User = None,
) -> list[Column]:  # temporal bypass to test everything. Depends(get_current_user),
    """Get all column descriptions in the database."""
    params, _ = prepare_params(params)
    if dtype:
        column_names = await get_columns_by_dtype(dtype)
        columns = {
            col["column_name"]: col
            for col in await get_all_assets(
                table="info.columns",
                filters={"column_name": column_names},
                user=user,
                **params,
            )
        }

        for col in column_names:
            if col not in columns:
                columns[col] = {"column_name": col}

        return sorted(
            columns.values(), key=lambda x: column_names.index(x["column_name"])
        )
    else:
        return await get_all_assets(table="info.columns", user=user, **params)


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
):
    params, filters = prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="info.columns",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{column_name}")
async def get_column(
    column_name: str = Path(description="Type of the column."),
    user: User = Depends(get_current_user),
) -> Column:
    """Get information about a single column."""
    return await get_asset(
        table="info.columns",
        values={"column_name": column_name},
        user=user,
    )


@router.get("/list/{dtype}")
async def get_columns_by_dtype_list(
    dtype: str = Path(description="Type of the column."),
    instrument: str = Query(None, description="Type of the column."),
):
    """Get information about a single column."""
    if instrument is None:
        columns = await get_columns_by_dtype(dtype)
    else:
        request = LoadRequest(ticker=instrument, dtype=dtype)
        builder = await BuilderFactory.get_builder(request=request, db=DB)
        columns = await builder.get_columns()
        if not columns:  # if some error happened return all the columns
            columns = await get_columns_by_dtype(dtype)
    return {"columns": columns}


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_column(
    background_tasks: BackgroundTasks,
    column: Column = Body(...),
    user: User = Depends(get_current_user),
) -> Column:
    """Add a column to the database."""
    return await post_asset(
        table="info.columns",
        asset=column,
        user=user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_column(
    background_tasks: BackgroundTasks,
    column: Column = Body(...),
    user: User = Depends(get_current_user),
) -> Column:
    """Update a column in the database."""
    return await put_asset(
        table="info.columns",
        asset=column,
        pkeys=["column_name"],
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_column(
    background_tasks: BackgroundTasks,
    column: Column = Body(...),
    user: User = Depends(get_current_user),
):
    return await delete_asset(
        table="info.columns",
        asset=column,
        pkeys=["column_name"],
        user=user,
        background_tasks=background_tasks,
    )
