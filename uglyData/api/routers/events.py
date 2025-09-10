from fastapi import (
    APIRouter,
    Body,
    Query,
    Path,
    Depends,
    status,
    BackgroundTasks,
    WebSocket,
)
import json
from typing import Union

from uglyData.api.models import Event, EventID, LoadRequest, User
from ..dependencies import (
    get_asset,
    get_all_assets,
    post_asset,
    put_asset,
    delete_asset,
    limit_params,
)
from ..auth import get_current_user
from ..wsockets import handle_load_websocket

router = APIRouter(prefix="/events", tags=["events"])


def prepare_params(params: dict, **kwargs) -> dict:
    filters = {k: v for k, v in kwargs.items() if k != "oi" and v}

    if kwargs.get("oi", {}):
        filters[">>>"] = [
            {
                "column": "other_information",
                "field": k.lower().strip().replace(" ", "_"),
                "value": v,
            }
            for k, v in kwargs.get("oi", {}).items()
        ]

    return params, filters


def categories(
    event_category: Union[list[str], str] = Query(
        None, description="Category of the event"
    ),
    event_name: Union[list[str], str] = Query(None, description="Name of the event"),
    # tags: list[str] = Query(None, description="Tags of the event"),
    oi: str = Query(None, description="key values from other_information"),
) -> dict:
    return {
        "event_category": event_category,
        "event_name": event_name,
        "oi": json.loads(oi) if oi is not None else {},
    }


@router.get("")
@router.get("/")
async def get_all_events(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
    categories: dict = Depends(categories),
) -> list[EventID]:
    """Get all events in the database."""

    params, filters = prepare_params(params, **categories)
    return await get_all_assets(
        table="info.events", filters=filters, user=user, **params
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
        table="info.events",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{id}")
async def get_event_info(
    id: int = Path(description="ID of the event"),
    user: User = Depends(get_current_user),
) -> EventID:
    """Get information about a single event."""
    return await get_asset(table="info.events", values={"id": id}, user=user)


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_event(
    background_tasks: BackgroundTasks,
    event: Event = Body(...),
    user: User = Depends(get_current_user),
) -> Event:
    """Add a event to the database."""
    return await post_asset(
        table="info.events",
        asset=event,
        user=user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_event(
    background_tasks: BackgroundTasks,
    event: EventID = Body(...),
    user: User = Depends(get_current_user),
) -> EventID:
    """Update a event in the database."""
    return await put_asset(
        table="info.events",
        asset=event,
        pkeys=["id"],
        user=user,
        background_tasks=background_tasks,
    )


@router.delete("")
@router.delete("/")
async def delete_event(
    background_tasks: BackgroundTasks,
    event: EventID = Body(...),
    user: User = Depends(get_current_user),
):
    res = await delete_asset(
        table="info.events",
        asset=event,
        pkeys=["id"],
        user=user,
        background_tasks=background_tasks,
    )
    return res
