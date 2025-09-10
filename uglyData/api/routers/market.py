import logging

from fastapi import (
    APIRouter,
    WebSocket,
)
from fastapi.responses import ORJSONResponse


from ..wsockets import handle_load_websocket
from uglyData.api.models import LoadRequest


LOG = logging.getLogger("uvicorn.error")


router = APIRouter(
    prefix="/market", tags=["market"], default_response_class=ORJSONResponse
)


DEFAULT_SOURCE = "refinitiv"


@router.websocket("/t2t/{dtype}")
async def get_market_data_t2t(websocket: WebSocket, dtype: str):
    return await handle_load_websocket(
        dtype=dtype,
        websocket=websocket,
        endpoint="/api/v1/market/",
        model_cls=LoadRequest,
    )


@router.websocket("/intraday/{dtype}")
async def get_market_data_intraday(websocket: WebSocket, dtype: str):
    if dtype == "quotes":
        dtype = "intraquote"
    elif dtype == "trades":
        dtype = "intrade"
    return await handle_load_websocket(
        dtype=dtype,
        websocket=websocket,
        endpoint="/api/v1/market/",
        model_cls=LoadRequest,
    )


@router.websocket("/{dtype}")
async def get_market_data(websocket: WebSocket, dtype: str):
    return await handle_load_websocket(
        dtype=dtype,
        websocket=websocket,
        endpoint="/api/v1/market/",
        model_cls=LoadRequest,
    )
