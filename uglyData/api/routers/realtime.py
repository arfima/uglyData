# import elasticapm
# import logging
# from fastapi import APIRouter, Depends, Query, WebSocket
# from starlette.websockets import WebSocketDisconnect


# from ..dependencies import bars_parameters
# from uglyData.api.models import LoadRequest

# # from ..service import ElasticDB


# LOG = logging.getLogger(__name__)

# router = APIRouter(prefix="/realtime", tags=["realtime"])


# @router.get("/instruments")
# async def get_instruments(
#     curve: str = Query(..., description="Curve name to filter contracts")
# ):
#     return await ElasticDB.get_outrights_instruments(curve)


# @router.get("/curves")
# async def get_curves():
#     return await ElasticDB.get_outrights_curves()


# @router.get("/data/count")
# async def get_count(request=Depends(bars_parameters)):
#     request = LoadRequest(
#         ticker=request["instrument"],
#         from_date=request["start"],
#         to_date=request["end"],
#     )
#     return await ElasticDB.get_count(request=request)


# @router.websocket("/data")
# async def get_data(websocket: WebSocket):
#     await websocket.accept()

#     elasticapm.instrument()
#     client = elasticapm.get_client()
#     client.begin_transaction(transaction_type="request")
#     elasticapm.set_transaction_name("WS /api/v1/realtime/data")

#     try:
#         request = await websocket.receive_json()
#         LOG.info("Request received", extra=request)
#         elasticapm.set_context(request)
#         request = LoadRequest(**request)
#         async for df in ElasticDB.paginate_realtime_data(
#             request=request, page_size=10_000, output="dataframe"
#         ):
#             size = df.shape[0]
#             with elasticapm.capture_span("build parquet"):
#                 if "curve" in df.columns:
#                     df = df.dropna(subset="curve")
#                 df = df.to_parquet()
#             with elasticapm.capture_span("send data"):
#                 await websocket.send_bytes(df)
#             LOG.debug("Send data block", extra={"size": size, **request.dict()})

#         client.end_transaction(result="SUCCESS")
#     except WebSocketDisconnect:
#         LOG.error(f"Client '{websocket.client.host}' disconnected")
#         client.end_transaction(result="FAILURE")
