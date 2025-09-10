import logging
from fastapi import (
    APIRouter,
    WebSocket,
    status,
    Body,
)
from fastapi.responses import ORJSONResponse
from uglyData.api.dependencies import (
    get_all_assets,
    post_asset,
)
from uglyData.api.models import (
    Account,
    Accounts,
    AuditTrailFTPFile,
    AuditTrailLoadRequest,
    User,
)
from ..builders import AUDIT_SCHEMA

from ..wsockets import handle_store_websocket, handle_load_websocket


LOG = logging.getLogger("uvicorn.error")

router = APIRouter(
    prefix="/audit-trail", tags=["audit-trail"], default_response_class=ORJSONResponse
)


@router.get("/ftp-files")
async def get_ftp_files(
    user: User = None,  # temporal bypass to test everything Depends(get_current_user),
) -> list[AuditTrailFTPFile]:
    """Get ftp file names already stored in the DB"""
    return await get_all_assets(table=f"{AUDIT_SCHEMA}.audittrail_ftp_files", user=user)


@router.post("/ftp-files", status_code=status.HTTP_201_CREATED)
async def add_ftp_file(
    files: AuditTrailFTPFile = Body(...), user: User = None
):  # temporal bypass to test everything Depends(get_current_user),
    """Add ftp file names that have been already stored in the DB"""
    return await post_asset(
        table=f"{AUDIT_SCHEMA}.audittrail_ftp_files",
        user=user,
        asset=files,
        discard_duplicates=True,
        enable_log=False,
    )


@router.post("/accounts")
async def store_accounts(
    accounts: Accounts = Body(...), user: User = None
):  # temporal bypass to test everything. Depends(get_current_user),
    """Store multiple accounts in the DB"""
    assets = []
    for account, insertion_time in zip(accounts.accounts, accounts.insertion_times):
        assets.append(Account(account=account, insertion_time=insertion_time))
    if assets:
        assets = [ass.model_dump() for ass in assets]
        await post_asset(
            table=f"{AUDIT_SCHEMA}.accounts",
            user=user,
            asset=assets,
            discard_duplicates=True,
            enable_log=False,
        )


@router.websocket("/store/tt-audit-trail")
async def store_audit_trail_tt(
    websocket: WebSocket,
    user: User = None,
):  # temporal bypass to test everything. Depends(get_current_user),
    """Stores audit trail that is received via websocket"""
    await handle_store_websocket(
        table="tt_audittrail",
        websocket=websocket,
        endpoint="/api/v1/audit-trail",
        user=user,
        schema=AUDIT_SCHEMA,
    )


@router.websocket("/store/tt-audit-trail-temp")
async def store_audit_trail_tt_temp(
    websocket: WebSocket,
    user: User = None,
):  # temporal bypass to test everything. Depends(get_current_user),
    """Stores audit trail that is received via websocket"""
    await handle_store_websocket(
        table="tt_audittrail_temp",
        websocket=websocket,
        endpoint="/api/v1/tt-audit-trail-temp",
        user=user,
        schema=AUDIT_SCHEMA,
    )


@router.websocket("/tt-audit-trail")
async def get_tt_audit_trail(websocket: WebSocket):
    await handle_load_websocket(
        "tt_audittrail",
        websocket,
        endpoint="/api/v1/audit-trail/",
        model_cls=AuditTrailLoadRequest,
        drop_cols=[],
        dropna_cols=False,
    )
