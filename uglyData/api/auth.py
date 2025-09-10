from typing import Annotated

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from fastapi import Depends
from fastapi.exceptions import HTTPException, WebSocketException
from fastapi.security import OAuth2PasswordBearer
from async_lru import alru_cache

from uglyData.api.models import User
from .service import DB
from starlette.datastructures import Headers

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

GITLAB_URL = "https://git.arfima.com/api/v4/user"


async def decode_token(token: str) -> User:
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession() as session:
        async with session.get(GITLAB_URL, headers=headers) as resp:
            try:
                r = await resp.json()
                resp.raise_for_status()
            except ClientResponseError as e:
                raise HTTPException(status_code=e.status, detail=r)
            return r


@alru_cache(ttl=10)  # 30 minutes
async def _get_user(token: str):
    user = await decode_token(token)
    user = await DB.get_asset(table="auth.users", values={"username": user["username"]})

    return User(**user)


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]) -> User:
    return await _get_user(token)


async def websocket_auth(headers: Headers) -> User:
    auth = headers.get("Authorization")
    if auth is None:
        raise WebSocketException(status_code=401, detail="Missing token")
    token = auth.split(" ")[1]
    return await _get_user(token)
