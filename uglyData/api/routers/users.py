from fastapi import (
    APIRouter,
    Body,
    Path,
    Depends,
    status,
    BackgroundTasks,
)

from uglyData.api.models import User, UserStr
from ..dependencies import (
    get_asset,
    get_all_assets,
    post_asset,
    put_asset,
    limit_params,
)
from ..auth import get_current_user

router = APIRouter(prefix="/users", tags=["users"])


def prepare_params(params: dict, **kwargs) -> dict:
    params["search_columns"] = [
        "username",
        "name",
        "name_unaccented",
    ]

    filters = {k: v for k, v in kwargs.items() if v}

    return params, filters


@router.get("")
@router.get("/")
async def get_all_users(
    params=Depends(limit_params), user: User = Depends(get_current_user)
) -> list[User]:
    """Get all users in the database."""

    params, filters = prepare_params(params)

    return await get_all_assets(
        table="auth.users_unaccent", auth_table="auth.users", user=user, **params
    )


@router.get("/count")
async def get_count(
    params=Depends(limit_params),
    user: User = Depends(get_current_user),
):
    params, filters = prepare_params(params)
    params["limit"] = None
    params["offset"] = None

    count = await get_all_assets(
        table="auth.users_unaccent",
        auth_table="auth.users",
        user=user,
        filters=filters,
        return_just_count=True,
        **params,
    )
    return count[0]


@router.get("/{username}")
async def get_user_info(
    username: str = Path(description="MIC of the user."),
    user: User = Depends(get_current_user),
) -> User:
    """Get information about a single user."""
    return await get_asset(table="auth.users", values={"username": username}, user=user)


@router.post("", status_code=status.HTTP_201_CREATED)
@router.post("/", status_code=status.HTTP_201_CREATED)
async def add_user(
    user: UserStr | User = Body(...),
    current_user: User = Depends(get_current_user),
    background_tasks: BackgroundTasks = None,
) -> User:
    """Add a user to the database."""
    return await post_asset(
        table="auth.users",
        asset=user,
        user=current_user,
        background_tasks=background_tasks,
    )


@router.put("")
@router.put("/")
async def update_user(
    user: UserStr | User = Body(...),
    current_user: User = Depends(get_current_user),
    background_tasks: BackgroundTasks = None,
) -> User:
    """Update a user in the database."""
    return await put_asset(
        table="auth.users",
        asset=user,
        pkeys=["username"],
        user=current_user,
        background_tasks=background_tasks,
    )


# @router.delete("/{user}")
# async def delete_user(
#     user: str = Path(description="Name of the user."),
#     current_user: User = Depends(get_current_user),
# ):
#     raise HTTPException(status_code=500, detail="Not implemented")
