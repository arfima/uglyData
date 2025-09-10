import os
import s3fs
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/s3", tags=["s3"])


def _fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        anon=False,
        key=os.environ.get("ACCESS_KEY"),
        secret=os.environ.get("SECRET_KEY"),
        client_kwargs={"endpoint_url": os.environ.get("S3_ENDPOINT")},
    )


BUCKET = os.environ.get("BUCKET", "nuts")


@router.get("/list")
async def list_files(
    prefix: str = Query(
        "", description="Prefijo dentro del bucket (p. ej. DB/HIST/MAIN/foo)"
    ),
    ext: str | None = Query(None, description="Filtra por extensión, p. ej. .txt"),
    recursive: bool = Query(False),
    limit: int = Query(1000, ge=1, le=5000),
    start_after: str | None = Query(
        None, description="Key desde la que continuar (paginación)"
    ),
):
    fs = _fs()
    base = f"{BUCKET}/{prefix}".rstrip("/")
    try:
        entries = fs.find(base) if recursive else fs.ls(base, detail=True)
    except FileNotFoundError:
        entries = []

    # normaliza a lista de dicts con Key
    if entries and isinstance(entries[0], str):
        entries = [{"Key": e} for e in entries]

    keys: list[str] = []
    for obj in entries:
        key: str = obj["Key"]
        if key.endswith("/"):  # carpetas virtuales en S3
            continue
        if ext and not key.endswith(ext):
            continue
        if start_after and key <= start_after:
            continue
        # quita el bucket del path
        keys.append(key.split("/", 1)[1])
        if len(keys) >= limit:
            break

    return {"bucket": BUCKET, "prefix": prefix, "files": keys}


@router.get("/download")
async def download_file(key: str, as_text: bool = True):
    """Descarga/streaming de un objeto por key relativa (sin el bucket)."""
    fs = _fs()
    path = f"{BUCKET}/{key}".lstrip("/")
    if not fs.exists(path):
        raise HTTPException(status_code=404, detail="No existe el objeto solicitado")

    fh = fs.open(path, "rb")  # s3fs devuelve file-like, perfecto para streaming
    media = "text/plain" if as_text else "application/octet-stream"
    return StreamingResponse(fh, media_type=media)


@router.put("/upload")
async def upload_file(key: str, request: Request):
    """Sube/overwrite un objeto (body = bytes)."""
    fs = _fs()
    path = f"{BUCKET}/{key}".lstrip("/")
    body = await request.body()
    with fs.open(path, "wb") as f:
        f.write(body)
    return {"ok": True, "key": key, "size": len(body)}
