from .db import AsyncDB, AsyncDBPool
import subprocess
import importlib


def get_version():
    try:
        output = (
            subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"])
            .decode()
            .strip()
        )
        return output.replace("v", "")
    except subprocess.CalledProcessError:
        return "unkown"


try:
    __version__ = importlib.metadata.version(__package__)
except importlib.metadata.PackageNotFoundError:
    __version__ = get_version()

__all__ = ["AsyncDB", "AsyncDBPool"]
