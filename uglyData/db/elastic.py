import os

# import yaml
from elasticsearch import AsyncElasticsearch

from .postgres import AbstractDB


CONFIG_FILE_PATH = os.environ.get(
    "DBYMLPATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "databases.yml"),
)


class ESClient(AbstractDB):
    def __init__(self, dbname: str):
        self.dbname = dbname
        self.conn = None

    async def connect(self):
        # config = yaml.safe_load(open(CONFIG_FILE_PATH))[self.dbname]
        config = {
            "host": "10.66.10.9",
            "port": "9200",
            "user": "elastic",
            "password": "changeme",
        }
        host = f"http://{config['host']}:{config['port']}"
        self.conn = AsyncElasticsearch(
            hosts=[host], basic_auth=(config["user"], config["password"])
        )

    async def close(self):
        await self.conn.close()

    async def search(self, query, **kwargs):
        return await self.conn.search(query, **kwargs)

    async def paginate(
        self,
        body,
        page_size,
    ):
        body["size"] = page_size
        body["from"] = 0
        while True:
            result = await self.conn.search(body=body)
            if result["hits"]["hits"]:
                yield result["hits"]["hits"]
                body["from"] += page_size
            else:
                yield []
                break
