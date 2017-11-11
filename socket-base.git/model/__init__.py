
import asyncio

from util.util import Singleton, app_log
from util.redis import BaseRedisPool, BaseEventBus, config_redis_default

from config import Config


class RedisPool(Singleton, BaseRedisPool):

    def __init__(self):

        addr = Config.RedisHost

        settings = {
            r'db': Config.RedisBase,
            r'minsize': Config.RedisMinConn,
            r'maxsize': Config.RedisMaxConn,
            r'password': Config.RedisPasswd
        }

        BaseRedisPool.__init__(self, addr, settings)


class EventBus(Singleton, BaseEventBus):

    def __init__(self):

        redis_pool = RedisPool()

        BaseEventBus.__init__(self, redis_pool)


@asyncio.coroutine
def initialize():

    yield from RedisPool().initialize()
