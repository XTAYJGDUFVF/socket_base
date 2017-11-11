
"""
requirements:
1. 用当前项目的@asyncio.coroutine, 替换本文件的@asyncio.coroutine
2. 用当前项目的log, 替换本文件的app_log

aioredis=1.1.0

"""

import pickle
import zlib
import asyncio
import aioredis

from .util import app_log


_DEFAULT_CONFIG = {
    r'expire': 0,
    r'key_prefix': r'',
    r'channel_prefix': r'',
}

def config_redis_default(**config):

    _DEFAULT_CONFIG.update(config)


class BaseRedisPool(object):

    def __init__(self, addr, settings):

        self._addr = addr
        self._settings = settings
        self._pool = None

    @asyncio.coroutine
    def initialize(self):

        if self._pool is None:

            self._pool = yield from aioredis.create_pool(self._addr, **self._settings)

            app_log.info(r'MCachePool initialized')

    def get_client(self):

        return CacheClient(self._pool)

    def get_conn_status(self):

        conn_status = {
            r'max_conn': self._pool.maxsize,
            r'min_conn': self._pool.minsize,
            r'conn_num': self._pool.size,
            r'idle_num': self._pool.freesize,
            r'db': self._pool.db
        }

        return conn_status


class BaseEventBus(object):

    def __init__(self, pool):

        self._pool = pool
        self._conn = None
        self._channel_handlers = {}

    @asyncio.coroutine
    def _acquire_conn(self):
        app_log.debug(r'init pubsub connection')
        self._conn = yield from self._pool.acquire()
        return self._conn

    @asyncio.coroutine
    def subscribe(self, channel, handler, *args, **kwargs):
        app_log.debug(r'{} subscribe channel: {}'.format(handler, channel))
        yield from self._check_closed()
        handler_info = (handler, args, kwargs)
        if channel in self._channel_handlers:
            self._channel_handlers[channel].append(handler_info)
            return
        self._channel_handlers[channel] = [handler_info]
        yield from self._listen(channel)

    @asyncio.coroutine
    def unsubscribe(self, channel, handler, *args, **kwargs):
        app_log.debug(r'{} unsubscribe channel: {}'.format(handler, channel))
        if channel not in self._channel_handlers:
            return
        handlers = self._channel_handlers[channel]
        handler_info = (handler, args, kwargs)
        if handler_info not in handlers:
            return
        handlers.remove(handler_info)
        if not handlers:
            yield from self._check_closed()
            yield from self._conn.unsubscribe(channel)
            if channel in self._channel_handlers:
                del self._channel_handlers[channel]

    @asyncio.coroutine
    def _listen(self, channel):
        app_log.debug(r'listen channel: {}'.format(channel))
        yield from self._check_closed()
        sub_result = yield from self._conn.subscribe(channel)
        ch_obj = sub_result[0]
        coro = self._wait_message(channel, ch_obj)
        loop = asyncio.get_event_loop()
        loop.create_task(coro)

    @asyncio.coroutine
    def _wait_message(self, channel, ch_obj):
        while (yield from ch_obj.wait_message()):
            json_data = yield from ch_obj.get_json()
            handlers = self._channel_handlers.get(channel, None)
            if handlers:
                for handler_info in handlers:
                    _callable, args, kwargs = handler_info
                    coro = _callable(json_data, *args, **kwargs)
                    loop = asyncio.get_event_loop()
                    loop.create_task(coro)

    @asyncio.coroutine
    def _check_closed(self):
        """
        1. 检查监听是否已经关闭
        2. 如果已经关闭，则获取新的连接，用内存中的数据重新恢复监听
        """
        if self._conn is None or self._conn.closed:
            self._conn = yield from self._acquire_conn()
            yield from self._recover_subscribe()

    @asyncio.coroutine
    def _recover_subscribe(self):
        """
        用内存中的数据重新恢复监听
        """
        for channel, _ in self._channel_handlers.items():
            yield from self._listen(channel)



class CacheClient(object):

    def __init__(self, pool):

        self._pool = pool

    @asyncio.coroutine
    def _acquire_conn(self):

        conn = yield from self._pool.acquire()

        return conn

    def _release_conn(self, conn):

        self._pool.release(conn)

    @staticmethod
    def pickle_dumps_zip(val):

        stream = pickle.dumps(val)

        result = zlib.compress(stream)

        return result

    @staticmethod
    def unzip_pickle_loads(val):

        if val is None:
            return None

        stream = zlib.decompress(val)

        result = pickle.loads(stream)

        return result

    @asyncio.coroutine
    def get(self, key):

        result = None

        key = _DEFAULT_CONFIG[r'key_prefix'] + key

        try:

            conn = yield from self._acquire_conn()

            b_val = yield from conn.get(key)

            result = self.unzip_pickle_loads(b_val)

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache get: {}'.format(e))

        return result

    @asyncio.coroutine
    def set(self, key, val, expire=None):

        if expire is None:
            expire = _DEFAULT_CONFIG[r'expire']

        result = None

        key = _DEFAULT_CONFIG[r'key_prefix'] + key

        try:

            conn = yield from self._acquire_conn()

            b_val = self.pickle_dumps_zip(val)

            yield from conn.set(key, b_val, expire=expire)

            result = True

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache set: {}'.format(e))

        return result

    @asyncio.coroutine
    def delete(self, key):

        key = _DEFAULT_CONFIG[r'key_prefix'] + key

        try:

            conn = yield from self._acquire_conn()

            yield from conn.delete(key)

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache delete: {}'.format(e))

    @asyncio.coroutine
    def publish(self, channel, content):

        try:

            if isinstance(content, str):
                content = bytes(content, r'utf-8')

            conn = yield from self._acquire_conn()

            yield from conn.publish(channel, content)

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache publish: {}'.format(e))

    @asyncio.coroutine
    def expire(self, key, expire):

        key = _DEFAULT_CONFIG[r'key_prefix'] + key

        try:

            conn = yield from self._acquire_conn()

            yield from conn.expire(key, expire)

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache expire: {}'.format(e))

    @asyncio.coroutine
    def ttl(self, key):

        result = 0

        key = _DEFAULT_CONFIG[r'key_prefix'] + key

        try:

            conn = yield from self._acquire_conn()

            ttl_second = yield from conn.ttl(key)

            if ttl_second > 0:
                result = ttl_second

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache ttl: {}'.format(e))

        return result

    @asyncio.coroutine
    def setnx(self, key, val):

        result = False

        key = _DEFAULT_CONFIG[r'key_prefix'] + key

        try:

            conn = yield from self._acquire_conn()

            setnx_result = yield from conn.setnx(key, val)

            if setnx_result:
                result = True 

            self._release_conn(conn)

        except Exception as e:

            app_log.exception(r'cache setnx: {}'.format(e))

        return result
