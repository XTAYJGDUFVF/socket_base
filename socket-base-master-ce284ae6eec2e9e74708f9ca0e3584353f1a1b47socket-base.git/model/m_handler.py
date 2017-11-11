
import asyncio

from util.util import Singleton


class HandlerFactory(Singleton):

    def __init__(self):

        self.routing_table = {}

    def load_settings(self, routing_table):

        self.routing_table = routing_table

    def __call__(self, msgid):

        return self.routing_table.get(msgid, None)


class BaseHandler(object):

    @asyncio.coroutine
    def prepare(self, conn, request):
        pass

    @asyncio.coroutine
    def finish(self, conn, request):
        pass

    @asyncio.coroutine
    def run(self, conn, request):
        pass

    @asyncio.coroutine
    def handle(self, conn, request):
        yield from self.prepare(conn, request)
        yield from self.run(conn, request)
