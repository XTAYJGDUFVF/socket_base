
import os
import asyncio

import router
from config import Config
from model import initialize
from model.m_handler import HandlerFactory
from model.m_protocol import ServerProtocol
from service import Service
from util.util import LoggerConfig, app_log


def start():

    # is_linux = bool(platform.system() == r'Linux')

    # load router
    HandlerFactory().load_settings(router.routing_table)

    # config logger
    lc = LoggerConfig()
    if Config.LogLevel:
        lc.set_level(Config.LogLevel)
    if Config.LogFilePath:
        if not os.path.exists(Config.LogFilePath):
            os.makedirs(Config.LogFilePath)
        log_file_prefix = r'{}/all.log'.format(Config.LogFilePath)
        lc.add_handler(Config.LogLevel, log_file_prefix, when=r'midnight', backupCount=Config.LogFileBackups)

    loop = asyncio.get_event_loop()

    # do cache, eventbus initialize
    loop.run_until_complete(initialize())

    # init
    coro = loop.create_server(ServerProtocol, Config.ServerHost, Config.ServerPort)
    server = loop.run_until_complete(coro)
    app_log.info(r'Serving on {}'.format(server.sockets[0].getsockname()))
    Service().run()
    try:
        loop.run_forever()
    except Exception as e:
        app_log.exception('{}'.format(e))

    # shutdown
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
    app_log.info(r'Server closed')


if __name__ == '__main__':

    start()
