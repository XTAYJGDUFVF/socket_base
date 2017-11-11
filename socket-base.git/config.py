
from util.util import Const


Config = Const()


Config.ServerHost = r'0.0.0.0'
Config.ServerPort = 4900

Config.LogLevel = r'debug'
Config.LogFilePath = r''

Config.ProtocolBufferSize = 2048


Config.RedisHost = (r'localhost', 6379)
Config.RedisBase = 0
Config.RedisMinConn = 32
Config.RedisMaxConn = 128
Config.RedisPasswd = None

