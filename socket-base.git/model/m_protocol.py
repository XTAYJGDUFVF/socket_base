
import asyncio
import copy
import json

from util.util import Singleton, app_log, Const
from config import Config

from .m_handler import HandlerFactory


class ConnectionManager(Singleton):

    def __init__(self):

        self.connections = {}

    def add_connection(self, session, connection):

        self.connections[session] = connection

    def remove_connection(self, session):

        if session in self.connections:
            del self.connections[session]



ContentType = Const()
ContentType.Json = r'json'
ContentType.String = r'string'
ContentType.Byte = r'byte'


class ServerProtocol(asyncio.Protocol):

    def __init__(self):

        self.transport = None
        self.client_host = r''
        self.client_port = 0
        self.session = r''
        self.authenticated = False
        self.account_info = None
        self.buffer = b''

    def connection_made(self, transport):
        self.transport = transport
        self.client_host, self.client_port = self.transport.get_extra_info(r'peername')
        self.session = r'session_{}_{}'.format(self.client_host, self.client_port)
        ConnectionManager().add_connection(self.session, self)
        app_log.info(r'connection {}:{} made'.format(self.client_host, self.client_port))

    def connection_lost(self, exc):
        ConnectionManager().remove_connection(self.session)
        if exc is not None:
            app_log.warning(r'connection lost exc: {}'.format(exc))
        app_log.info(r'connection {}:{} lost'.format(self.client_host, self.client_port))

    def data_received(self, data):
        self.buffer += data
        buffer = copy.deepcopy(self.buffer)
        msgid_pos = buffer.find(b'\r\n')
        if msgid_pos == -1:
            if len(self.buffer) < Config.ProtocolBufferSize:
                return
            self.close(r'protocol abuse, no msgid within buffer size')
            return
        try:
            msgid = buffer[:msgid_pos].decode(r'utf-8')
        except UnicodeDecodeError as e:
            self.close(r'protocol abuse, msgid decode error, {}'.format(e))
            return

        buffer = buffer[msgid_pos+2:]
        header_pos = buffer.find(b'\r\n')
        if header_pos == -1:
            if len(self.buffer) < Config.ProtocolBufferSize:
                return
            self.close(r'protocol abuse, no header within buffer size')
            return
        b_header = buffer[:header_pos]
        buffer = buffer[header_pos+2:]
        try:
            s_header = b_header.decode(r'utf-8')
            header = json.loads(s_header)
        except Exception as e:
            self.close(r'protocol abuse, parse header failed, {}'.format(e))
            return
        # header = {
        #     r'content_type': r'json', fixed
        #     r'content_length': 1024, fixed
        #     r'_': 0,
        # }
        try:
            content_type = header[r'content_type']
            content_length = header[r'content_length']
        except KeyError as e:
            self.close(r'protocol abuse, header error, {}'.format(e))
            return
        if len(buffer) < header[r'content_length']:
            return
        b_content = buffer[:content_length]
        buffer = buffer[content_length+1:]
        try:
            if content_type == ContentType.Json:
                s_content = b_content.decode(r'utf-8')
                content = json.loads(s_content)
            elif content_type == ContentType.Byte:
                content = b_content
            elif content_type == ContentType.String:
                content = b_content.decode(r'utf-8')
            else:
                self.close(r'protocol abuse, content type not supported')
                return
        except Exception as e:
            self.close(r'protocol abuse, parse content error, {}'.format(e))
            return
        self.buffer = buffer
        # msgid, header, content
        handler_class = HandlerFactory()(msgid)
        if handler_class is None:
            self.close(r'msgid not found')
            return
        request = {
            r'msgid': msgid,
            r'header': header,
            r'content': content,
        }
        coro = handler_class().handle(self, request)
        loop = asyncio.get_event_loop()
        loop.create_task(coro)

    def close(self, reason=None):
        if reason is not None:
            app_log.info(r'connection {}:{} closed because {}'.format(self.client_host, self.client_port, reason))
        self.transport.close()

    def _write(self, msgid, header, b_data):
        b_pack = b''
        try:
            b_pack += msgid.encode('utf-8')
            b_pack += b'\r\n'
            b_pack += json.dumps(header).encode('utf-8')
            b_pack += b'\r\n'
            b_pack += b_data
        except Exception as e:
            app_log.exception(r'pack data error, {}'.format(e))
            return
        self.transport.write(b_pack)

    def write_json(self, msgid, dict_data):
        try:
            s_data = json.dumps(dict_data)
            b_data = s_data.encode(r'utf-8')
        except Exception as e:
            app_log.exception(r'write json error, {}'.format(e))
            return
        header = {
            r'content_type': ContentType.Json,
            r'content_length': len(b_data),
        }
        self._write(msgid, header, b_data)

    def write_string(self, msgid, s_data):
        try:
            b_data = s_data.encode(r'utf-8')
        except Exception as e:
            app_log.exception(r'write string error, {}'.format(e))
            return
        header = {
            r'content_type': ContentType.String,
            r'content_length': len(b_data),
        }
        self._write(msgid, header, b_data)

    def write_byte(self, msgid, b_data):
        header = {
            r'content_type': ContentType.Byte,
            r'content_length': len(b_data),
        }
        self._write(msgid, header, b_data)
