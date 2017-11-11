
import time
import copy
import io
import datetime
import functools
import base64
import binascii
import textwrap
import asyncio
import logging
import logging.handlers
from collections import OrderedDict
from contextlib import contextmanager

from xlwt import Workbook, XFStyle, Borders, Pattern

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from sdk import jwt


app_log = logging.getLogger(r'app_log')

_fmt = '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s'
_datefmt = '%y%m%d %H:%M:%S'
_log_formatter = logging.Formatter(fmt=_fmt, datefmt=_datefmt)

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)
_stream_handler.setLevel(logging.NOTSET)

app_log.addHandler(_stream_handler)

class LoggerConfiguredError(Exception):
    pass

class LoggerConfig(object):

    ins = None

    def __new__(cls):

        if not cls.ins:
            cls.ins = super().__new__(cls)
            return cls.ins

        raise LoggerConfiguredError(r'app_log is already configured, further instantiate is not allowed')

    def __init__(self):

        self._logger = app_log

        self._formatter = _log_formatter

        self._default_handler = _stream_handler

    def set_level(self, logger_level):

        level = getattr(logging, logger_level.upper(), logging.NOTSET)

        self._logger.setLevel(level)

        return self

    def add_handler(self, handler_level, *args, disable_default_handler=True, **kw_args):

        file_handler = logging.handlers.TimedRotatingFileHandler(*args, **kw_args)

        level = getattr(logging, handler_level.upper(), logging.NOTSET)

        file_handler.setLevel(level)

        file_handler.setFormatter(self._formatter)

        self._logger.addHandler(file_handler)

        if disable_default_handler:
            self._logger.removeHandler(self._default_handler)

        return self

# lc = LoggerConfig()

# lc.set_level('debug')
# lc.add_handler('info', 'log.log', disable_default_handler=False, when=r'midnight')


# app_log.debug("debug")
# app_log.info("info")
# app_log.warning("warning")
# app_log.error("error")
# app_log.critical("critical")

# LoggerConfig()
# this will raise LoggerConfiguredError


class BaseUtil(object):

    @classmethod
    def timstamp_second(self):
        return int(time.time())

    @classmethod
    def timstamp_mili_second(self):
        return int(time.time()*1000)

class NullData():

    def __int__(self):

        return 0

    def __float__(self):

        return 0.0

    def __len__(self):

        return 0

    def __repr__(self):

        return r''

    def __eq__(self, obj):

        return bool(obj) == False

    def __nonzero__(self):

        return False

    def __cmp__(self, val):

        if val is None:
            return 0
        else:
            return 1


class Const(OrderedDict):

    class Predefine(NullData):
        pass

    class ConstError(TypeError):
        pass

    def __init__(self):

        super().__init__()

    def __getattr__(self, key):

        if key[:1] == r'_':
            return super().__getattr__(key)
        else:
            return self.__getitem__(key)

    def __setattr__(self, key, val):

        if key[:1] == r'_':
            super().__setattr__(key, val)
        else:
            self.__setitem__(key, val)

    def __delattr__(self, key):

        if key[:1] == r'_':
            super().__delattr__(key)
        else:
            self.__delitem__(key)

    def __setitem__(self, key, val):

        if key in self and not isinstance(self.__getitem__(key), self.Predefine):
            raise self.ConstError()
        else:
            super().__setitem__(key, val)

    def __delitem__(self, key):

        raise self.ConstError()

    def exist(self, val):

        return val in self.values()

class Ignore(Exception):

    pass


class SingletonMetaclass(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):

        result = None

        instances = cls.__class__._instances

        if cls in instances:

            result = instances[cls]

        else:

            result = instances[cls] = super().__call__(*args, **kwargs)

        return result


class Singleton(metaclass=SingletonMetaclass):

    pass


class RepeatTask(object):

    def __init__(self, interval, _callable, *args, **kw_args):

        self.interval = interval

        self.callable = _callable

        self.args = args

        self.kw_args = kw_args

        self._stop = False

    def wrapped_callable(self, _callable, *args, **kw_args):
        """
        wrap any kind of callable into chained callable, which will call itself after certain interval
        """

        if asyncio.iscoroutinefunction(_callable):

            @asyncio.coroutine
            def wrapper():
                try:
                    yield _callable(*args, **kw_args)
                except Exception as e:
                    app_log.exception(r'{}'.format(e))
                if not self._stop:
                    yield asyncio.sleep(self.interval)
                    # loop = IOLoop.current() # XXX
                    loop = asyncio.get_event_loop()
                    loop.call_later(0, wrapper)
            return wrapper

        else:

            def wrapper():
                try:
                    _callable(*args, **kw_args)
                except Exception as e:
                    app_log.exception(r'{}'.format(e))
                if not self._stop:
                    # loop = IOLoop.current() # XXX
                    loop = asyncio.get_event_loop()
                    loop.call_later(self.interval, wrapper)
            return wrapper


    @asyncio.coroutine
    def run(self):
        """
        run coroutine or normal function all in coroutine style
        """

        func_like = self.wrapped_callable(self.callable, *self.args, **self.kw_args)

        coro_like = func_like()

        if asyncio.iscoroutine(coro_like):
            yield coro_like

    def stop(self):

        self._stop = True


class _ErrorCode(object):

    def __init__(self):

        self._key_code_map = {}
        self._code_reason_map = {}

    def load_settings(self, settings):

        for key, info in settings.items():
            code = info[0]
            self._key_code_map[key] = code
            self._code_reason_map[code] = info[1]

    def get_error_response(self, code_delegate, extra=None):

        code = code_delegate.code

        extra = code_delegate.extra

        if hasattr(code_delegate, r'error'):
            error = code_delegate.error
        else:
            error = self._code_reason_map[code]

        resp = {r'code': code, r'error': error}

        if extra is not None:
            resp[r'extra'] = extra

        return resp

    def __getattr__(self, name):

        code = self._key_code_map[name]

        code_delegate = _ErrorCodeDelegate(code)

        return code_delegate

    def construct_code(self, error_resp):

        code = error_resp.get(r'code', None)
        error = error_resp.get(r'error', None)
        extra = error_resp.get(r'extra', None)

        if code is None or error is None:
            code = self._key_code_map[r'BadErrorFormat']
            code_delegate = _ErrorCodeDelegate(code).set_extra(str(error_resp))

        else:
            code_delegate = _ErrorCodeDelegate(code)
            setattr(code_delegate, r'error', error)
            if extra is not None:
                code_delegate.set_extra(extra)

        return code_delegate


@functools.total_ordering
class _ErrorCodeDelegate(object):

    def __init__(self, code):
        self.code = code
        self.extra = None

    def __eq__(self, other):
        return self.code == other.code

    def __lt__(self, other):
        return self.code < other.code

    def set_extra(self, extra):
        self.extra = extra
        return self

    def __repr__(self):
        return r'{} {}'.format(self.code, self.extra or r'')


class ExcelWT(Workbook):

    def __init__(self, name, encoding=r'utf-8', style_compression=0):

        super().__init__(encoding, style_compression)

        self._book_name = name
        self._current_sheet = None

        self._default_style = XFStyle()
        self._default_style.borders.left = Borders.THIN
        self._default_style.borders.right = Borders.THIN
        self._default_style.borders.top = Borders.THIN
        self._default_style.borders.bottom = Borders.THIN
        self._default_style.pattern.pattern = Pattern.SOLID_PATTERN
        self._default_style.pattern.pattern_fore_colour = 0x01

        self._default_title_style = copy.deepcopy(self._default_style)
        self._default_title_style.font.bold = True
        self._default_title_style.pattern.pattern_fore_colour = 0x16

    def create_sheet(self, name, titles=tuple()):

        sheet = self._current_sheet = self.add_sheet(name)
        style = self._default_title_style

        for index, title in enumerate(titles):
            sheet.write(0, index, title, style)
            sheet.col(index).width = 0x1200

    def add_sheet_row(self, *args):

        sheet = self._current_sheet
        style = self._default_style

        nrow = len(sheet.rows)

        for index, value in enumerate(args):
            sheet.write(nrow, index, value, style)

    def get_file(self):

        result = b''

        with io.BytesIO() as stream:

            self.save(stream)

            result = stream.getvalue()

        return result

    def write_request(self, request):

        filename = r'{0:s}.{1:s}.xls'.format(self._book_name, datetime.datetime.today().strftime('%y%m%d.%H%M%S'))

        request.set_header(r'Content-Type', r'application/vnd.ms-excel')
        request.set_header(r'Content-Disposition', r'attachment;filename={0:s}'.format(filename))

        filedata = self.get_file()

        return request.finish(filedata)


class RsaUtil():

    @classmethod
    def gen_rsa_key(cls, rsa_key, private=False):

        if private:
            start_line = r'-----BEGIN RSA PRIVATE KEY-----'
            end_line = r'-----END RSA PRIVATE KEY-----'
        else:
            start_line = r'-----BEGIN PUBLIC KEY-----'
            end_line = r'-----END PUBLIC KEY-----'

        rsa_key = textwrap.fill(rsa_key, 64)

        return '\n'.join([start_line, rsa_key, end_line])

    @classmethod
    def rsa_sha1_sign(cls, rsa_key, sign_data):

        algorithm = jwt.algorithms.RSAAlgorithm(hashes.SHA1)

        key = algorithm.prepare_key(cls.gen_rsa_key(rsa_key, True))

        signature = algorithm.sign(sign_data.encode(r'utf-8'), key)

        return base64.b64encode(signature).decode()

    @classmethod
    def rsa_sha1_verity(cls, pubic_key, verity_data, verity_sign):

        algorithm = jwt.algorithms.RSAAlgorithm(hashes.SHA1)

        public_key = load_pem_public_key(cls.gen_rsa_key(pubic_key).encode(r'utf-8'), backend=default_backend())

        result = algorithm.verify(verity_data.encode(), public_key, binascii.a2b_base64(verity_sign))

        return result

    @classmethod
    def rsa_md5_sign(cls, rsa_key, sign_data):

        algorithm = jwt.algorithms.RSAAlgorithm(hashes.MD5)

        key = algorithm.prepare_key(cls.gen_rsa_key(rsa_key, True))

        signature = algorithm.sign(sign_data.encode(r'utf-8'), key)

        return base64.b64encode(signature).decode()

    @classmethod
    def rsa_md5_verity(cls, pubic_key, verity_data, verity_sign):

        algorithm = jwt.algorithms.RSAAlgorithm(hashes.MD5)

        public_key = load_pem_public_key(cls.gen_rsa_key(pubic_key).encode(r'utf-8'), backend=default_backend())

        result = algorithm.verify(verity_data.encode(), public_key, binascii.a2b_base64(verity_sign))

        return result



def run_in_executor(threadpool_executor, func):

    @asyncio.coroutine
    def wrapper(*args, **kwargs):

        func_result = yield threadpool_executor.submit(func, *args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            result = yield func_result
        else:
            result = func_result

        return result

    return wrapper

@contextmanager
def catch_error():

    try:
        yield

    except Ignore:
        pass

    except Exception as err:
        app_log.exception(err)
