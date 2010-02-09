import os
import sys
import errno
import new
import shutil
import nose.tools

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from inoi.connectivity.server import Application


def unrandom(random=None):
    # Make os.urandom not so random. If user wants, a custom random
    # function can be used providing keyword argument `random`
    if random is None:
        random = lambda x: '1' * x

    def outer(func):
        @nose.tools.make_decorator(func)
        def wrap_urandom(*a, **kw):
            _old_urandom = os.urandom
            try:
                os.urandom = random
                func(*a, **kw)
            finally:
                os.urandom = _old_urandom
        return wrap_urandom
    return outer

def with_ioloop(ip='127.0.0.1'):
    def outer(func):
        @nose.tools.make_decorator(func)
        def wrapper(*args, **kwargs):
            # Chosen by fair dice roll. Guaranteed to be random.
            port = 8991

            statedir = maketemp()

            ioloop = IOLoop()
            application = Application(ip, statedir)

            http_server = HTTPServer(application, io_loop=ioloop)
            http_server.listen(port)

            # Override ioloop's _run_callback to let all exceptions through
            def run_callback(self, callback):
                callback()
            ioloop._run_callback = new.instancemethod(run_callback, ioloop)

            baseurl = 'http://localhost:%d' % port
            try:
                return func(baseurl, ioloop, application, *args, **kwargs)
            finally:
                # Close the HTTP server socket so that the address can
                # be reused
                http_server._socket.close()

        return wrapper
    return outer

def mkdir(*a, **kw):
    try:
        os.mkdir(*a, **kw)
    except OSError, e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

def find_test_name():
    try:
        from nose.case import Test
        from nose.suite import ContextSuite
        import types
        def get_nose_name(its_self):
            if isinstance(its_self, Test):
                file_, module, class_ = its_self.address()
                name = '%s:%s' % (module, class_)
                return name
            elif isinstance(its_self, ContextSuite):
                if isinstance(its_self.context, types.ModuleType):
                    return its_self.context.__name__
    except ImportError:
        # older nose
        from nose.case import FunctionTestCase, MethodTestCase
        from nose.suite import TestModule
        from nose.util import test_address
        def get_nose_name(its_self):
            if isinstance(its_self, (FunctionTestCase, MethodTestCase)):
                file_, module, class_ = test_address(its_self)
                name = '%s:%s' % (module, class_)
                return name
            elif isinstance(its_self, TestModule):
                return its_self.moduleName

    i = 0
    while True:
        i += 1
        frame = sys._getframe(i)
        # kludge, hunt callers upwards until we find our nose
        if (frame.f_code.co_varnames
            and frame.f_code.co_varnames[0] == 'self'):
            its_self = frame.f_locals['self']
            name = get_nose_name(its_self)
            if name is not None:
                return name

def maketemp():
    tmp = os.path.join(os.path.dirname(__file__), 'tmp')
    mkdir(tmp)

    name = find_test_name()
    tmp = os.path.join(tmp, name)
    try:
        shutil.rmtree(tmp)
    except OSError, e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise
    os.mkdir(tmp)
    return tmp


def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass, e:
        return e
    except:
        if hasattr(excClass,'__name__'): excName = excClass.__name__
        else: excName = str(excClass)
        raise AssertionError("%s not raised" % excName)
