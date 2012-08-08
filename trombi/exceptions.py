#! /usr/bin/python


__author__="Igor K."
__date__ ="$08-Aug-2012 17:50:30$"

class TrombiException(Exception):
    code=None
    message=None
    
    _exceptions={}
    
    def __init__(self, message=None, code=None):
        self.code = self.__class__.code if code is None else code
        super(TrombiException, self).__init__(self.__class__.message if message is None else message)
        
    @classmethod
    def get_exception_class(cls, name, attrs):
        exception = type(name, (cls,), attrs)
        code = attrs.get('code', None)
        if code in cls._exceptions:
            cls._exceptions[code].append(exception)
        else:
            cls._exceptions[code] = [exception]
        return exception
        
    @classmethod
    def get_excetion_by_code(cls, code, message=None):
        if code in cls._exceptions:
            return cls._exceptions[code][0](message)
        else:
            return cls(message, code)
        
def get_exception_by_code(code, message=None):
    return TrombiException.get_excetion_by_code(code, message)
    
TrombiBadRequest            = TrombiException.get_exception_class('TrombiBadRequest', dict(message='BAD_REQUEST', code=400))
TrombiConflict              = TrombiException.get_exception_class('TrombiConflict', dict(message='CONFLICT', code=409))
TrombiPreconditionFailed    = TrombiException.get_exception_class('TrombiPreconditionFailed', dict(message='PRECONDITION_FAILED', code=412))
TrombiNotFound              = TrombiException.get_exception_class('TrombiNotFound', dict(message='NOT_FOUND', code=404))
TrombiServerError           = TrombiException.get_exception_class('TrombiServerError', dict(message='CONFLICT', code=500))

TrombiInvalidDatabaseName   = TrombiException.get_exception_class('TrombiInvalidDatabaseName', dict(message='INVALID_DATABASE_NAME', code=51))
TrombiConnectionFailed      = TrombiException.get_exception_class('TrombiConnectionFailed', dict(message='CONNECTION_FAILED', code=599))
