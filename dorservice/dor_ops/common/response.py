'''
Created on 2017年8月25日

@author: walter628
'''
from jsonrpc.jsonrpc2 import JSONRPC20Response


class DorResponse(JSONRPC20Response):

    def __init__(self, _id, result):
        super(DorResponse, self).__init__(_id=_id, result=result)

    @property
    def id(self):
        return self._id


class DorErrorResponse(JSONRPC20Response):

    def __init__(self, _id, code, message):
        self.code = code
        self.message = message
        error = {'code': code, 'message': message}
        super(DorErrorResponse, self).__init__(_id=_id, error=error)
