from dataclasses import dataclass
from typing import Any, Optional, TypeVar, Generic

from xms.app.web_api.models.response_status import ResponseStatus

T = TypeVar("T")


@dataclass
class GenericResponse(Generic[T]):
    status: ResponseStatus
    message: Optional[str]
    data: T

    _S = "s"
    _M = "m"
    _D = "d"

    @staticmethod
    def from_dict(dic, data_deserialize):
        status = ResponseStatus(dic[GenericResponse._S])
        message = dic[GenericResponse._M]
        data = dic[GenericResponse._D]
        if data is not None:
            data = data_deserialize(data)
        return GenericResponse(status, message, data)

    def to_dict(self):
        dic = dict()
        dic[self._S] = self.status
        dic[self._M] = self.message
        data_type = type(self.data)
        if self.data is None:
            data_serialized = None
        elif data_type == list:
            data_serialized = [o.to_dict() for o in self.data]
        elif data_type == dict:
            data_serialized = {o : self.data[o] for o in self.data}
        elif hasattr(self.data, 'to_dict'):
            data_serialized = self.data.to_dict()
        else:
            data_serialized = self.data

        dic[self._D] = data_serialized

        return dic



