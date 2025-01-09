from typing import List, Dict

from helper.serialization.json import json_encoding


class BKMessageTranslator:
    __type_sep = "|"

    def __init__(self):
        self.type_dic: Dict[str, type] = {}

    def register_types(self, types: List[type]):
        for t in types:
            self.type_dic[t.__name__] = t

    def serialize(self, obj: object) -> str:
        type_s = type(obj).__name__
        serialized_message = json_encoding.encode(obj.to_dict())
        return type_s + self.__type_sep + serialized_message

    def deserialize(self, message: str):
        splitted = message.split(self.__type_sep)
        type_s = splitted[0]
        serialized_message = splitted[1]
        deserialized_dict = json_encoding.decode(serialized_message)
        if type_s not in self.type_dic:
            return None
        return self.type_dic[type_s].from_dict(deserialized_dict)
