
from typing import Any, Dict, Tuple, Type, TypeVar

T = TypeVar("T")

class IOCContainer:
    singleton_instances: Dict[str, object] = dict()
    type_implementation_map: Dict[str, type] = dict()

    @classmethod
    def register_instance_singleton(cls, type_class: Type[T], instance: Any):
        if type_class.__name__ in cls.singleton_instances:
            raise Exception("Trying to register multiple instances for type (%s)" % type_class.__name__)
        cls.singleton_instances[type_class.__name__] = instance

    @classmethod
    def get_instance_singleton(cls, type_class: Type[T]) -> T:
        if type_class.__name__ not in cls.singleton_instances:
            raise Exception("Trying to get implementation for unregistered type (%s)" % type_class.__name__)
        return cls.singleton_instances[type_class.__name__]

    @classmethod
    def register_implementation(cls, interface_type: Any, actual_type: Any):
        if interface_type.__name__ in cls.type_implementation_map:
            raise Exception("Trying to register multiple implementations for type (%s)" % interface_type.__name__)
        cls.type_implementation_map[interface_type.__name__] = actual_type

    @classmethod
    def get_new_instance_of_type(cls, interface_type: Type[T], *args: Tuple, **kwargs: Dict[str, Any]) -> T:
        if interface_type.__name__ not in cls.type_implementation_map:
            raise Exception("Unrecognized interface type %s" % interface_type.__name__)
        return cls.type_implementation_map[interface_type.__name__](*args, **kwargs)

    @classmethod
    def override_register_singleton(cls, type_class: type, instance: Any):
        if type_class.__name__ in cls.singleton_instances:
            del cls.singleton_instances[type_class.__name__]
        cls.register_instance_singleton(type_class, instance)

    @classmethod
    def clear(cls):
        cls.singleton_instances.clear()
        cls.type_implementation_map.clear()
