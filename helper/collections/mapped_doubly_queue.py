from typing import Generator, Generic, Tuple, TypeVar, Optional, Dict

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


class DLLNode(Generic[KeyT, ValueT]):
    def __init__(self, key: KeyT, value: ValueT):
        self.key: KeyT = key
        self.value: ValueT = value
        self.prev: Optional["DLLNode[KeyT, ValueT]"] = None
        self.next: Optional["DLLNode[KeyT, ValueT]"] = None


class MappedDoublyQueue(Generic[KeyT, ValueT]):
    def __init__(self):
        self.head: Optional[DLLNode[KeyT, ValueT]] = None
        self.tail: Optional[DLLNode[KeyT, ValueT]] = None
        self.map: Dict[KeyT, DLLNode[KeyT, ValueT]] = {}
        
    @property
    def is_empty(self) -> bool:
        return self.head is None

    def peek(self) -> Optional[ValueT]:
        """
        Return the value of the first element in the queue without removing it.
        """
        return self.head.value if self.head else None

    def enqueue(self, key: KeyT, value: ValueT):
        """
        Add a new element to the end of the queue.
        """
        if key in self.map:
            raise KeyError(f"Key {key} already exists in the queue.")

        new_node = DLLNode(key, value)
        self.map[key] = new_node

        if self.tail:
            self.tail.next = new_node
            new_node.prev = self.tail
        else:
            # Queue is empty, so the new node becomes both head and tail
            self.head = new_node

        self.tail = new_node

    def dequeue(self) -> ValueT:
        """
        Remove and return the value of the first element in the queue.
        """
        if not self.head:
            raise IndexError("Dequeue from an empty queue.")

        node_to_remove = self.head
        value = node_to_remove.value
        key = node_to_remove.key

        # Update head pointer
        self.head = self.head.next
        if self.head:
            self.head.prev = None
        else:
            # If head becomes None, the queue is empty, so set tail to None as well
            self.tail = None

        # Remove from map
        del self.map[key]
        return value

    def delete(self, key: KeyT) -> bool:
        """
        Remove the node with the given key from the queue.
        """
        if key not in self.map:
            return False

        node_to_remove = self.map[key]

        # Update links in the linked list
        if node_to_remove.prev:
            node_to_remove.prev.next = node_to_remove.next
        else:
            # Node to remove is the head
            self.head = node_to_remove.next

        if node_to_remove.next:
            node_to_remove.next.prev = node_to_remove.prev
        else:
            # Node to remove is the tail
            self.tail = node_to_remove.prev

        # Remove from map
        del self.map[key]
        return True
        
    def traverse(self) -> Generator[Tuple[KeyT, ValueT], None, None]:
        iter = self.head
        while iter is not None:
            yield iter.key, iter.value
            iter = iter.next
            
    def get(self, key: KeyT) -> Optional[ValueT]:
        node = self.map.get(key)
        if node is None:
            return None
        
        return node.value
            
