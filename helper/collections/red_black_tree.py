"""A minimalist Red-Black Tree implementation.

The code draws inspiration from several descriptions/code snippets for Red-Black Trees:
- https://en.wikipedia.org/wiki/Red%E2%80%93black_tree
- https://blog.boot.dev/python/red-black-tree-python/
- Chapter 13 of T. H. Cormen, et al., "Introduction to algorithms", MIT press, (2022)

The code was implemented with assistance from GitHub Copilot.
"""

from enum import Enum
from typing import Generator, Generic, Optional, Tuple, TypeVar


KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")

class _RBColor(Enum):
    Red = 0
    Black = 1

class RBNode(Generic[KeyT, ValueT]):

    def __init__(self, key: KeyT, parent=None, left=None, right=None, color=None, value=None):
        self.key = key
        self.parent = parent
        self.left = left
        self.right = right
        self.color = color
        self.value = value

    def __repr__(self):
        summary = f"Node({self.key}, color={self.color})"
        if self.parent:
            summary += f" parent={self.parent.key}"
        if self.left:
            summary += f" left={self.left.key}"
        if self.right:
            summary += f" right={self.right.key}"
        if self.value is not None:
            summary += f" value={self.value}"
        return summary


class Nil(RBNode):
    """Nil node (used to represent the leaves of the tree)."""

    def __init__(self):
        super().__init__(key="Nil", parent=None, left=None, right=None, color=_RBColor.Black)

    @staticmethod
    def __bool__():
        return False


class RedBlackTree(Generic[KeyT, ValueT]):

    def __init__(self):
        # Use a single Nil node as a "sentinel" for all leaves
        self.nil = Nil()
        self.root = self.nil
        self._minimum_node: RBNode = self.nil
        self._maximum_node: RBNode = self.nil
        
    @property
    def minimum(self) -> Optional[KeyT]:
        if self._minimum_node is self.nil:
            return None
        return self._minimum_node.key
    
    @property
    def maximum(self) -> Optional[KeyT]:
        if self._maximum_node is self.nil:
            return None
        return self._maximum_node.key

    def __repr__(self):
        return f"RedBlackTree({self.root})"

    def __search(self, key) -> RBNode:
        """Search for a node with a given key in the subtree of the given node.

        Args:
            key: the key to search for
        """
        node = self.root
        while node is not self.nil and node.key != key:
            if key < node.key:
                node = node.left
            else:
                node = node.right
        return node

    def _minimum(self, node: RBNode) -> RBNode:
        """Find the minimum node in the subtree rooted at node.

        Args:
            node: the root of the subtree to search.

        Returns:
            The minimum node in the tree rooted at node.
        """
        while node.left is not self.nil:
            node = node.left
        return node

    def _maximum(self, node: RBNode) -> RBNode:
        """Find the maximum node in the subtree rooted at node.

        Args:
            node: the root of the subtree to search.

        Returns:
            The maximum node in the tree.
        """
        while node.right is not self.nil:
            node = node.right
        return node

    def __inorder(self, node: RBNode) -> Generator[Tuple[KeyT, ValueT], None, None]:
        """Perform an inorder traversal of the tree.

        Args:
            node: Node - the root of the tree to traverse.
        """
        if node is not self.nil:
            for item in self.__inorder(node.left):
                yield item
            yield node.key, node.value
            for item in self.__inorder(node.right):
                yield item
            
    def in_order(self) -> Generator[Tuple[KeyT, ValueT], None, None]:
        for item in self.__inorder(self.root):
            yield item
            
    def __reverse_order(self, node: RBNode) -> Generator[Tuple[KeyT, ValueT], None, None]:
        if node is not self.nil:
            for item in self.__reverse_order(node.right):
                yield item
            yield node.key, node.value
            for item in self.__reverse_order(node.left):
                yield item
                
    def reverse_order(self) -> Generator[Tuple[KeyT, ValueT], None, None]:
        for item in self.__reverse_order(self.root):
            yield item

    def __preorder(self, node: RBNode) -> Generator[ValueT, None, None]:
        """Perform a preorder traversal of the tree rooted at node.

        Args:
            node: Node - the root of the tree to traverse.
        """
        if node is not self.nil:
            yield node.value
            for item in self.__preorder(node.left):
                yield item
            for item in self.__preorder(node.right):
                yield item
                
    def preorder(self) -> Generator[ValueT, None, None]:
        for item in self.__preorder(self.root):
            yield item

    def __postorder(self, node: RBNode):
        """Perform a postorder traversal of the tree rooted at node.

        Args:
            node: Node - the root of the tree to traverse.
        """
        if node is not self.nil:
            self.__postorder(node.left)
            self.__postorder(node.right)
            print(node.key, end=" ")

    def __rotate_left(self, u: RBNode):
        """Rotate the subtree rooted at u to the left."""
        v = u.right
        u.right = v.left
        if v.left != self.nil:
            v.left.parent = u
        v.parent = u.parent
        if not u.parent:
            self.root = v
        elif u == u.parent.left:
            u.parent.left = v
        else:
            u.parent.right = v
        v.left, u.parent = u, v

    def __rotate_right(self, v: RBNode):
        """Rotate the subtree rooted at v to the right."""
        u = v.left
        v.left = u.right
        if u.right != self.nil:
            u.right.parent = v
        u.parent = v.parent
        if not v.parent:
            self.root = u
        elif v == v.parent.right:
            v.parent.right = u
        else:
            v.parent.left = u
        u.right, v.parent = v, u

    def __insert(self, new_node: RBNode):
        """Insert a new node into the tree.

        Args:
            new_node: the node to insert.
        """
        if self._minimum_node is self.nil or self._minimum_node.key > new_node.key:
            self._minimum_node = new_node
        if self._maximum_node is self.nil or self._maximum_node.key < new_node.key:
            self._maximum_node = new_node
        # Typical Binary Search Tree insertion method
        node = self.root
        parent = None
        while not isinstance(node, Nil):
            parent = node
            if new_node.key < node.key:
                node = node.left
            elif new_node.key > node.key:
                node = node.right
            else:
                node.value = new_node.value
                return

        new_node.parent = parent

        if not parent:  # handle the case when the tree is empty
            self.root = new_node
        elif new_node.key < parent.key:
            parent.left = new_node
        else:
            parent.right = new_node

        # set Red-Black Tree node attributes
        new_node.left = self.nil
        new_node.right = self.nil
        new_node.color = _RBColor.Red

        self.__fix_insert_violations(new_node)

    def __fix_insert_violations(self, node: RBNode):
        """Fix any Red-Black Tree insert violations.

        Args:
            node: the node that was inserted.
        """
        while node != self.root and node.parent.color == _RBColor.Red:
            if node.parent == node.parent.parent.left:
                uncle = node.parent.parent.right
                if uncle.color == _RBColor.Red:
                    node.parent.color = _RBColor.Black
                    uncle.color = _RBColor.Black
                    node.parent.parent.color = _RBColor.Red
                    node = node.parent.parent
                else:
                    if node == node.parent.right:
                        node = node.parent
                        self.__rotate_left(node)
                    node.parent.color = _RBColor.Black
                    node.parent.parent.color = _RBColor.Red
                    self.__rotate_right(node.parent.parent)
            else:
                uncle = node.parent.parent.left
                if uncle.color == _RBColor.Red:
                    node.parent.color = _RBColor.Black
                    uncle.color = _RBColor.Black
                    node.parent.parent.color = _RBColor.Red
                    node = node.parent.parent
                else:
                    if node == node.parent.left:
                        node = node.parent
                        self.__rotate_right(node)
                    node.parent.color = _RBColor.Black
                    node.parent.parent.color = _RBColor.Red
                    self.__rotate_left(node.parent.parent)
        self.root.color = _RBColor.Black

    def __shift_nodes(self, old_node: RBNode, new_node: RBNode):
        """Replace the subtree rooted at old_node with the subtree rooted at new_node.

        Args:
            old_node: the root of the subtree to replace.
            new_node: the root of the subtree to replace with.
        """
        if not old_node.parent:
            self.root = new_node
        elif old_node == old_node.parent.left:
            old_node.parent.left = new_node
        else:
            old_node.parent.right = new_node
        new_node.parent = old_node.parent

    def __delete(self, node: RBNode):
        """Delete a node from the Red-Black Tree.

        Args:
            node: the node to delete.
        """ 
        original_color = node.color
        if node.left == self.nil:
            x = node.right
            self.__shift_nodes(node, x)
        elif node.right == self.nil:
            x = node.left
            self.__shift_nodes(node, x)
        else:
            v = self._minimum(node.right)
            original_color = v.color
            x = v.right
            if v.parent == node:
                x.parent = v
            else:
                self.__shift_nodes(v, v.right)
                v.right = node.right
                v.right.parent = v
            self.__shift_nodes(node, v)
            v.left = node.left
            v.left.parent = v
            v.color = node.color
        if original_color == _RBColor.Black:
            self.__fix_delete_violations(x)
            
        if node.key == self._maximum_node.key:
            if self.root is not self.nil:
                max_node = self._maximum(self.root)
                self._maximum_node = max_node
            else:
                self._maximum_node = self.nil
        if node.key == self._minimum_node.key:
            if self.root is not self.nil:
                min_node = self._minimum(self.root)
                self._minimum_node = min_node
            else:
                self._minimum_node = self.nil

    def __fix_delete_violations(self, node: RBNode):
        """Fix any Red-Black Tree delete violations.

        Args:
            node: the node that was deleted.
        """
        while node != self.root and node.color == _RBColor.Black:
            if node == node.parent.left:
                s = node.parent.right
                if s.color == _RBColor.Red:
                    s.color = _RBColor.Black
                    node.parent.color = _RBColor.Red
                    self.__rotate_left(node.parent)
                    s = node.parent.right
                if s.left.color == _RBColor.Black and s.right.color == _RBColor.Black:
                    s.color = _RBColor.Red
                    node = node.parent
                else:
                    if s.right.color == _RBColor.Black:
                        s.left.color = _RBColor.Black
                        s.color = _RBColor.Red
                        self.__rotate_right(s)
                        s = node.parent.right
                    s.color = node.parent.color
                    node.parent.color = _RBColor.Black
                    s.right.color = _RBColor.Black
                    self.__rotate_left(node.parent)
                    node = self.root
            else:
                s = node.parent.left
                if s.color == _RBColor.Red:
                    s.color = _RBColor.Black
                    node.parent.color = _RBColor.Red
                    self.__rotate_right(node.parent)
                    s = node.parent.left
                if s.right.color == _RBColor.Black and s.left.color == _RBColor.Black:
                    s.color = _RBColor.Red
                    node = node.parent
                else:
                    if s.left.color == _RBColor.Black:
                        s.right.color = _RBColor.Black
                        s.color = _RBColor.Red
                        self.__rotate_left(s)
                        s = node.parent.left
                    s.color = node.parent.color
                    node.parent.color = _RBColor.Black
                    s.left.color = _RBColor.Black
                    self.__rotate_right(node.parent)
                    node = self.root
        node.color = _RBColor.Black

    def __contains__(self, key) -> bool:
        """Check if the tree contains a node with the given key.

        Args:
            key: the key to search for.

        Returns:
            True if the tree contains a node with the given key, False otherwise.
        """
        return self.__search(key) is not self.nil

    def __delitem__(self, key):
        """Delete the node with the given key from the tree.

        Args:
            key: the key of the node to delete.
        """
        node = self.__search(key)
        if node is self.nil:
            raise KeyError(str(key))
        self.__delete(node)

    def __setitem__(self, key, value):
        """Insert or update node value, providing a dictionary-like interface.

        Args:
            key: the key of the new node.
            value: the value of the new node.
        """
        self.__insert(RBNode(key, value=value))

    def __getitem__(self, key) -> Optional[ValueT]:
        """Search for the value associated with the given key.

        Args:
            key: the key of the new node.

        Returns:
            The value associated with the given key.
        """
        node = self.__search(key)
        if node is self.nil:
            return None
        return node.value
    
    def insert_or_get(self, key: KeyT, value: ValueT) -> ValueT:
        node = self.__search(key)
        if node is not self.nil:
            return node.value
        else:
            self.__insert(RBNode(key=key, value=value))



