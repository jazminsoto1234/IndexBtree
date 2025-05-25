import struct
import os
import math

class KeyHandler:
    def __init__(self, tipo: str, size=50):
        self.tipo = tipo
        self.size = size

    def serialize(self, key):
        if self.tipo == 'int':
            return struct.pack('i', key)
        elif self.tipo == 'float':
            return struct.pack('f', key)
        elif self.tipo == 'str':
            return key.encode('utf-8')[:self.size].ljust(self.size, b'\x00')

    def deserialize(self, data):
        if self.tipo == 'int':
            return struct.unpack('i', data)[0]
        elif self.tipo == 'float':
            return struct.unpack('f', data)[0]
        elif self.tipo == 'str':
            return data.rstrip(b'\x00').decode('utf-8')

    def compare(self, a, b):
        return (a > b) - (a < b)

class RecordGeneric:
    type_map = {
        int: 'i',
        float: 'f',
        str: '50s'
    }

    def __init__(self, attribute_names):
        self._attributes = attribute_names
        for name in attribute_names:
            setattr(self, name, None)

    def build_format(self):
        self.FORMAT = ''
        for attr in self._attributes:
            val = getattr(self, attr)
            attr_type = type(val) if val is not None else str
            self.FORMAT += self.type_map.get(attr_type, '50s')
        self.FORMAT_SIZE = struct.calcsize(self.FORMAT)

    def to_bytes(self):
        values = []
        for attr in self._attributes:
            val = getattr(self, attr)
            if isinstance(val, str):
                val_bytes = val.encode('utf-8')[:50].ljust(50, b'\x00')
                values.append(val_bytes)
            else:
                values.append(val)
        return struct.pack(self.FORMAT, *values)

    def from_bytes(self, data):
        values = struct.unpack(self.FORMAT, data)
        for attr, val in zip(self._attributes, values):
            if isinstance(val, bytes):
                setattr(self, attr, val.rstrip(b'\x00').decode('utf-8'))
            else:
                setattr(self, attr, val)
        return self

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self._attributes}

class LeafNode:
    HEADER_FORMAT = 'Bqiq'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, parent, n_keys, next_leaf, values, key_handler):
        self.is_leaf = True
        self.parent = parent
        self.n_keys = n_keys
        self.next_leaf = next_leaf
        self.values = values
        self.key_handler = key_handler

    def to_bytes(self, order):
        key_size = self.key_handler.size if self.key_handler.tipo == 'str' else 4
        header = struct.pack(self.HEADER_FORMAT, self.is_leaf, self.parent, self.n_keys, self.next_leaf)
        body = b''
        for i in range(order - 1):
            if i < self.n_keys:
                key = self.key_handler.serialize(self.values[i][0])
                pos = struct.pack('q', self.values[i][1])
            else:
                key = self.key_handler.serialize("" if self.key_handler.tipo == 'str' else 0)
                pos = struct.pack('q', 0)
            body += key + pos
        final = header + body
        expected = self.HEADER_SIZE + (key_size + 8) * (order - 1)
        assert len(final) == expected, f"Leaf node size mismatch: {len(final)} vs {expected}"
        return final

    @staticmethod
    def from_bytes(data, order, key_handler):
        header = struct.unpack(LeafNode.HEADER_FORMAT, data[:LeafNode.HEADER_SIZE])
        is_leaf, parent, n_keys, next_leaf = header
        offset = LeafNode.HEADER_SIZE
        values = []
        key_size = key_handler.size if key_handler.tipo == 'str' else 4
        for _ in range(n_keys):
            key = key_handler.deserialize(data[offset:offset + key_size])
            offset += key_size
            pos = struct.unpack('q', data[offset:offset + 8])[0]
            offset += 8
            values.append([key, pos])
        return LeafNode(parent, n_keys, next_leaf, values, key_handler)

    def __str__(self):
        out = f"LeafNode (parent={self.parent}, next_leaf={self.next_leaf}, keys={self.n_keys})\\n"
        for k, p in self.values:
            out += f"  Key: {k}, Pos: {p}\\n"
        return out
    
class InternalNode:
    HEADER_FORMAT = 'Bqi'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, parent, n_keys, children, keys, key_handler):
        self.is_leaf = False
        self.parent = parent
        self.n_keys = n_keys
        self.children = children
        self.keys = keys
        self.key_handler = key_handler

    def to_bytes(self, order):
        key_size = self.key_handler.size
        header = struct.pack(self.HEADER_FORMAT, self.is_leaf, self.parent, self.n_keys)
        body = b''
        for i in range(order):
            ptr = self.children[i] if i < len(self.children) else -1
            body += struct.pack('q', ptr)
        for i in range(order - 1):
            key = self.key_handler.serialize(self.keys[i]) if i < len(self.keys) else self.key_handler.serialize("")
            body += key
        return header + body

    @staticmethod
    def from_bytes(data, order, key_handler):
        header = struct.unpack(InternalNode.HEADER_FORMAT, data[:InternalNode.HEADER_SIZE])
        is_leaf, parent, n_keys = header
        offset = InternalNode.HEADER_SIZE
        children = []
        for _ in range(order):
            ptr = struct.unpack('q', data[offset:offset + 8])[0]
            offset += 8
            children.append(ptr)
        keys = []
        key_size = key_handler.size
        for _ in range(order - 1):
            key = key_handler.deserialize(data[offset:offset + key_size])
            offset += key_size
            keys.append(key)
        return InternalNode(parent, n_keys, children, keys, key_handler)

class BPlusTree:
    HEADER_FORMAT = 'qii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, node_file, data_file, order=4, key_type='str', key_size=50):
        self.node_file = node_file
        self.data_file = data_file
        self.order = order
        self.key_handler = KeyHandler(key_type, key_size)
        self.record_count = 0
        self.root_pos = -1

        if not os.path.exists(node_file):
            with open(node_file, 'wb') as f:
                f.write(struct.pack(self.HEADER_FORMAT, -1, order, 0))
        else:
            with open(node_file, 'rb') as f:
                self.root_pos, self.order, self.record_count = struct.unpack(self.HEADER_FORMAT, f.read(self.HEADER_SIZE))

        if not os.path.exists(data_file):
            with open(data_file, 'wb') as f:
                f.write(struct.pack('i', 0))

    def node_size(self):
        key_size = self.key_handler.size if self.key_handler.tipo == 'str' else 4
        return LeafNode.HEADER_SIZE + (key_size + 8) * (self.order - 1)

    def write_node(self, node, pos):
        with open(self.node_file, 'r+b') as f:
            f.seek(pos)
            f.write(node.to_bytes(self.order))

    def read_node(self, pos):
        with open(self.node_file, 'rb') as f:
            f.seek(pos)
            data = f.read(self.node_size())
            is_leaf = struct.unpack('B', data[0:1])[0]
            if is_leaf:
                return LeafNode.from_bytes(data, self.order, self.key_handler)
            else:
                return InternalNode.from_bytes(data, self.order, self.key_handler)

    def append_node(self, node):
        pos = self.HEADER_SIZE + self.node_size() * self.record_count
        self.write_node(node, pos)
        self.record_count += 1
        with open(self.node_file, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack(self.HEADER_FORMAT, self.root_pos, self.order, self.record_count))
        return pos
    
    def insert_record(self, record: RecordGeneric):
        key = getattr(record, record._attributes[0])
        with open(self.data_file, 'ab') as f:
            f.write(record.to_bytes())
        self.insert(key, self.record_count)
        self.record_count += 1

    def insert(self, key, data_pos):
        if self.root_pos == -1:
            new_node = LeafNode(-1, 1, -1, [[key, data_pos]], self.key_handler)
            self.root_pos = self.append_node(new_node)
            with open(self.node_file, 'r+b') as f:
                f.seek(0)
                f.write(struct.pack('q', self.root_pos))
        else:
            leaf = self.read_node(self.root_pos)
            if leaf.n_keys >= self.order - 1:
                raise ValueError("Nodo hoja lleno: aún no se implementa split.")
            leaf.values.append([key, data_pos])
            leaf.values.sort(key=lambda x: x[0])
            leaf.n_keys += 1
            self.write_node(leaf, self.root_pos)

    def insert(self, key, data_pos):
        if self.root_pos == -1:
            leaf = LeafNode(-1, 1, -1, [[key, data_pos]], self.key_handler)
            self.root_pos = self.append_node(leaf)
            return

        result = self._insert_recursive(self.root_pos, key, data_pos)
        if result is not None:
            new_key, new_pos = result
            old_root = self.read_node(self.root_pos)
            new_root = InternalNode(-1, 1, [self.root_pos, new_pos], [new_key], self.key_handler)
            self.root_pos = self.append_node(new_root)
            with open(self.node_file, 'r+b') as f:
                f.seek(0)
                f.write(struct.pack('q', self.root_pos))

    def _insert_recursive(self, pos, key, data_pos):
        node = self.read_node(pos)

        if node.is_leaf:
            node.values.append([key, data_pos])
            node.values.sort(key=lambda x: x[0])
            if len(node.values) < self.order:
                node.n_keys = len(node.values)
                self.write_node(node, pos)
                return None
            else:
                mid = len(node.values) // 2
                left = node.values[:mid]
                right = node.values[mid:]
                node.values = left
                node.n_keys = len(left)
                new_leaf = LeafNode(-1, len(right), node.next_leaf, right, self.key_handler)
                new_pos = self.append_node(new_leaf)
                node.next_leaf = new_pos
                self.write_node(node, pos)
                return right[0][0], new_pos
        else:
            i = 0
            while i < node.n_keys and self.key_handler.compare(key, node.keys[i]) >= 0:
                i += 1
            result = self._insert_recursive(node.children[i], key, data_pos)
            if result is None:
                return None
            new_key, new_child_pos = result
            node.keys.insert(i, new_key)
            node.children.insert(i + 1, new_child_pos)
            node.n_keys += 1

            if node.n_keys < self.order:
                self.write_node(node, pos)
                return None
            else:
                mid = node.n_keys // 2
                promote_key = node.keys[mid]
                left_keys = node.keys[:mid]
                right_keys = node.keys[mid + 1:]
                left_children = node.children[:mid + 1]
                right_children = node.children[mid + 1:]

                node.keys = left_keys
                node.children = left_children
                node.n_keys = len(left_keys)

                new_internal = InternalNode(-1, len(right_keys), right_children, right_keys, self.key_handler)
                new_pos = self.append_node(new_internal)
                self.write_node(node, pos)
                return promote_key, new_pos

    def print_tree(self, pos=None, nivel=0):
        indent = '  ' * nivel
        if pos is None:
            pos = self.root_pos
        node = self.read_node(pos)
        if node.is_leaf:
            print(f"{indent}[Hoja] Pos: {pos}")
            for k, p in node.values:
                print(f"{indent}  {k} → {p}")
        else:
            print(f"{indent}[Interno] Pos: {pos}")
            for i in range(node.n_keys):
                self.print_tree(node.children[i], nivel + 1)
                print(f"{indent}  Key[{i}]: {node.keys[i]}")
            self.print_tree(node.children[node.n_keys], nivel + 1)
    def search(self, key):
        if self.root_pos == -1:
            return []
        return self._search_in_leaf(self.root_pos, key)

    def _search_in_leaf(self, pos, key):
        node = self.read_node(pos)
        if node.is_leaf:
            return [pair for pair in node.values if self.key_handler.compare(pair[0], key) == 0]
        else:
            i = 0
            while i < node.n_keys and self.key_handler.compare(key, node.keys[i]) >= 0:
                i += 1
            return self._search_in_leaf(node.children[i], key)

    def range_search(self, start_key, end_key):
        if self.root_pos == -1:
            return []
        results = []
        self._range_collect(self.root_pos, start_key, end_key, results)
        return results

    def _range_collect(self, pos, start_key, end_key, results):
        node = self.read_node(pos)
        if node.is_leaf:
            for key, value in node.values:
                if self.key_handler.compare(start_key, key) <= 0 <= self.key_handler.compare(end_key, key):
                    results.append((key, value))
            next_pos = node.next_leaf
            if next_pos != -1:
                self._range_collect(next_pos, start_key, end_key, results)
        else:
            i = 0
            while i < node.n_keys and self.key_handler.compare(start_key, node.keys[i]) > 0:
                i += 1
            self._range_collect(node.children[i], start_key, end_key, results)

