
import struct
import os
import math

#Structs para índice agrupado
# Esto cambia cuando se hace un insert de tablas

class Producto:
    FORMAT = 'i30sf'
    FORMAT_SIZE = struct.calcsize(FORMAT)

    def __init__(self, id=0, nombre="", precio=""): #Con valores por defecto
        self.id = id
        self.nombre = nombre.ljust(30)[:30] #Por ahora, hagamos que solo use 30 caracteres
        self.precio = precio

    def to_bytes(self):
        return struct.pack(Producto.FORMAT, self.id, self.nombre.encode('utf-8'), self.precio)

    @staticmethod
    def from_bytes(data):
        id_, nombre_, precio_ = struct.unpack(Producto.FORMAT, data)
        return Producto(id_, nombre_.decode().strip('\x00'), precio_)

    def imprimir(self):
        print(f"ID: {self.id}, Nombre: {self.nombre}, Precio: {self.precio}")

#Al trabajar con B+ Tree, trabajamos con dos tipos de nodos. Habrá uno para nodos internos y otro para hoja.
#Los nodos internos guardarán una cantidad de claves y una cantidad de posiciones, siendo superior a claves en uno.
#En el hoja, se guardarán cantidad igual de claves y posiciones, además de un puntero a un nodo siguiente
#Necesario ingresar la cantidad de hijos máximo u orden (order).
class Leaf_node:
    FORMAT = 'Bqiq' #Formato de headers: indicador de hoja, posición de nodo padre, n llaves y siguiente nodo
    FORMAT_SIZE = struct.calcsize(FORMAT)

    #Tambien hay que guardar el formato de los valores: una clave y una posición
    key_format = 'i'
    pos_format = 'q'
    key_pos_format = key_format + pos_format
    key_pos_size = struct.calcsize(key_pos_format)

    def __init__(self, parent, n_keys, nextLeaf, values):
        
        if n_keys != len(values):
            raise ValueError("La cantidad de llaves no coincide con la cantidad de valores.")

        #self.order = order    #Número de hijos como máximo. El mínimo es el techo de su mitad
        #Puede que quites order si trabajarás en IndexPage, que ya lo tiene.
        #EJM: si order es 5 => tendrá como mínimo 3 hijos y mínimo 2 claves
        self.n_keys = n_keys     #Número de llaves actualmente. Importante, que el nodo puede que no este del todo lleno
        self.values = values     #Lista de pares ordenados de clave y posiciones en el nodo . Posicion long (8 bytes: q) y clave (4 bytes: i)
        self.nextLeaf = nextLeaf   # Posición del siguiente nodo hoja
        self.parent = parent    #Posición del padre
        self.check_leaf = True  #Indicador si es hoja o no. Hoja = verdad (0 en booleano), si no es falso (1)

    #Función para buscar una clave y devolver su índice y posición
    def find_key(self, key):
      for i in range(self.n_keys):
        if self.values[i][0] == key:
          return i, self.values[i][1]
      return -1, -1

    #Podemos hacer una función que agrega un valor en el nodo, junto a su posición en archivo de datos.
    #Busca a que valor es menor. Si no lo encuentra, entonces es mayor que todos y se va al final
    def insert_key_leaf(self, key, pos):
       for i in range(self.n_keys):
        if key < self.values[i][0]: #clave en 0, posición en 1
         self.values = self.values[:i] + [[key, pos]] + self.values[i:]
         self.n_keys += 1
         return

       if key > self.values[self.n_keys-1][0]:
        self.values = self.values + [[key, pos]]
        self.n_keys += 1
        return

    #Esta será una función para eliminar una llave. Devuelve si se eliminó un valor o no
    def delete_key(self, key):
      if self.n_keys == 0:
        return False, -1, -1
      for i in range(self.n_keys):
        if self.values[i][0] == key:
          delete_value = self.values.pop(i)
          self.n_keys -= 1
          return True, i, delete_value[1]

      return False, -1, -1 #En caso no se eliminó nada, o sea, no se encontró la clave.

    #Verifica si se pasó la cantidad máxima de llaves. Acuerdate, que el número de llaves no debe superar a el orden - 1
    def more_full(self, order):
      if self.n_keys > order - 1:
        return True
      else:
        return False

    #Esta función determina si la cantidad de llaves es menor que el permitido
    #Solo funciona si no trabajamos con una raíz
    def min_allow(self, order):
      if self.parent == -1: #No tiene padre, entonces es raíz
        return False
      else:
        return self.n_keys < (math.ceil(order / 2) - 1)

    #Este será una función para almacenar en binario: si es hoja, su padre, numero de llaves y siguiente nodo.
    #Acuérdate, que tenemos un valor de headers y luego el ingreso de varias posiciones y claves
    def to_bytes(self, order):
      header_bin = struct.pack(Leaf_node.FORMAT, self.check_leaf, self.parent, self.n_keys, self.nextLeaf)
      body_bin = b'' #Para valores binarios vacios: comillas simples con una b antes
      for i in range(self.n_keys):
        body_bin += struct.pack(self.key_pos_format, self.values[i][0], self.values[i][1])
      #Tenemos los registros de claves y punteros. Pero esta función esta hecha para tenerlo listo para escribir en una posición
      #El nodo se compone de registros, y si no iguala a order - 1, entonces se llena el resto con valores por defecto.
      for i in range(self.n_keys, order - 1):
        body_bin += struct.pack(self.key_pos_format, 0, 0)
      #Ya lleno el cuerpo, retornamos el dato binario del nodo, juntando el header y cuerpo.
      return header_bin + body_bin

    #Método para leer un binario y convertirlo a un nodo hoja
    @staticmethod
    def from_bytes(data):
      header = struct.unpack(Leaf_node.FORMAT, data[:Leaf_node.FORMAT_SIZE])
      check_leaf = bool(header[0])
      parent = header[1]
      n_keys = header[2]
      nextLeaf = header[3]

      values = []
      offset = Leaf_node.FORMAT_SIZE
      for _ in range(n_keys):
        key, pos = struct.unpack(Leaf_node.key_pos_format, data[offset:offset+Leaf_node.key_pos_size])
        values.append([key, pos])  # <-- Clave, Posición
        offset += Leaf_node.key_pos_size
      return Leaf_node(parent, n_keys, nextLeaf, values)


    #Función para imprimir
    def __str__(self):
      output = "Leaf Node\n"
      output += f"  Is Leaf       : {self.check_leaf}\n"
      output += f"  Parent        : {self.parent}\n"
      output += f"  # of Keys     : {self.n_keys}\n"
      output += f"  Next Leaf     : {self.nextLeaf}\n"
      output += "  Values (key, pos):\n"
      for key, pos in self.values:
        output += f"    ({key}, {pos})\n"
      return output


#Este es un nodo interno. Dice si es hoja, posición de nodo padre y numero de llaves
class Internal_node:
    FORMAT = 'Bqi'
    FORMAT_SIZE = struct.calcsize(FORMAT)

    pos_format = 'q'
    pos_size = struct.calcsize(pos_format)

    key_format = 'i'
    key_size = struct.calcsize(key_format)

    def __init__(self, parent, n_keys, keys, children):
        #Ver si la cantidad de llaves coincide:
        if n_keys != len(keys):
            raise ValueError("La cantidad de llaves no coincide con la cantidad de valores.")

        #self.order = order    #Número de hijos como máximo. El mínimo es el techo de su mitad
        #Puede que quites order si trabajarás en IndexPage, que ya lo tiene.
        self.n_keys = n_keys         #Número de llaves actualmente
        self.children = children    #Lista con las posiciones a las páginas hijas
        self.keys = keys        #Lista de llaves
        self.parent = parent    #Posición del padre
        self.check_leaf = False  #Indicador si es hoja o no

    #Función para buscar una clave y devolver su índice y posición
    def find_key(self, key):
      for i in range(self.n_keys):
        if self.keys[i] == key:
          return i, self.children[i+1]
      return -1, -1

    #Se inserta un valor y la posición de su nodo
    #Acuérdate: que la inserción proviene de un split de un nodo hijo. Se lleva la clave mayor del nodo nuevo y su posición.
    #En el nodo actual, comparas la clave nueva con las demás y miras si es menor que alguna. Si lo es, añadir en esa posición
    #En cambio, el parámetro posición se agrega una posición más adelante
    def insert_key_internal(self, key, pos):
       for i in range(self.n_keys):
        if key < self.keys[i]:
         self.keys = self.keys[:i] + [key] + self.keys[i:]
         self.children = self.children[:i+1] + [pos] + self.children[i+1:]
         self.n_keys += 1
         return

       #En caso key es superior a cualquiera de keys (mayor, que no aceptamos duplicados de claves), pos y key se agregan al final de sus listas.
       if key > self.keys[self.n_keys-1]:
         self.keys = self.keys + [key]
         self.children = self.children + [pos]
         self.n_keys += 1

    #Esta es una función para eliminar una clave. Puede usarse cuando ocurra una fusión
    def delete_key(self, key):
      if self.n_keys == 0:
        return False, -1, -1
      for i in range(self.n_keys):
        if self.keys[i] == key:
          self.keys = self.keys[:i] + self.keys[i+1:]
          delete_children = self.children.pop(i+1)
          self.n_keys -= 1
          return True, i, delete_children[1]
          #Retornamos una verificación de si se eliminó la clave, y el índice del padre que se eliminó.

      return False, -1, -1 #En caso no se eliminó nada, o sea, no se encontró la clave.

    #Verifica si esta lleno. Acuerdate, que el número de llaves no debe superar a el orden - 1
    def more_full(self, order):
      if self.n_keys > (order - 1):
        print(f"Tienes más de {order-1} registros en este nodo hoja, que es lo máximo permitido.")
        return True
      else:
        print("Aún no se llenó tu nodo interno.")
        return False

    #Verifica si se tienen menos llaves de lo permitido, que es techo de orden entre 2, - 1
    def min_allow(self, order):
      if self.parent == -1: #No tiene padre, entonces es raíz, y no tiene límite mínimo.
        return False
      else:
        return self.n_keys < (math.ceil(order / 2) - 1)

    #Este será una función para almacenar en binario: si es hoja, su padre y numero de llaves.
    #Acuérdate, que tenemos un valor de headers y luego el ingreso de varias posiciones y claves
    def to_bytes(self, order):
      header_bin = struct.pack(Internal_node.FORMAT, self.check_leaf, self.parent, self.n_keys)
      body_bin = b'' #Para valores binarios vacios: comillas simples con una b antes

      #Primero se ingresa la primera posición, y luego se alterna entre claves y posiciones
      body_bin += struct.pack(self.pos_format, self.children[0])
      for i in range(self.n_keys):
        body_bin += struct.pack(self.key_format, self.keys[i])
        body_bin += struct.pack(self.pos_format, self.children[i+1])
      #Ahora, llenamos con valores por defecto
      for _ in range(self.n_keys, order - 1):
        body_bin += struct.pack(self.key_format, 0)
        body_bin += struct.pack(self.pos_format, 0)
      return header_bin + body_bin

    #Ahora toca hacer una función para convertir de un valor binario a un nodo interno
    @staticmethod
    def from_bytes(data):
     header = struct.unpack(Internal_node.FORMAT, data[:Internal_node.FORMAT_SIZE])

     check_leaf = bool(header[0])
     parent = header[1]
     n_keys = header[2]

     keys = []
     children = []
     offset = Internal_node.FORMAT_SIZE

     # Leer primer hijo
     first_child = struct.unpack(Internal_node.pos_format, data[offset:offset+Internal_node.pos_size])[0]
     children.append(first_child)
     offset += Internal_node.pos_size

     # Leer claves y posiciones alternamente
     for _ in range(n_keys):
         key = struct.unpack(Internal_node.key_format, data[offset:offset+Internal_node.key_size])[0]
         offset += Internal_node.key_size
         pos = struct.unpack(Internal_node.pos_format, data[offset:offset+Internal_node.pos_size])[0]
         offset += Internal_node.pos_size
         keys.append(key)
         children.append(pos)

     #Retornamos el nodo interno nuevo
     return Internal_node(parent, n_keys, keys, children)

    #Función para imprimir
    def __str__(self):
      output = "Internal Node\n"
      output += f"  Is Leaf       : {self.check_leaf}\n"
      output += f"  Parent        : {self.parent}\n"
      output += f"  # of Keys     : {self.n_keys}\n"
      output += f"  Keys          : {self.keys}\n"
      output += f"  Children Pos  : {self.children}\n"
      return output



#Creamos una clase que lo represente.
#Se guardará un archivo con los nodos y otro con los registros.
#El archivo de registros tendrá solo registros y un header con la cantidad de registros. De todo se encargará el archivo de nodos.
#El archivo de nodos guardará un header hacia el nodo raíz (posición), orden y cantidad de nodos internos y hoja
class BPlusTree:
    #Este es el formato para el archivo con nodos
    HEADER_FORMAT = 'qiii' #Posición de nodo raíz, orden, número de nodos internos y número de nodos hoja
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    #Mientras, el archivo con registros solo contiene la cantidad de registros de tipo Producto o cual escogas como header

    #Aquí se define el formato y tamaño de los datos. Por ahora producto, lo reemplazas luego con el tipo de registro que gustes.
    DATA_FORMAT = Producto.FORMAT
    DATA_SIZE = Producto.FORMAT_SIZE
    #Aún así, hay uso de Producto dentro del código. Vemos como cambiarlo para otros tipos de clase luego.

    #Vamos a leer dos archivos: uno de datos y otro de nodos
    def __init__(self, node_filename, data_filename, order=None):
      self.node_filename = node_filename
      self.data_filename = data_filename
      #Se guardará el dato en el parámetro order si no existe el archivo de nodos
      if not os.path.exists(self.node_filename) or os.path.getsize(self.node_filename) < self.HEADER_SIZE:
        if order is None: #No existe archivo ni un orden a especificar, entonces usaré por defecto que el orden es 4
          self.order = 4
        else:
          self.order = order
      self.init_BPT()

    #Llenamos los datos del header, y creamos el archivo de nodos si no existe
    def init_BPT(self):
      if not os.path.exists(self.node_filename) or os.path.getsize(self.node_filename) < self.HEADER_SIZE:
        with open(self.node_filename, 'wb') as f:
            # Raíz en -1 (no existe aún), se usa order y no hay ningun nodo creado
            f.write(struct.pack(self.HEADER_FORMAT, -1, self.order, 0, 0))
        self.root = -1
        self.n_internal_nodes = 0
        self.n_leaf_nodes = 0
      else:
        with open(self.node_filename, 'rb') as f:
            f.seek(0)
            root, order, n_internal_nodes, n_leaf_nodes = struct.unpack(self.HEADER_FORMAT, f.read(self.HEADER_SIZE))
            self.order = order
            self.root = root
            self.n_internal_nodes = n_internal_nodes
            self.n_leaf_nodes = n_leaf_nodes
      #Vemos si no existe el archivo de datos. En nuestra clase, guardamos la cantidad de registros.
      if not os.path.exists(self.data_filename) or os.path.getsize(self.data_filename) < 4: #Este tendrá solo un header con N cantidad de registros
        with open(self.data_filename, 'wb') as f:
          f.write(struct.pack('i', 0))
          self.n_data = 0
      else:
        with open(self.data_filename, 'rb') as f:
          f.seek(0)
          self.n_data = struct.unpack('i', f.read(4))[0] #Numero de registros

    #Función para imprimir
    def __str__(self):
      output = "B+ Tree\nArchivo de nodos:"
      output += f"  Order         : {self.order}\n"
      output += f"  Root          : {self.root}\n"
      output += f"  # of Internal : {self.n_internal_nodes}\n"
      output += f"  # of Leaf     : {self.n_leaf_nodes}\n"
      output += "Archivo de datos:\n"
      output += f"  # of Data     : {self.n_data}\n"
      return output

    #Una función para mostrar todos los nodos sin problema
    #Tendremos una lista que actue como cola. La llenamos con la posición raíz.
    #Mientras este llena, quitamos el último que tenga, lo hacemos clase y leemos.
    #Luego, sumamos todos las posiciones del nodo
    def recorrido_BFS(self):
      queue = [self.root]
      while queue:
        current_pos = queue.pop(0)
        node = self.read_node(current_pos)
        print(node)
        if not node.check_leaf:
          for i in range(node.n_keys + 1):
            queue.append(node.children[i])

    def mostrar_data_fisicamente(self):
        print("Contenido actual de data.dat (orden físico):")
        with open(self.data_filename, 'rb') as f:
            f.seek(0)
            total = struct.unpack('i', f.read(4))[0]
            for i in range(total):
                data_bytes = f.read(self.DATA_SIZE)
                prod = Producto.from_bytes(data_bytes)
                print(f"Pos {4 + i * self.DATA_SIZE}: ID {prod.id}, Nombre {prod.nombre.strip()}, Precio {prod.precio}")

    def write_node_file_header(self):
      with open(self.node_filename, 'r+b') as f:
        f.seek(0)
        f.write(struct.pack(self.HEADER_FORMAT, self.root, self.order, self.n_internal_nodes, self.n_leaf_nodes))

    def write_root(self, root_=None):
      with open(self.node_filename, 'r+b') as f:
        if root_ is None:
          root_ = self.root
        f.seek(0)
        f.write(struct.pack('q', root_))

    def write_order(self, order_=None):
      with open(self.node_filename, 'r+b') as f:
        if order_ is None:
          order_ = self.order
        f.seek(8)
        f.write(struct.pack('i', order_))

    def write_n_internal_nodes(self, ni_nodes_=None):
      if ni_nodes_ is None:
        ni_nodes_ = self.n_internal_nodes
      with open(self.node_filename, 'r+b') as f:
        f.seek(12)
        f.write(struct.pack('i', ni_nodes_))

    def write_n_leaf_nodes(self, nl_nodes_=None):
      if nl_nodes_ is None:
        nl_nodes_ = self.n_leaf_nodes
      with open(self.node_filename, 'r+b') as f:
        f.seek(16)
        f.write(struct.pack('i', nl_nodes_))

    def write_n_data(self, n_data_=None):
      if n_data_ is None:
        n_data_ = self.n_data
      with open(self.data_filename, 'r+b') as f:
        f.seek(0)
        f.write(struct.pack('i', n_data_))

    #Se me pasó xd: función para ver el máximo y mínimo de claves según el orden
    def min_keys_leaf(self):
      return math.ceil(self.order / 2) - 1

    def max_keys_leaf(self):
      return self.order - 1

    def min_children_internal(self):
      return math.ceil(self.order / 2)

    def max_children_internal(self):
      return self.order

    #Por qué usar 'r+b' y no 'wb'? Porque el segundo borra todo lo del archivo y sobreescribe. El primero permite leer y escribir, sin borrar nada.

    #Función para obtener el tamaño de un nodo, ya sea hoja o no
    #Un nodo hoja tiene tres valores en header (FORMAT_SIZE) y luego varios valores pares (value_format_size), llegando a ser order - 1
    #Un nodo interno tiene 4 valores en el header , con n claves y n+1 hijos
    def node_size(self, isLeaf):
      if isLeaf:
        return Leaf_node.FORMAT_SIZE + Leaf_node.key_pos_size * (self.order - 1)
      else:
        return Internal_node.FORMAT_SIZE + Internal_node.key_size * (self.order - 1) + Internal_node.pos_size * (self.order)

    #Función para añadir un nodo al final de la cantidad de nodos actuales (puede que haya algunos sin uso por eliminación, visto luego)
    #Servirá cuando quieras ingresar un nodo nuevo luego de split.
    #Retornará la dirección donde se ingresó en el archivo de nodos. Actualiza la cantidad de nodos en la clase
    def add_node(self, node):
      print("Iniciamos el proceso de agregar un nuevo nodo al final de registros de nodos")
      new_pos = self.HEADER_SIZE + self.n_leaf_nodes * self.node_size(True) + self.n_internal_nodes * self.node_size(False)
      with open(self.node_filename, 'r+b') as f:
        f.seek(new_pos)
        f.write(node.to_bytes(self.order))
      if node.check_leaf:
        self.n_leaf_nodes += 1
        self.write_n_leaf_nodes()
        print("Se ingresó un nuevo nodo hoja en la posición " + str(new_pos))
      else:
        self.n_internal_nodes += 1
        self.write_n_internal_nodes()
        print("Se ingresó un nuevo nodo interno en la posición " + str(new_pos))

      print("Numero de nodos internos: "+str(self.n_internal_nodes)+".")
      print("Numero de nodos hoja: "+str(self.n_leaf_nodes)+".")

      #Podemos añadir el caso de que el nodo sera una nueva raíz, y no exista una raíz.
      if node.parent == -1 and self.root == -1:
        print("El nodo que añadiste se convertirá en el nuevo nodo raíz.")
        self.root = new_pos
        self.write_root(self.root)
        print("Mira como quedó tu nodo raíz:")
        print(node)
      return new_pos

    #Función para leer un nodo en una posición. Retorna el nodo que representa
    def read_node(self, pos):
      with open(self.node_filename, 'rb') as f:
        f.seek(pos)
        isLeaf = bool(struct.unpack('B',f.read(1))[0])
        f.seek(pos)
        if isLeaf:
          node_data = f.read(self.node_size(True))
          return Leaf_node.from_bytes(node_data)
        else:
          node_data = f.read(self.node_size(False))
          return Internal_node.from_bytes(node_data)

    #Función para escribir un nodo en una posición
    def write_node(self, node, pos):
      with open(self.node_filename, 'r+b') as f:
        f.seek(pos)
        f.write(node.to_bytes(self.order))

    #Función para buscar un nodo hoja
    def find_leaf(self, clave):
      print("\nVamos a buscar un nodo hoja que posiblemente contenga al registro con clave "+str(clave))
      current_pos = self.root
      while True:
        with open(self.node_filename, 'rb') as f:
          f.seek(current_pos)
          is_leaf = bool(struct.unpack('B',f.read(1))[0])
          f.seek(current_pos)

          if not is_leaf:
            print("Se realiza un búsqueda en el nodo interno de posición "+str(current_pos))
            node_data = f.read(self.node_size(False))
            nodo = Internal_node.from_bytes(node_data)
            print(f"Las claves del nodo interno son: {nodo.keys}")

            #Ya que no es hoja, se compara con las llaves, y si es menor a un se va a su posición en lista de posiciones.
            #Si se pasó la cantidad de llaves, entonces la clave debe estar en la última posición en la lista
            #Esta es una forma óptima de hallar la página a donde ir.
            i = 0
            while i < nodo.n_keys and clave > nodo.keys[i]:
              i += 1
            current_pos = nodo.children[i]
          else:
            print(f"Posiblemente, nuestra clave {clave} este en el nodo hoja en posición {current_pos}")
            return current_pos

    #Escribe un registro al final. Devuelve su posición, que puede usarse en el nodo hoja. Actualiza la cantidad de registros en la clase
    def add_data(self, data):
        print("Se añadirá un nuevo registro de clave " + str(data.id))
        productos = []

        # Leer todos los productos existentes
        with open(self.data_filename, 'rb') as f:
            f.seek(4)
            for _ in range(self.n_data):
                prod_bytes = f.read(self.DATA_SIZE)
                producto = Producto.from_bytes(prod_bytes)
                productos.append(producto)

        # Insertar ordenadamente por id
        insertado = False
        for i in range(len(productos)):
            if data.id < productos[i].id:
                productos.insert(i, data)
                insertado = True
                break
        if not insertado:
            productos.append(data)

        # Escribir de nuevo todo el archivo ordenado
        with open(self.data_filename, 'wb') as f:
            f.write(struct.pack('i', len(productos)))  # actualizar header
            for producto in productos:
                f.write(producto.to_bytes())

        self.n_data = len(productos)
        self.write_n_data()

        # Calcular posición lógica del dato insertado
        for i, producto in enumerate(productos):
            if producto.id == data.id:
                pos = 4 + i * self.DATA_SIZE
                print(f"Insertado ordenadamente en posición {pos}")
                return pos

        return -1  # no debería llegar aquí


    #Función para leer un registro, lo devuelve como una instancia de clase Producto.
    def read_data(self, pos):
      with open(self.data_filename, 'rb') as f:
        f.seek(pos)
        data_bytes = f.read(self.DATA_SIZE)
        return Producto.from_bytes(data_bytes)

    
    #Ahora, vamos a crear una función para ingresar un registro en un nodo hoja
    def insert_in_leaf(self, leaf_node, data):
      print("\nIniciamos el proceso de inserción en un nodo hoja.")
      exist_pos = self.internal_search(data.id) #Se comprueba si existe otro registro con misma clave
      if exist_pos is not None:
        print("Ya existe un registro con esa clave.")
        return

      print("Vamos a insertar en nuestro nodo hoja un nuevo registro")
      #Ahora insertamos en el nodo hoja. Primero se hace en registro
      data_pos = self.add_data(data) #Solo se guarda el registro, aun no los nodos
      print("Se ingresó un nuevo registro en la posición " + str(data_pos))
      leaf_node.insert_key_leaf(data.id, data_pos)
      print("Se ingresó un nuevo registro en el nodo hoja")

    #Función para hacer split a un nodo hoja lleno
    def split_leaf_node(self, node):
      print("\nIniciamos el proceso de split en nodo hoja.")
      #Lo primero: parte la cantidad de registros
      data1 = [] #Parte con claves inferiores
      data2 = [] #Parte con claves superiores
      mid_pos = math.floor(node.n_keys / 2) #Posición del medio
      for i in range(mid_pos):
        data1.append(node.values[i])
      for i in range(mid_pos, node.n_keys):
        data2.append(node.values[i])
      print("Partimos la lista de valores en dos.")
      print(f"Parte 1: {data1}")
      print(f"Parte 2: {data2}")
      #Ahora, cambiamos los datos de los dos nodos
      #Algunas cosas: tu nodo existente apuntaba a un nodo siguiente (o ninguno representado con -1)
      #Pero ahora apuntará a tu nodo nuevo que tiene valores que siguen inmediatamente
      #Por ello: creas tu nodo nuevo, que su siguiente nodo será el que era del anterior, sus valores y su tamaño propios
      new_node = Leaf_node(node.parent, len(data2), node.nextLeaf, data2)
      new_node_pos = self.add_node(new_node)
      #Y cambias valores en tu nodo existente. Lo guardas en su archivo en la función de inserción, aquí no.
      node.n_keys = mid_pos
      node.nextLeaf = new_node_pos
      node.values = data1

      print("Veamos como quedaron los nodos hoja luego del split: ")
      print(node)
      print(new_node)
      #Retornamos la dirección del nodo nuevo y la clave que representa
      return new_node_pos, new_node.values[0][0]

    #Función para insertar en el nodo padre.
    #Tendremos primero dos casos: el padre no existe (posición de padre = -1) o si existe
    #Para el primero: creamos un nuevo nodo raíz, le añadimos las posiciones de hijos y cambiamos sus punteros a padre
    def insert_in_parent(self, parent_pos, left_child_pos, right_child_pos, new_key): #Acuérdate, el hijo derecho se insertó recientemente con una clave.
      print("Ya hicimos el split, entonces debemos ingresar una clave y posicion al nodo padre, en posición "+ str(parent_pos))
      if parent_pos == -1: #O sea, creamos el nuevo nodo raíz
        #Para que funcione, hacemos que root = -1, y así add_node guardará al nuevo nodo como raíz.
        print("¡Mira esto! Tu nodo anterior ya era la raíz. Entonces, creemos un nuevo nodo raíz")
        new_root = Internal_node(-1, 1, [new_key], [left_child_pos, right_child_pos])
        self.root = -1
        print(f"Por ahora, el nodo raíz es {self.root}")
        self.add_node(new_root) #La función ya actualiza la dirección del nuevo nodo raíz en archivo y clase B+ Tree. Toca cambiar pos a padres
        left_child_node = self.read_node(left_child_pos)
        right_child_node = self.read_node(right_child_pos)
        left_child_node.parent = self.root
        right_child_node.parent = self.root
        self.write_node(left_child_node, left_child_pos)
        self.write_node(right_child_node, right_child_pos)
        print("Mostremos como quedaron los nodos hermanos")
        print(left_child_node)
        print(right_child_node)

      #Segundo caso: si existe el nodo padre, entonces la altura es mayor o igual que dos. Como nodo, le agregamos nuestro nodo
      #Si el nodo interno se llenó, no hay problema y terminamos
      #Si no, entramos en el proceso de split en un nodo interno, que varia un poco con el de nodo hijo
      else:
        print("Dentro de insert_in__parent, vamos a ingresar un nuevo valor al padre, de clave "+ str(new_key))
        parent_node = self.read_node(parent_pos)
        parent_node.insert_key_internal(new_key, right_child_pos)
        if parent_node.more_full(self.order):
          print("!Oye¡ El nodo al que insertaste ahora también se llenó. Tendrás que insertar al padre del padre xd")
          #Haces uso de una función de split para nodos internos, que retorne la dirección del nuevo nodo y su clave
          #Luego escribes como quedó el nodo interno existente (por los cambios en split interno)
          #Haces uso de la inserción en el nodo padre, es decir, usas esta misma función
          new_node_pos, new_key = self.split_internal_node(parent_node)
          self.write_node(parent_node, parent_pos)
          self.insert_in_parent(parent_node.parent, parent_pos, new_node_pos, new_key)
        else:
          self.write_node(parent_node, parent_pos)

    #Hacemos nuestra función de split en nodos internos
    #Es un poco más distinto: partes en dos las claves, tomas el valor de la mitad y lo quitas de ambas claves
    def split_internal_node(self, node):
      print("\nIniciamos el proceso de split en un nodo interno.")
      #Lo primero: parte la cantidad de registros
      keys1 = [] #Llaves inferiores
      keys2 = [] #Llaves superiores
      pos1 = [] #Posiciones inferiores
      pos2 = [] #Posiciones superiores

      mid_pos = math.floor((node.n_keys-1) / 2)
      mid_key = node.keys[mid_pos] #Posición del medio, que debe subir.

      for i in range(mid_pos):
        keys1.append(node.keys[i])
      for i in range(mid_pos+1, node.n_keys):
        keys2.append(node.keys[i])

      for i in range(mid_pos+1):
        pos1.append(node.children[i])
      for i in range(mid_pos+1, node.n_keys+1):
        pos2.append(node.children[i])

      print("Partimos la lista de claves en dos.")
      print(f"Parte 1: {keys1}")
      print(f"Parte 2: {keys2}")
      print("Partimos la lista de hijos en dos.")
      print(f"Parte 1: {pos1}")
      print(f"Parte 2: {pos2}")

      #Creas un nodo con los valores del lado superior y lo registras
      new_node = Internal_node(node.parent, len(keys2), keys2, pos2)
      new_node_pos = self.add_node(new_node)

      #Luego, a tu nodo existente le cambias sus valores
      node.n_keys = mid_pos
      node.keys = keys1
      node.children = pos1

      #Algo más: ya que tienes hijos que por ahora apuntan al padre antiguo, deben los de las claves posteriores apuntar al nuevo padre:
      for i in range(len(new_node.children)):
        child_node = self.read_node(new_node.children[i])
        child_node.parent = new_node_pos
        self.write_node(child_node, new_node.children[i])

      print("Veamos como quedaron los nodos internos luego del split")
      print(node)
      print(new_node)
      #Y por último devuelves la posición del nuevo nodo y su clave para ingresar
      return new_node_pos, mid_key

    #Vamos a implementar la opción de búsqueda para nuestro árbol. Devolverá una posición del archivo de registros
    def internal_search(self, clave):
      current_pos = self.root  # Comenzamos desde la raíz
      #Pero puede que no existan nodos en el archivo. Obviamente no hay registros
      if current_pos == -1:
        return None

      #Ahora toca buscar desde el nodo raíz.
      #Entonces, buscamos el nodo hoja donde puede que se encuentre la clave.
      leaf_pos = self.find_leaf(clave)
      leaf_nodo = self.read_node(leaf_pos)
      print(f"Las claves del nodo hoja son: {leaf_nodo.values}")

      #Ahora, dentro del nodo hoja, buscamos entre sus claves.
      indice, posicion = leaf_nodo.find_key(clave)
      if indice != -1 and posicion != -1:
        print(f"Clave encontrada en el índice {indice} y en la posición {posicion} del archivo de registros.")
        return posicion

      #Y en caso no se encuentre:
      print("Clave no encontrada")
      return None

    #Ahora vamos a usar la posición para buscar en el registro
    def search(self, clave):
      print("\nIniciamos el proceso de busqueda.")
      data_pos = self.internal_search(clave)
      if data_pos is not None:
        print("Clave encontrada en posición " + str(data_pos))
        with open(self.data_filename, 'rb') as f:
          f.seek(data_pos)
          data_bytes = f.read(self.DATA_SIZE)
          return Producto.from_bytes(data_bytes)
      else:
          print("No se encontró el registro")
          return None

  #Ahora, vamos a insertar un valor en el registro.
  #Lo primero será buscar dentro de los nodos internos hasta llegar al nodo hoja
  #Luego se insertará en el nodo hoja la clave, además de hacerlo en el registro
  #Si no se llenó, entonces solo se guarda el registro, y la clave con la posición en el
  #Si el nodo hoja se llenó, se realizará un split (se retornará la posición del nuevo nodo creado y la llave para ascender)
  #Se escribe el nodo actual (que tiene la mitad inferior por el split), además de añadir el nuevo nodo
  #Luego, se usa una función que inserta la clave y posición del nodo nuevo en el nodo padre
    def insert(self, registro):
      print("\n!Iniciamos el proceso de inserción¡ Se intentará insertar un registro con clave "+str(registro.id))
      current_pos = self.root
      #Si no existe raíz, eso significa que no existe registros. Entonces solo ingresamos nuestro registro.
      if current_pos == -1:
        print("¡Oye! Parece que no existían nodos. Entonces lo vamos a crear")
        current_pos = self.add_data(registro)
        print("Se ingresó un nuevo registro en el archivo de datos, en la posición "+str(current_pos)+" con clave "+str(registro.id))
        new_root = Leaf_node(-1,1,-1,[[registro.id, current_pos]]) #Puntero a padre, número de hijos, siguiente nodo, lista de valores
        current_pos = self.add_node(new_root) #Ahora escribimos el nodo raíz. Dentro se guarda que este será nodo raíz.
        print("Se creó el nodo raíz en la posición "+str(current_pos))
        print("Características del nuevo nodo raíz:")
        print(new_root)
        return

      #Buscamos al nodo hoja que puede contenerlo
      leaf_pos = self.find_leaf(registro.id)
      leaf_nodo = self.read_node(leaf_pos)

      #Ahora lo agregamos en el nodo hoja (aun no en el archivo)
      self.insert_in_leaf(leaf_nodo, registro)

      print("Dentro de insert, vamos a comprobar si tu nodo hoja se llenó.")
      #Vemos si se llenó el nodo hoja
      if leaf_nodo.more_full(self.order):
        print("!Mira¡ Se llenó tu nodo hoja. Vamos a realizar el proceso de split en hoja.")
        #Si este se llenó, vamos a hacer split. Crea nuevo nodo y lo archiva, cambia info de nodo existente (aquí lo archivamos)
        #Devuelve la posición del nuevo nodo y su llave, para inserción en nodo padre. Archiva al nodo nuevo.
        new_node_pos, new_key = self.split_leaf_node(leaf_nodo)

        #Guardamos nuestro nodo existente, que se cambio sus datos en el split
        self.write_node(leaf_nodo, leaf_pos)

        #Haremos la inserción, donde cualquier otra acción se realiza en insert_in_parent
        #Tendremos una función para insertar en el nodo padre.
        self.insert_in_parent(leaf_nodo.parent, leaf_pos, new_node_pos, new_key)
        self.write_node_file_header()
        self.write_n_data(self.n_data)
      else:
        print("Aún no se llenó tu nodo hoja. Solo ingresamos")
        #Si no se llenó, solo guardamos el nodo hoja
        self.write_node(leaf_nodo, leaf_pos)
        self.write_node_file_header()
        self.write_n_data(self.n_data)
      print("Un ultimo vistazo al nodo hoja.")
      print(leaf_nodo)


#eliminación

#Función para manejar el caso de que se tengan menos claves en el nodo hoja de lo esperado
    def handle_leaf_underflow(self, leaf_node, leaf_pos, del_index, del_key): #del_index: índice del nodo hoja del que se eliminó un valor (clave, posición).
      #Vemos que tal esta el padre:
      parent_pos = leaf_node.parent
      parent_node = self.read_node(parent_pos)

      left_sibling_pos = -1
      right_sibling_pos = -1
      left_sibling_node = None
      right_sibling_node = None

      idx_in_parent = 0

      #Buscamos al hijo entre sus claves:
      for i in range(len(parent_node.children)):
        if parent_node.children[i] == leaf_pos:
          idx_in_parent = i
          print(f"En el nodo padre, nuestro nodo hoja se encontraba en la posición {i} de la lista de hijos.")
          break

      print("Veamos que hermanos puede tener.")
      if idx_in_parent > 0:
        left_sibling_pos = parent_node.children[idx_in_parent-1]
        left_sibling_node = self.read_node(left_sibling_pos)
        print("Nuestro nodo tiene un hermano izquierdo, que su posición en el archivo de nodos es "+str(left_sibling_pos)+". Demosle un vistazo:")
        print(left_sibling_node)
      if idx_in_parent < parent_node.n_keys:
        right_sibling_pos = parent_node.children[idx_in_parent+1]
        right_sibling_node = self.read_node(right_sibling_pos)
        print("Nuestro nodo tiene un hermano derecho, que su posición en el archivo de nodos es "+str(right_sibling_pos)+". Demosle un vistazo:")
        print(right_sibling_node)

      #Hay que revisar si estos tienen más del mínimo permitido de llaves. Empezando desde la izquierda.
      if left_sibling_node is not None and left_sibling_node.n_keys > self.min_keys_leaf():
        #En la redistribución, vas a sacar un elemento del nodo izquierdo, y lo agregaras al derecho.
        #UNa cosa: los nodos hijos que del nodo padre interno que tengan un índice superior a 0, al cambiar su clave, solo afectan al padre.
        #Por ello, luego de los cambios se llama al padre para que cambie en esa posición la clave.
        self.redistribution_leaf_2(leaf_node, leaf_pos, left_sibling_node, left_sibling_pos, parent_node, parent_pos, idx_in_parent, del_index, del_key, True)
        return

      elif right_sibling_node is not None and right_sibling_node.n_keys > self.min_keys_leaf():
        #Esto puede ser algo distinto:
        #Se va a sacar un elemento de la derecha para dárselo a la izquierda.
        #Si el nodo de la izquierda esta vacío (creo que ocurre cuando el order es 3 o 4), entonces le das una nueva clave, y tendrás que buscar
        #en todo el árbol la clave y reemplazarlo con la nueva que extrajiste de la derecha.
        #Luego, el de la derecha es un nodo que en el padre tiene un índice superior a 0, por lo que también lo cambias
        self.redistribution_leaf_2(leaf_node, leaf_pos, right_sibling_node, right_sibling_pos, parent_node, parent_pos, idx_in_parent, del_index, del_key, False)
        return

      #Si no se paró con los return, entonces los hermanos inmediatos ya alcanzaron el mínimo permitido. Solo queda fusionar.
      if right_sibling_node is not None:
        self.merge_leaf(leaf_node, leaf_pos, right_sibling_node, right_sibling_pos, parent_node, parent_pos, idx_in_parent, del_index, del_key, True)

#Función para cambiar una clave por otra, usado cuando se elimina un valor que era clave
    def change_delete_key(self, old_key, new_key):
        print(f"\nreemplazaremos la clave eliminada {old_key} por la clave nueva {new_key}")
        rep_key_pos = self.root #Nodo donde puede encontrarse la clave eliminada.
        while True:
          #Vas a buscar entre las claves, primero revisando si el valor es igual, y si no, se sigue viendo si es menor
          internal_node = self.read_node(rep_key_pos) #Clase nodo interno

          #Si es nodo hoja, paramos
          if internal_node.check_leaf:
            print(f"Nos encontramos en un nodo hoja, de claves {internal_node.values}. Paramos")
            return

          print("Se busca la llave "+str(old_key)+" para ser reemplazada por "+str(new_key)+", esto en la siguiente lista:")
          print(internal_node.keys)
          for i in range(internal_node.n_keys+1):
            if i == internal_node.n_keys: #No es menor a ninguno, entonces debe encontrarse en la última página
              rep_key_pos = internal_node.children[i]
              print("Vamos a bajar un nivel, llegando a la página con valores mayores a "+str(old_key)+" de posición "+str(rep_key_pos))
              break
            elif internal_node.keys[i] == old_key: #Listo, lo encontraste, ahora lo reemplazas por la nueva llave
              internal_node.keys[i] = new_key
              print("Se encontró. Se reemplaza la llave "+str(old_key)+" por "+str(new_key)+".\nPARAMOS el cambio de claves.")
              break
            elif internal_node.keys[i] < old_key: #No es igual, entonces vemos si es menor.
              rep_key_pos = internal_node.children[i]
              print("Vamos a bajar un nivel, llegando a la página con valores menores a "+str(old_key))

        #Hicimos nuestro trabajo de reemplazar en ese nodo, y ahora hay que guardarlo.
        print("Se rompió el While, o sea, se encontró la llave. Solo guardamos como quedo este nodo interno.")
        self.write_node(internal_node, rep_key_pos) #Solo se cambió el valor de clave eliminado por uno existente, nada más.

#Otra alternativa de la función de redistribución
#Para no perderse: tienes el nodo y su posición en el nodo de registros del nodo hoja donde se eliminó un registro, de su hermano y su padre.
#También guardas en qué posición de lista de nodos hijos del nodo padre esta tu nodo principal (idx_in_parent)
#Además, del índice en la lista de posiciones de nodos hijos de donde se eliminó tu registro del nodo principal, y la llave que estaba ahí.
#Por último, un indicador de si la redistribución es con el hermano izquierdo o no.
    def redistribution_leaf_2(self, leaf_node, leaf_pos, sibling_node, sibling_pos, parent_node, parent_pos, idx_in_parent, del_index, del_key, isLeft):
      if isLeft:
        print("Sacaremos el registro del nodo hermano izquierdo")
        #Sacamos el registro del nodo izquierdo. Es su valor más a la derecha
        key_pos_moved = sibling_node.values.pop(sibling_node.n_keys-1)
        print(f"Removimos un registro del nodo hermano izquierdo, con clave y posición {key_pos_moved}.")
        #Lo insertamos en el nodo hermano derecho
        leaf_node.insert_key_leaf(key_pos_moved[0], key_pos_moved[1])
        print("Se inserto el registro en el nodo principal. Demosle un vistazo al nodo:")
        print(leaf_node)
        #Usamos idx_in_parent, para que este tenga de clave a la nueva clave de nuestro nodo
        parent_node.keys[idx_in_parent-1] = leaf_node.values[0][0]

        print("Veamos como quedaron el nodo de donde se eliminó, el nodo hermano izquierdo, y el padre de ambos.")

      else:
        print("Sacaremos el registro del nodo hermano derecho")
        key_pos_moved = sibling_node.values.pop(0)
        print(f"Removimos un registro del nodo hermano derecho, con clave y posición {key_pos_moved}.")
        leaf_node.insert_key_leaf(key_pos_moved[0], key_pos_moved[1])
        print("Se inserto el registro en el nodo principal. Demosle un vistazo al nodo:")
        print(leaf_node)

        #Hay que ver si nuestro nodo donde se eliminó ahora tiene 1 solo registro.
        #Si es así, entonces antes tuvo 0 registros, y necesita cambiar la clave antigua en el árbol por la que tiene actualmente.
        if leaf_node.n_keys == 1:
          self.change_delete_key(del_key, leaf_node.values[0][0])
          #Puede que cambie el nodo padre por ello. Por si acaso lo cargamos de nuevo
          parent_node = self.read_node(parent_pos)

        #Luego, en el padre cambiamos la clave para el nodo hermano, que es otra ya que la que era clave se prestó al hermano
        parent_node.keys[idx_in_parent-1] = sibling_node.values[0][0]

        print("Veamos como quedaron el nodo de donde se eliminó, el nodo hermano derecho, y el padre de ambos.")

      #Con ello, concluimos y solo queda guardar.
      self.write_node(leaf_node, leaf_pos)
      self.write_node(sibling_node, sibling_pos)
      self.write_node(parent_node, parent_pos)

      #Un vistazo a cada nodo.
      print(leaf_node)
      print(sibling_node)
      print(parent_node)

#Función para fusionar hojas.
  # --- Completa merge_leaf ---
    def merge_leaf(self, leaf_node, leaf_pos, sibling_node, sibling_pos, parent_node, parent_pos, idx_in_parent, del_index, del_key, isLeft):
      if not isLeft:
        print("Vamos a realizar la fusión con el hermano de la derecha. ")
        print("Veamos que nodos fusionar. Primero el de la izquierda y luego el de la derecha:")
        print(leaf_node)
        print(sibling_node)
        for i in range(sibling_node.n_keys):
          leaf_node.insert_key_leaf(sibling_node.values[i][0], sibling_node.values[i][1])
        leaf_node.n_keys += sibling_node.n_keys
        leaf_node.nextLeaf = sibling_node.nextLeaf

        # Eliminar clave y puntero del nodo padre
        key_eliminada = parent_node.keys.pop(idx_in_parent)
        parent_node.children.pop(idx_in_parent+1)
        parent_node.n_keys -= 1

        print("Actualizamos el nodo padre luego de la fusión")

        # Guardar nodos actualizados
        self.write_node(leaf_node, leaf_pos)
        self.write_node(parent_node, parent_pos)

        # Si el padre también se quedó con pocas claves, manejamos underflow recursivamente
        if parent_node.parent != -1 and parent_node.min_allow(self.order):
          print("El nodo padre ahora tiene menos claves de las permitidas. Se maneja underflow.")
          self.handle_internal_underflow(parent_node, parent_pos)

        print("Fusión completa")

      
  # --- Añade delete_data ---
    def delete_data(self, clave):
        print(f"Vamos a eliminar físicamente el registro con clave {clave}.")
        productos = []
        with open(self.data_filename, 'rb') as f:
            f.seek(4)
            for _ in range(self.n_data):
                prod_bytes = f.read(self.DATA_SIZE)
                prod = Producto.from_bytes(prod_bytes)
                if prod.id != clave:
                    productos.append(prod)

        with open(self.data_filename, 'wb') as f:
            f.write(struct.pack('i', len(productos)))
            for producto in productos:
                f.write(producto.to_bytes())

        self.n_data = len(productos)


# --- Implementa handle_internal_underflow ---
    def handle_internal_underflow(self, internal_node, internal_pos):
        print("[Implementación] Manejo de underflow en nodo interno.")
        if internal_node.parent == -1:
            if internal_node.n_keys == 0:
                new_root_pos = internal_node.children[0]
                new_root = self.read_node(new_root_pos)
                new_root.parent = -1
                self.root = new_root_pos
                self.write_node(new_root, new_root_pos)
                self.write_root(self.root)
                print("La raíz se ha reemplazado con su único hijo.")
            return

        parent_pos = internal_node.parent
        parent_node = self.read_node(parent_pos)
        idx = parent_node.children.index(internal_pos)

        left_sibling_pos = parent_node.children[idx - 1] if idx > 0 else None
        right_sibling_pos = parent_node.children[idx + 1] if idx < parent_node.n_keys else None

        if left_sibling_pos:
            left_node = self.read_node(left_sibling_pos)
            if left_node.n_keys > self.min_children_internal() - 1:
                key_move = parent_node.keys[idx - 1]
                pos_move = left_node.children.pop()
                k_move = left_node.keys.pop()
                internal_node.children.insert(0, pos_move)
                internal_node.keys.insert(0, key_move)
                parent_node.keys[idx - 1] = k_move

                self.write_node(left_node, left_sibling_pos)
                self.write_node(internal_node, internal_pos)
                self.write_node(parent_node, parent_pos)
                return

        if right_sibling_pos:
            right_node = self.read_node(right_sibling_pos)
            if right_node.n_keys > self.min_children_internal() - 1:
                key_move = parent_node.keys[idx]
                pos_move = right_node.children.pop(0)
                k_move = right_node.keys.pop(0)
                internal_node.children.append(pos_move)
                internal_node.keys.append(key_move)
                parent_node.keys[idx] = k_move

                self.write_node(right_node, right_sibling_pos)
                self.write_node(internal_node, internal_pos)
                self.write_node(parent_node, parent_pos)
                return

        if right_sibling_pos:
            right_node = self.read_node(right_sibling_pos)
            k_merge = parent_node.keys.pop(idx)
            internal_node.keys.append(k_merge)
            internal_node.keys.extend(right_node.keys)
            internal_node.children.extend(right_node.children)
            parent_node.children.pop(idx + 1)
            internal_node.n_keys = len(internal_node.keys)
            parent_node.n_keys -= 1
            self.write_node(internal_node, internal_pos)
            self.write_node(parent_node, parent_pos)
            if parent_node.min_allow(self.order):
                self.handle_internal_underflow(parent_node, parent_pos)

        elif left_sibling_pos:
            left_node = self.read_node(left_sibling_pos)
            k_merge = parent_node.keys.pop(idx - 1)
            left_node.keys.append(k_merge)
            left_node.keys.extend(internal_node.keys)
            left_node.children.extend(internal_node.children)
            parent_node.children.pop(idx)
            left_node.n_keys = len(left_node.keys)
            parent_node.n_keys -= 1
            self.write_node(left_node, left_sibling_pos)
            self.write_node(parent_node, parent_pos)
            if parent_node.min_allow(self.order):
                self.handle_internal_underflow(parent_node, parent_pos)

# --- Añade delete ---
    def delete(self, clave):
        print(f"\n¡Iniciamos el proceso de eliminación de la clave {clave}!")
        leaf_pos = self.find_leaf(clave)
        leaf_node = self.read_node(leaf_pos)

        idx, data_pos = leaf_node.find_key(clave)
        if idx == -1:
            print("La clave no se encontró en el nodo hoja.")
            return False

        success, del_index, del_data_pos = leaf_node.delete_key(clave)
        if not success:
            print("No se pudo eliminar la clave del nodo hoja.")
            return False

        print("Clave eliminada correctamente del nodo hoja.")
        self.write_node(leaf_node, leaf_pos)

        if leaf_node.min_allow(self.order):
            print("El nodo hoja tiene menos claves de las permitidas. Manejo de underflow iniciado.")
            self.handle_leaf_underflow(leaf_node, leaf_pos, del_index, clave)

        self.delete_data(clave)
        self.write_node_file_header()
        self.write_n_data()
        return True
    
    
    def search_all(self, key):
        pos = self.root
        if pos == -1:
            return []
        leaf_pos = self.find_leaf(key)
        leaf_node = self.read_node(leaf_pos)
        results = []
        i = 0
        # Busca índice inicial dentro del nodo hoja
        while i < leaf_node.n_keys and leaf_node.values[i][0] < key:
            i += 1
        # Recorre registros con clave igual, incluso en hojas sucesivas
        while leaf_pos != -1:
            while i < leaf_node.n_keys and leaf_node.values[i][0] == key:
                data_pos = leaf_node.values[i][1]
                results.append(self.read_data(data_pos))
                i += 1
            if i >= leaf_node.n_keys:
                leaf_pos = leaf_node.nextLeaf
                if leaf_pos != -1:
                    leaf_node = self.read_node(leaf_pos)
                    i = 0
                else:
                    break
            else:
                break
        return results

    def range_search(self, begin_key, end_key):
        pos = self.root
        if pos == -1:
            return []
        leaf_pos = self.find_leaf(begin_key)
        leaf_node = self.read_node(leaf_pos)
        results = []
        i = 0
        # Empieza en el primer índice con clave >= begin_key
        while i < leaf_node.n_keys and leaf_node.values[i][0] < begin_key:
            i += 1
        # Recorre nodos hoja sucesivos y registros dentro del rango
        while leaf_pos != -1:
            while i < leaf_node.n_keys and leaf_node.values[i][0] <= end_key:
                data_pos = leaf_node.values[i][1]
                results.append(self.read_data(data_pos))
                i += 1
            if i >= leaf_node.n_keys:
                leaf_pos = leaf_node.nextLeaf
                if leaf_pos != -1:
                    leaf_node = self.read_node(leaf_pos)
                    i = 0
                else:
                    break
            else:
                break
        return results




# --- Función de prueba para validar la eliminación en casos de esquina ---
def test_eliminacion_extremos():
    bpt = BPlusTree("nodos.dat", "data.dat", order=4)
    print("\n[PRUEBA] Inserción de registros para prueba de eliminación en extremos")
    registros = [
        Producto(10, "A", 1.0),
        Producto(20, "B", 2.0),
        Producto(30, "C", 3.0),
        Producto(40, "D", 4.0),
        Producto(50, "E", 5.0),
        Producto(60, "F", 6.0),
        Producto(70, "G", 7.0)
    ]

    for r in registros:
      bpt.insert(r)

    bpt.mostrar_data_fisicamente()
    print("\n[PRUEBA] Eliminando claves en posiciones de esquina: primera, última y mitad")
    extremos = [10, 70, 40]
    for clave in extremos:
      print(f"\nEliminando clave: {clave}")
      bpt.delete(clave)
      bpt.mostrar_data_fisicamente()
      print("\nEstado del árbol tras eliminación:")
      bpt.recorrido_BFS()

    print("\n[PRUEBA COMPLETADA] Se probaron eliminaciones en extremos con fusión y redistribución")



if __name__ == "__main__":
    #bpt = BPlusTree("nodos.dat", "data.dat", order=4)
    #bpt.insert(Producto(5, "Café", 3.5))
    #bpt.insert(Producto(2, "Arroz", 1.8))
    #bpt.insert(Producto(7, "Pan", 0.5))
    #bpt.insert(Producto(1, "Azúcar", 2.2))
    #bpt.mostrar_data_fisicamente()
    test_eliminacion_extremos()