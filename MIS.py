import time
import random
import numpy as np
import multiprocessing
from multiprocessing import Pipe, Lock
import multiprocessing.pool

ADJ_MATRIX_FILE = 'graph1.txt'

class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass
class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess
class NestablePool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NestablePool, self).__init__(*args, **kwargs)

class Graph:
    def __init__(self, adj_mat):
        self.adj_mat = adj_mat
        self.create_nodes()
        self.connect_nodes()

    def create_nodes(self):
        degrees = self.calc_degrees()
        node_ids = set([i for i in range(len(self.adj_mat[0]))])
        self.V = [Node(i, degrees[i]) for i in node_ids]

    def calc_degrees(self):
        return multiprocessing.Pool().map(self.calc_degree, range(len(self.adj_mat[0])))
    
    def calc_degree(self, vertex):
        return sum(self.adj_mat[vertex])
    
    def connect_nodes(self):
        for i in range(len(self.adj_mat[0])):
            self.connect_node(self.V[i])

    def connect_node(self, node):
        neighbour_indexes = np.nonzero(self.adj_mat[node.id])[0]
        for i in range(len(neighbour_indexes)):
            node.connect(self.V[neighbour_indexes[i]])


class Node:
    def __init__(self, id, degree):
        self.id = id
        self.MIS = False
        self.used = False
        self.selected = False
        self.degree = degree + 1
        self.neighbours = {}

    def set_neighbour(self, neighbour_id, pipe):
        if not neighbour_id in self.neighbours:
            self.neighbours[neighbour_id] = pipe
            return True
        return False
    
    def unset_neighbour(self, neighbour_id):
        self.neighbours.pop(neighbour_id)
        self.degree -= 1

    def connect(self, node):
        conn1, conn2 = Pipe()
        if node.set_neighbour(self.id, conn2):
            self.set_neighbour(node.id, conn1)

    def inform_neighbours(self, msg):
        packages = [(key, msg) for key in self.neighbours.keys()]
        multiprocessing.Pool().map(self.inform_neighbour, packages)

    def inform_neighbour(self, payload):
        return self.neighbours.get(payload[0]).send((self.id, payload[1]))

    def check_for_messages(self):
        return multiprocessing.Pool().map(self.check_neighbour_message, self.neighbours.keys())

    def check_neighbour_message(self, neighbour_id):
        return self.neighbours.get(neighbour_id).recv()

    def delete_neighbours(self, neighbours_to_del):
        for neighbour in neighbours_to_del: 
            self.delete_neighbour(neighbour)

    def delete_neighbour(self, neighbour_to_del):
        if neighbour_to_del[1] == True:
            self.unset_neighbour(neighbour_to_del[0])


def work(node):
    node.selected = random.random() < 1 / (2 *  node.degree)
    node.inform_neighbours((node.selected, node.degree))
    competing_neighbours = list(filter(lambda msg: msg[1][0] == True, node.check_for_messages()))
    winner = find_MIS_node(node, competing_neighbours)
    node.MIS = winner == node.id
    node.inform_neighbours(winner == node.id)
    neighbours_won = list(filter(lambda msg: msg[1] == True, node.check_for_messages()))
    remove_self_from_graph = winner == node.id or len(neighbours_won) > 0
    node.inform_neighbours(remove_self_from_graph)
    node.delete_neighbours(node.check_for_messages())
    if remove_self_from_graph:
        return node.MIS
    return work(node)

def find_MIS_node(node, competing_neighbours):
    best_neighbour = find_best_neighbour(competing_neighbours)
    if not node.selected and best_neighbour == None:
        return None
    elif not node.selected:
        return best_neighbour[0]
    elif node.selected and best_neighbour == None:
        return node.id
    else:
        if best_neighbour[0] < node.id:
            return best_neighbour[0]
        else:
            return node.id

def find_best_neighbour(competitors):
    best_neighbour = None
    for competitor in competitors:
        best_neighbour = better(competitor, best_neighbour)
    return best_neighbour

def better(competitor, current_best):
    if current_best == None:
        return competitor
    if competitor[1][1] < current_best[1][1]:
        return current_best
    elif competitor[1][1] > current_best[1][1]:
        return competitor
    else:
        if competitor[0] < current_best[0]:
            return current_best
        else:
            return competitor

def string_to_matrix(source):
    lines = source.split('\n')
    return [list(map(int, line.split(','))) for line in lines]

def read_file(file):
    with open(file) as f: s = f.read()
    return s

def get_adj_matrix():
    s = read_file(ADJ_MATRIX_FILE)
    return string_to_matrix(s)

def preporcess():
    return Graph(get_adj_matrix())

def lubyMIS(graph):
    return NestablePool().map(work, graph.V)

def main():
    print("Preprocessing...")
    start_time = time.time()
    graph = preporcess()
    mid_time = time.time()
    print("Preprocessing finished in %s seconds!" % (mid_time - start_time))
    print("Calculating MIS...")
    result = lubyMIS(graph)
    end_time = time.time()
    print("Calculating MIS finished in %s seconds!" % (end_time - mid_time))
    print("--- Total execution: %s seconds ---" % (end_time - start_time))
    print("Result: " + str(result))

if __name__ == "__main__":
    main()