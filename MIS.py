import multiprocessing
import time

ADJ_MATRIX_FILE = 'graph1.txt'

class Graph:
    def __init__(self, adj_mat):
        self.adj_mat = adj_mat
        self.V = set([i for i in range(len(adj_mat[0]))])
        self.d = calc_degrees(adj_mat)

def calc_degrees(adj_mat):
    print("\tCalculating vertex degrees...")
    keys = [(vertex, adj_mat) for vertex in range(len(adj_mat[0]))]
    return multiprocessing.Pool().map(calc_degree_unpack, keys)
    
def calc_degree_unpack(args):
    return calc_degree(*args)

def calc_degree(vertex, adj_mat):
    return sum(adj_mat[vertex])

def string_to_matrix(source):
    print("\tParsing matrix...")
    lines = source.split('\n')
    return [list(map(int, line.split(','))) for line in lines]

def read_file(file):
    print("\tReading adjeciency matrix file...")
    with open(file) as f: s = f.read()
    return s

def get_adj_matrix():
    s = read_file(ADJ_MATRIX_FILE)
    return string_to_matrix(s)

def preporcess():
    graph = Graph(get_adj_matrix())
    print(graph.adj_mat)
    return graph

def luby(graph):
    
    return


def main():
    print("Preprocessing...")
    start_time = time.time()
    graph = preporcess()
    mid_time = time.time()
    print("Preprocessing finished in %s seconds!" % (mid_time - start_time))
    print("Calculating Luby...")
    luby(graph)
    end_time = time.time()
    print("Calculating Luby finished in %s seconds!" % (end_time - mid_time))
    print("--- Total execution: %s seconds ---" % (end_time - start_time))


if __name__ == "__main__":
    main()
