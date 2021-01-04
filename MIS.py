

ADJ_MATRIX_FILE = 'graph1.txt'

def string_to_matrix(source):
    lines = source.split('\n')
    return [line.split(',') for line in lines]

def get_adj_matrix():
    with open(ADJ_MATRIX_FILE) as f: s = f.read()
    return string_to_matrix(s)

get_adj_matrix();