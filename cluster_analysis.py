import networkx as nx
from networkx.algorithms.community.modularity_max import greedy_modularity_communities
import random
import matplotlib
import matplotlib.pyplot as plt


def check_coverage(res_files):
    '''

    :return:
    '''

    lines = []
    for f in res_files:
        with open(f) as f:
            lines += f.readlines()

    res = []
    random.shuffle(lines)
    for i in range(0, len(lines), 1000):
        nodes = set()
        lines_ = lines[:i]
        for line in lines_:
            for n in line.strip('\n').split('\t'):
                if len(n) > 0:
                    nodes.add(n)
        res.append(len(nodes))

    plt.plot(range(0, len(lines), 1000), res)
    plt.show()

    # not converged yet

    return


def get_network(res_files):
    edges = set()
    for f in res_files:
        with open(f) as f:
            line = f.readline()
            while line:
                nodes = sorted([n for n in line.strip('\n').split('\t') if len(n) > 0])
                for i in range(len(nodes) - 1):
                    for j in range(i+1, len(nodes)):
                        edges.add((nodes[i], nodes[j]))
                line = f.readline()
    g = nx.Graph()

    g.add_edges_from(edges)

    return g


def get_clusters(g):

    res = greedy_modularity_communities(g)

    for p, i in zip(res, range(len(res))):
        print(p, file=open('p{}'.format(i), 'a+'))

    return


if __name__ == "__main__":
    # g = get_network(['log/{}_edgelist.tsv'.format(i) for i in range(5)])
    # get_clusters(g)


    check_coverage(['log/{}_edgelist.tsv'.format(i) for i in range(5)])