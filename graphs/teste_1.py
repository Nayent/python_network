from pyvis.network import Network
from os.path import join


def load_graph(home_path):

    destiny = join(home_path, 'test_1.html')

    net = Network()

    nodes = [2, 3, 4, 5]

    net.add_node(1, label="Node 1")
    net.add_nodes(nodes)
    
    g = Network()
    g.add_nodes([1,2,3], value=[10, 100, 400],
                         title=['I am node 1', 'node 2 here', 'and im node 3'],
                         x=[21.4, 54.2, 11.2],
                         y=[100.2, 23.54, 32.1],
                         label=['NODE 1', 'NODE 2', 'NODE 3'],
                         color=['#00ff1e', '#162347', '#dd4b39'])
    
    net.show(destiny)