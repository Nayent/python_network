from pyvis.network import Network
from os.path import join
from libs.csv_util import load_csv_dict
import ipdb


def load_graph(home_path):

    csv_source = 'data/ref_cit_patentes.csv'
    destiny = join(home_path, 'test_2.html')

    data_dict = load_csv_dict(csv_source, 'id')

    g = Network()
    
    # Adding NODES
    for id, row in data_dict.items():
        if eval(row['is_main_pat']):
            color = 'black'
            title = 'Main Node'
        else:
            color = 'grey'
            title = 'Sec Node'

        g.add_node(
            id,
            value=200,
            title=title,
            label=row['nome_patente'],
            color=color
        )
    
    # Adding REFERENCIAS
    for id, row in data_dict.items():
        if not eval(row['is_main_pat']):
            continue

        if not row['referencias']:
            continue

        for edge in eval(row['referencias']):
            g.add_edge(id, edge, color='red')

    # Adding CITACOES
    for id, row in data_dict.items():
        if not eval(row['is_main_pat']):
            continue
    
        if not row['citacoes']:
            continue

        for edge in eval(row['citacoes']):
            g.add_edge(id, edge, color='blue')
    
    g.show(destiny)
    #Teste de github