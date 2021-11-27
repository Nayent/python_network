import pandas as pd
from os.path import join

from libs.csv_util import csv_writer


def load_data(home_path):
    xlsx_source = join(home_path, 'Planilha Referências e Citações das Patentes.xlsx')
    csv_destiny = join(home_path, 'ref_cit_patentes.csv')

    csv_writer(csv_destiny, model_csv(xlsx_source))

    print('All Done !')


def model_csv(xlsx_source):
    data_reader = pd.read_excel(xlsx_source)

    for index, row in data_reader.iterrows():
        ref = format_value(row['REF.'])
        cit = format_value(row['CIT.'])

        is_main_pat = True if ref or cit else False


        yield {
            'id': row['Unnamed: 0'],
            'numero_patente': row['PATENTE N°'],
            'nome_patente': row['PATENTE NOME'],
            'ano': row['ANO'],
            'depositante': row['DEPOSITANTE'],
            'referencias': ref,
            'citacoes': cit,
            'is_main_pat': is_main_pat
        }
    
    print('Model CSV Done!')


def format_value(value):
    if str(value) == 'nan':
        value = None
    else:
        value = str(value).split('-')

    return value
