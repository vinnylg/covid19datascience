import pandas as pd
import numpy as np
from datetime import date
from os.path import dirname, join
from hashlib import md5

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_date, normalize_municipios, normalize_igbe

def read(path:str=join(dirname(__root__),'tmp','notifica.csv')):
    df = pd.read_csv(path)
    print(f"{path} loaded")
    df.columns = [ normalize_text(x) for x in df.columns ]
    covid_cases = df.rename(columns={'id':'nu_not','paciente':'name','sexo':'sex', 'nome_mae':'mom',
                                 'raca_cor':'race','uf_residencia':'uf_res', 'ibge_residencia':'ibge_res',
                                 'uf_unidade_notifica':'uf_not', 'ibge_unidade_notifica':'ibge_not',
                                 'classificacao_final':'classification', 'evolucao':'evolution',
                                 'data_cura_obito':'dt_evo'})

    # casos['nome'] = casos['nome'].apply(normalize_text)
    covid_cases['dt_not'] = covid_cases['dt_not'].apply(normalize_date)
    #covid_cases['name'] = covid_cases['name'].apply(normalize_text)
    covid_cases['sex'] = covid_cases['sex'].apply(normalize_text)
    covid_cases['dt_nasc'] = covid_cases['dt_nasc'].apply(normalize_date)
    #covid_cases['mom'] = covid_cases['mom'].apply(normalize_text)
    #covid_cases['ibge_res'] = covid_cases['ibge_res'].apply(normalize_igbe)
    #covid_cases['ibge_not'] = covid_cases['ibge_not'].apply(normalize_igbe)
    covid_cases['dt_evo'] = covid_cases['dt_evo'].apply(normalize_date)
    #covid_cases['metodo'] = covid_cases['metodo'].apply(normalize_text)

    covid_cases['hash1'] = covid_cases.apply(lambda row: md5(str.encode(normalize_text(row['name']) + str(row['dt_nasc']) + normalize_text(row['mom']))).hexdigest(), axis=1)
    covid_cases['hash2'] = covid_cases.apply(lambda row: md5(str.encode(normalize_text(row['name']) + str(row['dt_nasc']))).hexdigest(), axis=1)
    covid_cases['hash3'] = covid_cases.apply(lambda row: md5(str.encode(normalize_text(row['name']) + normalize_text(row['mom']))).hexdigest(), axis=1)
    
    covid_cases = covid_cases[['se', 'nu_not', 'dt_not', 'name', 'sex', 'dt_nasc','mom', 'race',
                               'uf_res', 'ibge_res', 'uf_not', 'ibge_not', 'classification','evolution',
                               'dt_evo', 'metodo', 'hash1', 'hash2', 'hash3']]

    #covid_cases = covid_cases.loc[(covid_cases["classification"] == 2)]
    #notifica_deaths = covid_cases.loc[covid_cases["evolution"] == 2]
    
    covid_cases.to_pickle(join(dirname(__root__),'tmp','covid_cases.pkl'))
    print(f"{join(dirname(__root__),'tmp','covid_cases.pkl')} saved")

    return covid_cases

def load():
    try:
        covid_cases = pd.read_pickle(join(dirname(__root__),'tmp','covid_cases.pkl'))
        print(f"{join(dirname(__root__),'tmp','covid_cases.pkl')} loaded")
    except:
        covid_cases = read()

    return covid_cases