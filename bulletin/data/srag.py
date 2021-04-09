import pandas as pd
import numpy as np
from simpledbf import Dbf5
from datetime import date
from os.path import dirname, join
from hashlib import md5

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_date, normalize_municipios, normalize_igbe

def read(path:str=join(dirname(__root__), 'tmp', 'srag.dbf')):
    #Abre e le arquivos em DBF
    df = Dbf5(path, codec = 'cp860').to_dataframe()
    print(f"{path} loaded")
    df.columns = [ normalize_text(x) for x in df.columns ]
    srag_cases = pd.DataFrame(df, columns = ['sem_not', 'nu_notific', 'dt_notific', 'nm_pacient', 'cs_sexo', 'dt_nasc',
                                        'nm_mae_pac', 'cs_raca', 'sg_uf', 'co_mun_res', 'sg_uf_not','co_mun_not',
                                        'classi_fin', 'evolucao', 'dt_evolucao', 'tp_amostra'])
    srag_cases = srag_cases.rename(columns={'sem_not':'se', 'nu_notific':'nu_not',
                                       'dt_notific':'dt_not', 'nm_pacient':'name',
                                       'cs_sexo':'sex', 'nm_mae_pac':'mom', 'cs_raca':'race',
                                       'sg_uf':'uf_res', 'co_mun_res':'ibge_res', 'sg_uf_not':'uf_not',
                                       'co_mun_not':'ibge_not', 'classi_fin':'classification',
                                        'evolucao':'evolution', 'dt_evolucao':'dt_evo', 'tp_amostra':'metodo'})
    
    #Normaliza series - Obs: Criar srag_cases["system"]
    srag_cases['dt_not'] = srag_cases['dt_not'].apply(normalize_date)
    #srag_cases['name'] = srag_cases['name'].apply(normalize_text)
    srag_cases['sex'] = srag_cases['sex'].apply(normalize_text)
    srag_cases['dt_nasc'] = srag_cases['dt_nasc'].apply(normalize_date)
    #srag_cases['mom'] = srag_cases['mom'].apply(normalize_text)
    #srag_cases['ibge_res'] = srag_cases['ibge_res'].apply(normalize_igbe)
    #srag_cases['ibge_not'] = srag_cases['ibge_not'].apply(normalize_igbe)
    srag_cases['dt_evo'] = srag_cases['dt_evo'].apply(normalize_date)
    
    #Cria combinacoes hash
    srag_cases['hash1'] = srag_cases.apply(lambda row: md5(str.encode(normalize_text(row['name']) + str(row['dt_nasc']) + normalize_text(row['mom']))).hexdigest(), axis=1)
    srag_cases['hash2'] = srag_cases.apply(lambda row: md5(str.encode(normalize_text(row['name']) + str(row['dt_nasc']))).hexdigest(), axis=1)
    srag_cases['hash3'] = srag_cases.apply(lambda row: md5(str.encode(normalize_text(row['name']) + normalize_text(row['mom']))).hexdigest(), axis=1)
    
    #Filtra casos e obitos confirmados
    srag_cases = srag_cases = srag_cases[['se', 'nu_not', 'dt_not', 'name', 'sex', 'dt_nasc','mom',
                                          'race','uf_res', 'ibge_res', 'uf_not', 'ibge_not', 'classification',
                                          'evolution', 'dt_evo', 'metodo', 'hash1', 'hash2', 'hash3']]
    #srag_cases = srag_cases.loc[(srag_cases["classification"] == '5')]
    #srag_deaths = srag_cases.loc[srag_cases["evolution"] == '2']
    
    #Cria arquivos pkl
    srag_cases.to_pickle(join(dirname(__root__),'tmp','srag_cases.pkl'))
    print(f"{join(dirname(__root__),'tmp','srag_cases.pkl')} saved")
    #srag_deaths.to_pickle(join(dirname(__root__),'tmp','srag_deaths.pkl'))
    #print(f"{join(dirname(__root__),'tmp','srag_deaths.pkl')} saved")
    
    return srag_cases

def load():
    try:
        srag_cases = pd.read_pickle(join(dirname(__root__),'tmp','srag_cases.pkl'))
        print(f"{join(dirname(__root__),'tmp','srag_cases.pkl')} loaded")
    except:
        srag_cases = read()

    return srag_cases