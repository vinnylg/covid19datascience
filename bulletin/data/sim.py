import pandas as pd
import numpy as np
from datetime import date
from os.path import dirname, join
from hashlib import md5

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_date, normalize_municipios, normalize_igbe

def read(path:str=join(dirname(__root__), 'tmp', 'sim.dbf')):
    #Abre e le arquivos em DBF
    df = Dbf5(path, codec = 'cp860').to_dataframe()
    print(f"{path} loaded")
    df.columns = [ normalize_text(x) for x in df.columns ]
    covid_deaths = pd.DataFrame(df, columns = ['numerodo', 'dtcadastro', 'nome', 'sexo', 'dtnasc',
                                        'nomemae', 'racacor', 'codestres', 'codmunres', 'codestocor',
                                        'codmunocor', 'causabas', 'causabas_o', 'dtobito'])
    covid_deaths = pd.DataFrame(df, columns = ['numerodo', 'dtcadastro', 'nome', 'sexo', 'dtnasc',
                                        'nomemae', 'racacor', 'codestres', 'codmunres', 'codestocor',
                                        'codmunocor', 'causabas', 'causabas_o', 'dtobito'])
    
    #covid_deaths["system"]
    #covid_deaths['dt_not'] = covid_deaths['dt_not'].apply(normalize_date)
    #covid_deaths['name'] = covid_deaths['name'].apply(normalize_text)
    covid_deaths['sex'] = covid_deaths['sex'].apply(normalize_text)
    #covid_deaths['dt_nasc'] = covid_deaths['dt_nasc'].apply(normalize_date)
    #covid_deaths['mom'] = covid_deaths['mom'].apply(normalize_text)
    #covid_deaths['ibge_res'] = covid_deaths['ibge_res'].apply(normalize_igbe)
    #covid_deaths['ibge_not'] = covid_deaths['ibge_not'].apply(normalize_igbe)
    #covid_deaths['dt_evo'] = covid_deaths['dt_evo'].apply(normalize_date)
    
    #Cria combinacoes hash
    covid_deaths['hash1'] = covid_deaths.apply(lambda row: md5(str.encode(normalize_text(row['name']) + str(row['dt_nasc']) + normalize_text(row['mom']))).hexdigest(), axis=1)
    covid_deaths['hash2'] = covid_deaths.apply(lambda row: md5(str.encode(normalize_text(row['name']) + str(row['dt_nasc']))).hexdigest(), axis=1)
    covid_deaths['hash3'] = covid_deaths.apply(lambda row: md5(str.encode(normalize_text(row['name']) + normalize_text(row['mom']))).hexdigest(), axis=1)
    
    #Filtra casos e obitos confirmados
    covid_deaths = pd.DataFrame(covid_deaths, columns = ['nu_not', 'dt_not', 'name', 'sex', 'dt_nasc','mom', 'race',
                                                 'uf_res', 'ibge_res', 'uf_not', 'ibge_not', 'classification',
                                                 'evolution', 'dt_evo', 'hash1', 'hash2', 'hash3'])
    covid_deaths = covid_deaths.loc[(covid_deaths["classification"] == 'B342')]
    
    #Cria arquivos pkl
    covid_deaths.to_pickle(join(dirname(__root__),'tmp','covid_deaths.pkl'))
    print(f"{join(dirname(__root__),'tmp','covid_deaths.pkl')} saved")
    
	return covid_deaths

def load():
	try:
		covid_deaths = pd.read_pickle(join(dirname(__root__),'tmp','covid_deaths.pkl'))
		print(f"{join(dirname(__root__),'tmp','covid_deaths.pkl')} loaded")
	except:
		covid_deaths = read()

	return covid_deaths