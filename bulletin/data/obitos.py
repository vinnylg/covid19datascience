import pandas as pd
import numpy as np
from datetime import date
from os.path import dirname, join
from hashlib import md5

from bulletin import __file__ as __root__
from bulletin.data import casos
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_date, normalize_municipios, normalize_igbe

def read(path:str=join(dirname(__root__),'tmp','cievs.xlsx'), sheet:str='Obitos', cols:str='B,D:G,I'):
	obitos = pd.read_excel(path,sheet,usecols=cols)
	print(f"{path}:{sheet} loaded")

	obitos.columns = [ normalize_text(x) for x in obitos.columns ]
	obitos = obitos.rename(columns={'ibge_res_pr':'ibge','municipio':'mun_resid','data_do_obito':'dt_obt'})

	obitos['nome'] = obitos['nome'].apply(normalize_text)
	obitos['sexo'] = obitos['sexo'].apply(normalize_text)
	obitos['idade'] = obitos['idade'].apply(normalize_number)
	obitos['ibge'] = obitos['ibge'].apply(normalize_igbe)
	obitos['mun_resid'] = obitos['mun_resid'].apply(normalize_municipios)
	obitos['dt_obt'] = obitos['dt_obt'].apply(normalize_date)

	obitos = obitos.loc[obitos['mun_resid']!='excluir']

	obitos['hash'] = obitos.apply(lambda row: md5(str.encode(row['nome']+str(row['idade'])+row['mun_resid'])).hexdigest(), axis=1)

	# obitos = obitos[['hash','nome','sexo','idade','ibge','mun_resid','dt_obt']]
	obitos = obitos[['hash','dt_obt']]
	obitos.to_pickle(join(dirname(__root__),'tmp','cievs_obitos.pkl'))
	print(f"{join(dirname(__root__),'tmp','cievs_obitos.pkl')} saved")

	return obitos

def load():
	try:
		obitos = pd.read_pickle(join(dirname(__root__),'tmp','cievs_obitos.pkl'))
		print(f"{join(dirname(__root__),'tmp','cievs_obitos.pkl')} loaded")
	except:
		obitos = read()

	return obitos