import pandas as pd
import numpy as np
from datetime import date
from os.path import dirname, join
from hashlib import md5

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_date, normalize_municipios, normalize_igbe

def add_dt_com(value, newdate: date=date.today()):
	if pd.isna(value):
		value = newdate
	return value

def read(path:str=join(dirname(__root__),'tmp','cievs.xlsx'), sheet:str='Casos confirmados', cols:str='B,D:H,J:L'):
	casos = pd.read_excel(path,sheet,usecols=cols)
	print(f"{path}:{sheet} loaded")
	casos.columns = [ normalize_text(x) for x in casos.columns ]
	casos = casos.rename(columns={'comunicacao':'dt_com','ibge_res_pr':'ibge'})

	# casos['nome'] = casos['nome'].apply(normalize_text)
	casos['sexo'] = casos['sexo'].apply(normalize_text)
	casos['idade'] = casos['idade'].apply(lambda x: normalize_number(x,fill=0))
	casos['ibge'] = casos['ibge'].apply(normalize_igbe)
	casos['mun_resid'] = casos['mun_resid'].apply(normalize_municipios)
	casos['mun_atend'] = casos['mun_atend'].apply(normalize_municipios)
	# casos['laboratorio'] = casos['laboratorio'].apply(normalize_text)
	casos['dt_com'] = casos['dt_com'].apply(normalize_date)
	casos['dt_diag'] = casos['dt_diag'].apply(normalize_date)

	casos = casos.loc[casos['mun_resid']!='excluir']

	casos['hash'] = casos.apply(lambda row: md5(str.encode(normalize_text(row['nome']) + str(row['idade']) + row['mun_resid'])).hexdigest(), axis=1)

	casos = casos[['hash','nome','sexo','idade','ibge','mun_resid','mun_atend','laboratorio','dt_com','dt_diag']]
	# casos = casos[['hash','sexo','idade','ibge','mun_resid','mun_atend','laboratorio','dt_com','dt_diag']]
	casos.to_pickle(join(dirname(__root__),'tmp','cievs_casos.pkl'))
	print(f"{join(dirname(__root__),'tmp','cievs_casos.pkl')} saved")

	return casos

def load():
	try:
		casos = pd.read_pickle(join(dirname(__root__),'tmp','cievs_casos.pkl'))
		print(f"{join(dirname(__root__),'tmp','cievs_casos.pkl')} loaded")
	except:
		casos = read()

	return casos