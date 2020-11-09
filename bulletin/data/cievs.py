import pandas as pd
import numpy as np
from datetime import date
from os.path import dirname, join
import hashlib

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_date, normalize_municipios, normalize_igbe

def add_dt_com(value, newdate: date=date.today()):
	if pd.isna(value):
		value = newdate
	return value

def read_casos(path:str=join(dirname(__root__),'tmp','cievs.xlsx'), sheet:str='Casos confirmados', cols:str='B,D:H,J:L'):
	casos = pd.read_excel(path,sheet,usecols=cols)
	print(f"{path}:{sheet} loaded")
	casos.columns = [ normalize_text(x) for x in casos.columns ]
	casos = casos.rename(columns={'comunicacao':'dt_com','ibge_res_pr':'ibge'})

	casos['nome'] = casos['nome'].apply(normalize_text)
	casos['sexo'] = casos['sexo'].apply(normalize_text)
	casos['idade'] = casos['idade'].apply(lambda x: normalize_number(x,fill=0))
	casos['ibge'] = casos['ibge'].apply(normalize_igbe)
	casos['mun_resid'] = casos['mun_resid'].apply(normalize_text)
	casos['mun_atend'] = casos['mun_atend'].apply(normalize_text)
	casos['laboratorio'] = casos['laboratorio'].apply(normalize_text)
	casos['dt_com'] = casos['dt_com'].apply(normalize_date)
	casos['dt_diag'] = casos['dt_diag'].apply(normalize_date)

	casos.to_hdf(join(dirname(__root__),'tmp','cievs_casos.h5'),'casos',index=None, encoding='utf-8-sig')
	print(f"{join(dirname(__root__),'tmp','cievs_casos.h5')} saved")

	return casos

def read_obitos(path:str=join(dirname(__root__),'tmp','cievs.xlsx'), sheet:str='Obitos', cols:str='D:G,I'):
	obitos = pd.read_excel(path,sheet,usecols=cols)
	print(f"{path}:{sheet} loaded")

	obitos.columns = [ normalize_text(x) for x in obitos.columns ]
	obitos = obitos.rename(columns={'municipio':'mun_resid','data_do_obito':'dt_obt'})

	obitos['nome'] = obitos['nome'].apply(normalize_text)
	obitos['sexo'] = obitos['sexo'].apply(normalize_text)
	obitos['idade'] = obitos['idade'].apply(normalize_number)
	# obitos['ibge'] = obitos['ibge'].apply(normalize_igbe)
	obitos['mun_resid'] = obitos['mun_resid'].apply(normalize_municipios)
	obitos['dt_obt'] = obitos['dt_obt'].apply(normalize_date)

	obitos.to_hdf(join(dirname(__root__),'tmp','cievs_obitos.h5'),'obitos',index=None, encoding='utf-8-sig')
	print(f"{join(dirname(__root__),'tmp','cievs_obitos.h5')} saved")

	return obitos

def load_casos():
	try:
		casos = pd.read_hdf(join(dirname(__root__),'tmp','cievs_casos.h5'))
		print(f"{join(dirname(__root__),'tmp','cievs_casos.h5')} loaded")
	except:
		casos = read_casos()

	return casos

def load_obitos():
	try:
		obitos = pd.read_hdf(join(dirname(__root__),'tmp','cievs_obitos.h5'))
		print(f"{join(dirname(__root__),'tmp','cievs_obitos.h5')} loaded")
	except:
		obitos = read_obitos()

	return obitos