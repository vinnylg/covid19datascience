import pandas as pd
from unidecode import unidecode
from os.path import dirname, join

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text

def read(path:str=join(dirname(__root__),'tmp','regionais.csv'), sep=';'):
	regionais = pd.read_csv(path, sep=sep)
	regionais.columns = [ x.lower() for x in regionais.columns ]
	regionais = regionais.rename(columns={'nu_reg':'reg','nu_cep':'cep'})


	regionais['ibge'] = regionais['ibge'].apply(int)
	regionais['reg'] = regionais['reg'].apply(lambda x: int(x))
	regionais['nm_reg'] = regionais['nm_reg'].apply(lambda x: str(x).capitalize())
	regionais['nm_macro'] = regionais['nm_macro'].apply(lambda x: str(x).capitalize())
	regionais['cep'] = regionais['cep'].apply(lambda x: str(x))

	regionais.to_hdf(join(dirname(__root__),'resources','database','regionais.h5'),'regionais',index=None, encoding='utf-8-sig')

	return regionais

def load():
	return pd.read_hdf(join(dirname(__root__),'resources','database','regionais.h5'))