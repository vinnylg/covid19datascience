import pandas as pd
from unidecode import unidecode
from os.path import dirname, join

from bulletin import __file__ as __root__
from bulletin.commom.normalize import normalize_text

def generate(path:str=join(dirname(__root__),'tmp','regionais.csv'), sep=';'):
	regionais = pd.read_csv(path, sep=sep)
	regionais.columns = [ x.lower() for x in regionais.columns ]
	regionais = regionais.rename(columns={'nu_reg':'reg','nm_reg':'nome','nm_macro':'macro','nu_cep':'cep'})


	regionais['ibge'] = regionais['ibge'].apply(lambda x: str(x))
	regionais['reg'] = regionais['reg'].apply(lambda x: int(x))
	regionais['nome'] = regionais['nome'].apply(lambda x: str(x).capitalize())
	regionais['macro'] = regionais['macro'].apply(lambda x: str(x).capitalize())
	regionais['cep'] = regionais['cep'].apply(lambda x: str(x))

	regionais.to_hdf(join(dirname(__root__),'resources','database','regionais.h5'),'regionais',index=None, encoding='utf-8-sig')

	return regionais

def load():
	return pd.read_hdf(join(dirname(__root__),'resources','database','regionais.h5'))