import pandas as pd
from os.path import dirname, join
from bulletin import __file__ as __root__

__municipios = pd.read_csv(join(dirname(__root__),'resources','csv','municipios.csv'), dtype={'ibge':str})
__populacao = pd.read_csv(join(dirname(__root__),'resources','csv','populacao.csv'), dtype={'ibge':str})[['ibge','municipio']]

municipios_sesa_ibge = pd.merge(left=__municipios, right=__populacao, how='outer', on='ibge', suffixes=('_sesa','_ibge'))

municipios = pd.read_csv(join(dirname(__root__),'resources','csv','populacao.csv'))
regionais = pd.read_csv(join(dirname(__root__),'resources','csv','regionais.csv'))
termos = pd.read_csv(join(dirname(__root__),'resources','csv','termos.csv'))
se = pd.read_csv(join(dirname(__root__),'resources','csv','se.csv'), parse_dates=['inicio','fim'])

meses = ['jan','fev','mar','abril','mai','jun','jul','ago','set','out','nov','dez']
