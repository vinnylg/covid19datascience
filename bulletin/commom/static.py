import pandas as pd
from os.path import dirname, join
from bulletin import __file__ as __root__

municipios = pd.read_csv(join(dirname(__root__),'resources','csv','municipios.csv'))
paises = pd.read_csv(join(dirname(__root__),'resources','csv','paises.csv'))
regionais = pd.read_csv(join(dirname(__root__),'resources','csv','regionais.csv'))
termos = pd.read_csv(join(dirname(__root__),'resources','csv','termos.csv'))
se = pd.read_csv(join(dirname(__root__),'resources','csv','se.csv'), parse_dates=['inicio','fim'])

meses = ['jan/2021','fev','mar','abril','mai','jun','jul','ago','set','out','nov','dez']
