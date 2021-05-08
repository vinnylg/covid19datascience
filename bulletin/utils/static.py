import pandas as pd
from os.path import dirname, join
from bulletin import __file__ as __root__

Municipios = lambda: pd.read_csv(join(dirname(__root__),'resources','csv','populacao.csv')).copy()
Regionais = lambda: pd.read_csv(join(dirname(__root__),'resources','csv','regionais.csv')).copy()

meses = ['jan','fev','mar','abril','mai','jun','jul','ago','set','out','nov','dez']
