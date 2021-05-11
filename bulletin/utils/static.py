import pandas as pd
from os.path import dirname, join
from bulletin import __file__ as __root__

Municipios = lambda x='csv': pd.read_csv(join(dirname(__root__),'resources','csv','municipios.csv'))
aaa = lambda x='csv': pd.read_csv(join(dirname(__root__),'resources','csv','municipios2.csv'))
                         
meses = ['jan','fev','mar','abril','mai','jun','jul','ago','set','out','nov','dez']
