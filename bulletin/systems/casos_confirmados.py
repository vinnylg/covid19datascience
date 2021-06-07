# from datetime import datetime, timedelta, date
# import gc
import logging
import pandas as pd
from os import makedirs
from pathlib import Path
from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta, date

from bulletin import root, default_input, default_output
from bulletin.systems.notifica import Notifica
from bulletin.utils.normalize import normalize_number, normalize_labels, normalize_hash, date_hash, normalize_ibge, normalize_text
from bulletin.utils.timer import Timer
from bulletin.utils.static import Municipios
import glob

ontem = date.today() - timedelta(days=1)

# ----------------------------------------------------------------------------------------------------------------------
class CasosConfirmados:

    def __init__(self):
        self.df = None
        self.database_dir = join(root, 'database', 'casos_confirmados')

        if not isdir(self.database_dir):
            makedirs(self.database_dir)
        else:
            self.databases = sorted([ Path(path).stem for path in glob.glob(join(self.database_dir,"cc_*.pkl"))])
            print("databases:",self.databases)
 
        self.default_columns = [ 'identificacao', 'id_notifica', 'uf_resid', 'ibge_resid', 'ibge_atend',
                                 'nome', 'sexo', 'idade', 'laboratorio', 'dt_diag', 'comunicacao', 'is',
                                 'evolucao', 'data_evolucao', 'data_com_evolucao']


    def __len__(self):
        return len(self.df)

    def __str__(self):
        return self.database
    
    @Timer('reading Casos Confirmados xlsx')
    def load_excel(self,filename=join(default_input,'casos_confirmados.xlsx')):
        ef = pd.ExcelFile(filename)
        casos_confirmados =  pd.concat([ pd.read_excel(ef,sheet,index_col=0) for sheet in ['Leste','Oeste','Noroeste','Norte','Fora']])
        casos_confirmados = casos_confirmados.sort_values(['comunicacao','nome','idade']).copy()
        
        municipios = Municipios()
        municipios['mun_resid'] = municipios['municipio'].apply(normalize_text)
        municipios.loc[municipios['uf']!='PR','mun_resid'] = municipios.loc[municipios['uf']!='PR','municipio'].apply(normalize_text) + '/' + municipios['uf']
        
        casos_confirmados = pd.merge(casos_confirmados.rename(columns={'ibge_resid':'ibge'}),municipios,how='left',on='ibge').rename(columns={'uf':'uf_resid', 'ibge':'ibge_resid'})
        
        casos_confirmados.loc[casos_confirmados['evolucao']=='OBITO', 'evolucao']=2
        casos_confirmados.loc[casos_confirmados['evolucao']=='CURA', 'evolucao']=1
        casos_confirmados.loc[casos_confirmados['evolucao']=='ATIVO', 'evolucao']=3
        
        if not 'identificacao' in casos_confirmados.columns:
            casos_confirmados['identificacao'] = -1
            
        casos_confirmados['hash'] = ( casos_confirmados['nome'].apply(normalize_hash) +
               casos_confirmados['idade'].astype(str) +
               casos_confirmados['ibge_resid'].astype(str) )
        

        casos_confirmados['hash_atend'] = (casos_confirmados['nome'].apply(normalize_hash) +
                          casos_confirmados['idade'].astype(str) +
                          casos_confirmados['ibge_atend'].apply(normalize_hash) )
        
        casos_confirmados['hash_less'] = ( casos_confirmados['nome'].apply(normalize_hash) +
                            casos_confirmados['idade'].apply(lambda x: str(x-1)) +
                            casos_confirmados['ibge_resid'].astype(str) )
        
        casos_confirmados['hash_more'] = ( casos_confirmados['nome'].apply(normalize_hash) +
                            casos_confirmados['idade'].apply(lambda x: str(x+1)) +
                            casos_confirmados['ibge_resid'].astype(str) )
        
        casos_confirmados['hash_diag'] = ( casos_confirmados['nome'].apply(normalize_hash) +
                            casos_confirmados['dt_diag'].apply(date_hash) )
        
        self.df = casos_confirmados[self.default_columns + ['hash','hash_atend','hash_less','hash_more','hash_diag']].copy()        

        return casos_confirmados[self.default_columns + ['hash','hash_atend','hash_less','hash_more','hash_diag']]      
        
    @Timer('saving Casos Confirmados to pkl')
    def save(self,database=None,replace=False):
        database = database if isinstance(database,str) else self.databases[database] if isinstance(database,int) else self.databases[-1]
        
        pathfile = join(self.database_dir,f"{database}.pkl")
        
        if database in self.databases and not replace:
            raise Exception(f"{pathfile} already saved, set replace=True to replace")
        else:
            print(f"Load {pathfile}")
        
        self.df[self.default_columns].to_pickle(pathfile)
            
            
    @Timer('loading Casos Confirmados from pkl')
    def load(self, database=None):
        
        database = database if isinstance(database,str) else self.databases[database] if isinstance(database,int) else self.databases[-1]
        
        pathfile = join(self.database_dir,f"{database}.pkl")
        
        if database not in self.databases:
            raise Exception(f"{pathfile} not found")
        else:
            print(f"Load {pathfile}")
        
        df = pd.read_pickle(pathfile)[self.default_columns]
        
        df['hash'] = ( df['nome'].apply(normalize_hash) +
                       df['idade'].astype(str) +
                       df['ibge_resid'].apply(str) )
        
        df['hash_atend'] = (df['nome'].apply(normalize_hash) +
                          df['idade'].astype(str) +
                          df['ibge_atend'].apply(str) )
                
        df['hash_less'] = ( df['nome'].apply(normalize_hash) +
                            df['idade'].apply(lambda x: str(x-1)) +
                            df['ibge_resid'].apply(str) )
        
        df['hash_more'] = ( df['nome'].apply(normalize_hash) +
                            df['idade'].apply(lambda x: str(x+1)) +
                            df['ibge_resid'].apply(str) )
        
        df['hash_diag'] = ( df['nome'].apply(normalize_hash) +
                            df['dt_diag'].apply(date_hash) )
        
        self.df = df
    

    def export(self, output_file):
        pass

    def get_obitos(self):
        return self.df.loc[self.df['evolucao'] == 2].copy()

    def get_casos(self):
        return self.df.copy()

    def novos_casos(self, casos_notifica):
        casos = self.df
        return casos_notifica.loc[~(
                (casos_notifica['hash_resid'].isin(casos['hash'])) |
                (casos_notifica['hash_resid'].isin(casos['hash_less'])) |
                (casos_notifica['hash_resid'].isin(casos['hash_more'])) |
                (casos_notifica['hash_diag'].isin(casos['hash_diag']))
        )]

    def novos_obitos(self, obitos_notifica):
        obitos = self.get_obitos()
        return obitos_notifica.loc[~(
                (obitos_notifica['hash_resid'].isin(obitos['hash'])) |
                (obitos_notifica['hash_resid'].isin(obitos['hash_less'])) |
                (obitos_notifica['hash_resid'].isin(obitos['hash_more'])) |
                (obitos_notifica['hash_diag'].isin(obitos['hash_diag'])) |
                (obitos_notifica['hash_obito'].isin(obitos['hash_obito']))
        )]
    
    
