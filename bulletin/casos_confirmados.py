# from datetime import datetime, timedelta, date
# import gc
import logging
import pandas as pd
from os import makedirs
from pathlib import Path
from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta, date

from bulletin import root, default_input, default_output
from bulletin.notifica import Notifica
from bulletin.utils.normalize import normalize_number, normalize_labels, normalize_hash, date_hash, normalize_ibge, normalize_text
from bulletin.utils.timer import Timer
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
            self.databases = [ Path(path).stem for path in glob.glob(join(self.database_dir,"cc_*.pkl"))]
            print("databases:",self.databases)
            
        self.default_columns = [ 'identificacao', 'id_notifica', 'uf_resid', 'ibge_resid', 'ibge_atend',
                                 'nome', 'sexo', 'idade', 'laboratorio', 'dt_diag', 'comunicacao', 'is',
                                 'evolucao', 'data_evolucao', 'data_com_evolucao']

        
    def __len__(self):
        return len(self.df)

    def __str__(self):
        return self.database
            
    @Timer('saving Casos Confirmados to pkl')
    def save(self,database,replace=False):
        pathfile = join(self.database_dir,f"{database}.pkl")
        
        if database in self.databases and not replace:
            raise Exception(f"{pathfile} already saved, set replace=True to replace")
        
        self.df[self.default_columns].to_pickle(pathfile)
            
            
    @Timer('loading Casos Confirmados from pkl')
    def load(self, database=f"cc_{ontem.strftime('%d_%m_%Y')}"):
        pathfile = join(self.database_dir,f"{database}.pkl")
        
        if database not in self.databases:
            raise Exception(f"{pathfile} not found")
        
        df = pd.read_pickle(pathfile)[self.default_columns]
        
        df['hash'] = ( df['nome'].apply(normalize_hash) +
                       df['idade'].astype(str) +
                       df['ibge_resid'].astype(str) )
        
        df['hash_less'] = ( df['nome'].apply(normalize_hash) +
                            df['idade'].apply(lambda x: str(x-1)) +
                            df['ibge_resid'].astype(str) )
        
        df['hash_more'] = ( df['nome'].apply(normalize_hash) +
                            df['idade'].apply(lambda x: str(x+1)) +
                            df['ibge_resid'].astype(str) )
        
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
    
    
