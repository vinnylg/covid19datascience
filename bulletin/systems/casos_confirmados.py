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


    def __len__(self):
        return len(self.df)


    def __str__(self):
        return self.database


    @Timer('saving Casos Confirmados to pkl')
    def save(self,database=None,replace=False):
        database = database if isinstance(database,str) else self.databases[database] if isinstance(database,int) else self.databases[-1]
        
        pathfile = join(self.database_dir,f"{database}.pkl")
        
        if database in self.databases and not replace:
            raise Exception(f"{pathfile} already saved, set replace=True to replace")
        else:
            print(f"Load {pathfile}")
        
        self.df.to_pickle(pathfile)
            
            
    @Timer('loading Casos Confirmados from pkl')
    def load(self, database=None):
        
        database = database if isinstance(database,str) else self.databases[database] if isinstance(database,int) else self.databases[-1]
        
        pathfile = join(self.database_dir,f"{database}.pkl")
        
        if database not in self.databases:
            raise Exception(f"{pathfile} not found")
        else:
            print(f"Load {pathfile}")
        
        df = pd.read_pickle(pathfile)
        
        self.df = df
    

    def export(self, output_file):
        pass