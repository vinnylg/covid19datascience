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

from datetime import datetime
import pyminizip as pz

# ----------------------------------------------------------------------------------------------------------------------
class CasosConfirmados:
    municipios = Municipios()
    municipios['mun_resid'] = municipios['municipio'].apply(normalize_text)
    municipios.loc[municipios['uf']!='PR','mun_resid'] = municipios.loc[municipios['uf']!='PR','municipio'].apply(normalize_text) + '/' + municipios['uf']

    today = datetime.today()
    ontem = today - timedelta(1)
    anteontem = ontem - timedelta(1)

    def __init__(self, database=f"cc_{ontem.strftime('%d_%m_%Y')}"):
        self.df = None
        self.database_dir = join(root, 'database', 'casos_confirmados')
        self.database = database

        if not isdir(self.database_dir):
            makedirs(self.database_dir)

        self.databases = lambda: sorted([ Path(path).stem for path in glob.glob(join(self.database_dir,"*.pkl"))])
        print(self.databases())


    def __len__(self):
        return len(self.df)

    def __str__(self):
        return self.database

    def rename_cols(self):
        try:
            self.df.columns = ['identificacao','id_notifica','uf_residencia','ibge_residencia','ibge_unidade_notifica','paciente','sexo','idade','exame','data_diagnostico','data_comunicacao','data_1o_sintomas','evolucao','data_evolucao','data_com_evolucao','hash_resid','hash_resid_less','hash_resid_more','hash_atend','hash_atend_less','hash_atend_more','hash_diag']
        except:
            print('error rename_cols')

    def hashes(self):
        assert not self.df is None

        for col in [ col for col in self.df.columns if 'hash' in col ]:
            del self.df[col]
        
        less = lambda x: str(x-1)
        more = lambda x: str(x+1)

        self.df['hash_resid'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].astype(str) + self.df['ibge_residencia'].astype(str)
        self.df['hash_resid_less'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].apply(less) + self.df['ibge_residencia'].astype(str)
        self.df['hash_resid_more'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].apply(more) + self.df['ibge_residencia'].astype(str)
        self.df['hash_atend'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].astype(str) + self.df['ibge_unidade_notifica'].astype(str)
        self.df['hash_atend_less'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].apply(less) + self.df['ibge_unidade_notifica'].astype(str)
        self.df['hash_atend_more'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].apply(more) + self.df['ibge_unidade_notifica'].astype(str)
        self.df['hash_diag'] = self.df['paciente'].apply(normalize_hash) + self.df['data_diagnostico'].apply(date_hash)

    def fix_dtypes(self):
        assert not self.df is None
        
        cols = pd.DataFrame(zip(self.df.columns,self.df.dtypes),columns=['col','dtype']).set_index('col')
        floats = cols.loc[cols['dtype']=='float64']
        if len(floats):
            for col in floats.index:
                self.df[col] = self.df[col].fillna(-1).apply(int)

    @Timer('saving Casos Confirmados to pkl')
    def save(self,database=None,replace=False):
        assert not self.df is None
   
        if not database is None:
            self.database = database


        if self.database in self.databases() and not replace:
            raise Exception(f"{self.database} already saved, set replace=True to replace")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")

        self.df.to_pickle(pathfile)
            
            
    @Timer('loading Casos Confirmados from pkl')
    def load(self, database=None):
        if not database is None:
            self.database = database

        if not self.database in self.databases():
            raise Exception(f"{self.database} not found")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")
        self.df = pd.read_pickle(pathfile)    
        self.fix_dtypes()

    def export(self, output_file):
        pass