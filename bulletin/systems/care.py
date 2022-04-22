import pandas as pd
from os.path import join

import logging
logger = logging.getLogger(__name__)

from bulletin.systems import System

class Care(System):
    def __init__(self,database:str='internados'):
        super().__init__('care',database)

    def read(self, pathfile, append=False):
        if isinstance(pathfile,list):
            for path in pathfile:
                self.read(path,True)
        else:
            if '.xls' in pathfile:
                df = pd.read_excel(
                        pathfile,
                        dtype=str
                )
            elif '.csv' in pathfile:
                df = pd.read_csv(
                    pathfile,
                    dtype=str
                )

            if isinstance(self.df, pd.DataFrame) and append:
                self.df = self.df.append(df, ignore_index=True)
            else:
                self.df = df
    
    def load(self, database:str=None, compress:bool=False):
        super().load(database=database,compress=compress)

    def save(self,database:str=None,replace:bool=False, compress:bool=False):
        super().save(database=database,replace=replace,compress=compress)

    def calculate_idade(self):
        pass

    def normalize(self):
        self.df.loc[self.df['Leito Tipo'].astype(str).str.upper().str.contains('UTI'),'Leito Tipo'] = 2
        self.df.loc[self.df['Leito Tipo']!=2,'Leito Tipo'] = 1
        super().normalize()

    def download_all(self):
        raise NotImplemented()

    def download_update():
        raise NotImplemented()
    
    def update(self, parts):
        news = super().process_update(parts,Care)
        super().update(news)
    