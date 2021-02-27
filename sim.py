#-----------------------------------------------------------------------------------------------------------------------------#
# Classe de tratamento de dados do SIM Paraná
# Todos os direitos reservados ao autor
#-----------------------------------------------------------------------------------------------------------------------------#

from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from os import makedirs
import pandas as pd
from simpledbf import Dbf5
from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_labels, normalize_number, normalize_municipios, normalize_igbe, normalize_hash, date_hash

class sim:
    
    def __init__(self, pathfile=''):
        self.__source = None
        self.pathfile = pathfile
        self.database = join(dirname(__root__),'resources','database','sim.pkl')
        self.errorspath = join('output','errors','sim',datetime.today().strftime('%B_%Y'))

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))

    def __len__(self):
        return len(self.__source)

    def read(self,pathfile, append=False):
        obitos = Dbf5(pathfile, codec = 'cp860').to_dataframe()

        if isinstance(self.__source, pd.DataFrame) and append:
            self.__source = self.__source.append(obitos, ignore_index=True)
        else:
            self.__source = obitos
    
    def read_all_database_files(self):
        last_digit = ['0', '1']

        for x in last_digit:
            self.read(join('input','dbf',f"DOPR202{x}.dbf"), append=True)

    def load(self):
        try:
            self.__source = pd.read_pickle(self.database)
        except:
            raise Exception(f"Arquivo {self.database} não encontrado")

    def save(self, df=None):
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            new_df = df
            self.__source = new_df
        elif isinstance(self.__source, pd.DataFrame) and len(self.__source) > 0:
            new_df = self.__source
        else:
            raise Exception('Não é possível salvar um DataFrame inexistente, realize a leitura antes ou passe como para esse método')

        new_df.to_pickle(self.database)

    def to_notifica(self):
        self.__source = self.__source[[]]