#-----------------------------------------------------------------------------------------------------------------------------#
# Esse arquivo faz parte de um pacote de scripts criados para realizar o tratamento de dados do Notifica Covid-19 Paraná.
# Todos os direitos reservados ao autor
#-----------------------------------------------------------------------------------------------------------------------------#

from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
import pandas as pd
import codecs
from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_labels, normalize_number, normalize_municipios, normalize_igbe, normalize_hash, date_hash

#----------------------------------------------------------------------------------------------------------------------
class CasosComunicados:

    #----------------------------------------------------------------------------------------------------------------------
    def __init__(self, pathfile=''):
        self.__source = None
        self.pathfile = pathfile
        self.database = join(dirname(__root__),'resources','database','casos_comunicados.pkl')
        self.errorspath = join('output','errors','casos_comunicados',datetime.today().strftime('%B_%Y'))

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))

    #----------------------------------------------------------------------------------------------------------------------
    def __len__(self):
        return len(self.__source)

    #----------------------------------------------------------------------------------------------------------------------
    def shape(self):
        return (len(self.__source),len(self.__source.loc[self.__source['evolucao'] == 1]),len(self.__source.loc[self.__source['evolucao'] == 2]),len(self.__source.loc[self.__source['evolucao'] == 3]))

    #----------------------------------------------------------------------------------------------------------------------
    def read(self,pathfile=join('input','casos_comunicados.csv'),append=False):
        self.pathfile = pathfile
        casos_comunicados = pd.csv(pathfile)

        casos_comunicados['hash_resid'] = casos_comunicados.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        casos_comunicados['hash_atend'] = casos_comunicados.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_atend']), axis=1)
        casos_comunicados['hash_diag'] = casos_comunicados.apply(lambda row: normalize_hash(row['paciente'])+row['data_diagnostico'].strftime('%d%m%Y'), axis=1)

        casos_comunicados['checksum'] = casos_comunicados.apply(
            lambda row:
                sha256(
                    str.encode(
                        normalize_hash(row['paciente']) + normalize_hash(row['idade']) + normalize_hash(row['mun_resid']) + 
                        normalize_hash(row['evolucao'])
                    )
                ).hexdigest()
            ,axis = 1
        )

        if isinstance(self.__source, pd.DataFrame) and append:
            self.__source = self.__source.append(casos_comunicados, ignore_index=True)
        else:
            self.__source = casos_comunicados

    #----------------------------------------------------------------------------------------------------------------------
    def load(self):
        self.__source = pd.read_pickle(self.database)

    #----------------------------------------------------------------------------------------------------------------------
    def save(self, df):
        new_df = df
        self.__source = new_df
        new_df.to_pickle(self.database)

    #----------------------------------------------------------------------------------------------------------------------
    def get_casos(self):
        return self.__source.copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_obitos(self):
        return self.__source.loc[self.__source['evolucao']==2].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_recuperados(self):
        return self.__source.loc[self.__source['evolucao']==1].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_ativos(self):
        return self.__source.loc[self.__source['evolucao']==3].copy()