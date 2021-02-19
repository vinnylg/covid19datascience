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
        return self.__source.loc[self.__source['obito']].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_recuperados(self):
        return self.__source.loc[self.__source['recuperado']].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_ativos(self):
        return self.__source.loc[self.__source['ativo']].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_daily_news(self, notifica):
        casos_comunicados = self.__source
        novos_casos = notifica.loc[ ~(notifica['id'].isin(casos_comunicados['id'])) ]

        return novos_casos

    #----------------------------------------------------------------------------------------------------------------------
    def get_novos_obitos(self, notifica):
        pass

    #----------------------------------------------------------------------------------------------------------------------
    def get_novos_recuperados(self, notifica):
        pass
