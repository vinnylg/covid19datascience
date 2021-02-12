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
class CasosConfirmados:

    #----------------------------------------------------------------------------------------------------------------------
    def __init__(self, pathfile=''):
        self.__source = None
        self.pathfile = pathfile
        self.database = join(dirname(__root__),'resources','database','casos_confirmados.pkl')
        self.errorspath = join('output','errors','casos_confirmados',datetime.today().strftime('%B_%Y'))

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))

    def __len__(self):
        return len(self.__source)

    #----------------------------------------------------------------------------------------------------------------------
    def shape(self):
        return (len(self.__source),len(self.__source.loc[self.__source['obito'] == 1]),len(self.__source.loc[self.__source['recuperado'] == 1]),len(self.__source.loc[self.__source['ativo'] == 1]))

    #----------------------------------------------------------------------------------------------------------------------
    def read(self,pathfile=join('input','casos_confirmados.xlsx'),append=False):
        self.pathfile = pathfile
        casos_confirmados = pd.read_excel(pathfile)

        casos_confirmados['hash_resid'] = casos_confirmados.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        casos_confirmados['hash_atend'] = casos_confirmados.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_atend']), axis=1)
        casos_confirmados['hash_diag'] = casos_confirmados.apply(lambda row: normalize_hash(row['paciente'])+row['data_diagnostico'].strftime('%d%m%Y'), axis=1)

        if isinstance(self.__source, pd.DataFrame) and append:
            self.__source = self.__source.append(casos_confirmados, ignore_index=True)
        else:
            self.__source = casos_confirmados

    #----------------------------------------------------------------------------------------------------------------------
    def load(self):
        try:
            self.__source = pd.read_pickle(self.database)
        except:
            raise Exception(f"Arquivo {self.database} não encontrado")

    #----------------------------------------------------------------------------------------------------------------------
    def save(self, df=None):
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            new_df = df
            self.__source = new_df
        elif isinstance(self.__source, pd.DataFrame) and len(self.__source) > 0:
            new_df = self.__source
        else:
            raise Exception('Não é possível salvar um DataFrame inexistente, realize a leitura antes ou passe como para esse método')

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
    def __get_uf(self,mun):
        if '/' in mun:
            _, uf = mun.split('/')
        else:
            uf = 'PR'
        return uf

    #----------------------------------------------------------------------------------------------------------------------
    def __get_mun(self, mun):
        if '/' in mun:
            mun, _ = mun.split('/')
        return mun

    #----------------------------------------------------------------------------------------------------------------------
    def get_daily_news(self, notifica):
        casos_confirmados = self.__source
        novos_casos = notifica.loc[ ~(notifica['id'].isin(casos_confirmados['id'])) ]

        notifica_obitos = notifica.loc[notifica['cod_evolucao']==2]
        obitos_confirmados = casos_confirmados.loc[casos_confirmados['obito']==1]
        novos_obitos = notifica_obitos.loc[ ~(notifica_obitos['id'].isin(obitos_confirmados['id'])) ]

        return novos_casos, novos_obitos

    #----------------------------------------------------------------------------------------------------------------------
    def get_novos_obitos(self, notifica):
        pass

    #----------------------------------------------------------------------------------------------------------------------
    def get_novos_recuperados(self, notifica):
        pass
