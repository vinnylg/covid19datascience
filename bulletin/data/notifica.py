from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
from sys import exit
import pandas as pd

from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_hash

class Notifica:
    def __init__(self, pathfile=join('input','notifica.csv'), force=False, hard=False):
        self.pathfile = pathfile
        self.__source = None
        self.checksum_file = join(dirname(__root__),'resources','database','notifica_checksum')
        self.database = join(dirname(__root__),'resources','database','notifica','notifica.pkl')
        self.errorspath = join('output','errors')

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))

        if isfile(self.pathfile):
            saved_checksum = None

            if isfile(self.checksum_file):
                with open(self.checksum_file, "r") as checksum:
                    saved_checksum = checksum.read()

            with open(self.pathfile, "rb") as filein:
                bytes = filein.read()
                self.checksum = sha256(bytes).hexdigest()

            if saved_checksum != self.checksum:
                if force:
                    self.update()
            else:
                if hard:
                    self.update()

            if isfile(self.database):
                self.__source = pd.read_pickle(self.database)
            else:
                self.update()

        else:
            if not isdir(dirname(self.pathfile)):
                makedirs(dirname(self.pathfile))

            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def __len__(self):
        return len(self.__source)

    def shape(self):
        return (len(self.__source),len(self.__source.loc[self.__source['cod_evolucao'] == 1]),len(self.__source.loc[self.__source['cod_evolucao'] == 2]),len(self.__source.loc[self.__source['cod_evolucao'] == 3]),len(self.__source.loc[self.__source['cod_evolucao'] == 4]))

    def read_notifica(self,pathfile):
        return pd.read_csv(pathfile,
                           dtype = {
                               'id': 'int32',
                           },
                           converters = {
                               'paciente': normalize_text,
                               'nome_mae': normalize_text,
                               'cpf': lambda x: normalize_number(x,fill=0),
                               'cod_tipo_paciente': lambda x: normalize_number(x, fill=0),
                               'tipo_paciente': normalize_text,
                               'idade': lambda x: normalize_number(x, fill=0),
                               'sexo': normalize_text,
                               'cod_raca_cor': lambda x: normalize_number(x, fill=0),
                               'raca_cor': normalize_text,
                               'cod_etnia': lambda x: normalize_number(x, fill=0),
                               'etnia': normalize_text,
                               'uf_residencia': lambda x: normalize_number(x, fill=0),
                               'ibge_residencia': lambda x: normalize_number(x, fill=0),
                               'semana_epidemiologica': lambda x: normalize_number(x, fill=0),
                               'cod_classificacao_final': lambda x: normalize_number(x, fill=0),
                               'classificacao_final': normalize_text,
                               'cod_criterio_classificacao': lambda x: normalize_number(x, fill=0),
                               'criterio_classificacao': normalize_text,
                               'cod_evolucao': lambda x: normalize_number(x, fill=0),
                               'evolucao': normalize_text,
                               'numero_do': lambda x: normalize_number(x, fill=0),
                               'lab_executor': normalize_text,
                               'requisicao': lambda x: normalize_number(x, fill=0),
                               'co_seq_exame': lambda x: normalize_number(x, fill=0),
                               'cod_metodo': lambda x: normalize_number(x, fill=0),
                               'metodo': normalize_text,
                               'cod_exame': lambda x: normalize_number(x, fill=0),
                               'exame': normalize_text,
                               'cod_resultado': lambda x: normalize_number(x, fill=0),
                               'resultado': normalize_text,

                               'cod_origem': lambda x: normalize_number(x,fill=0),
                               'origem': normalize_text,
                               'uf_unidade_notifica': lambda x: normalize_number(x,fill=0),
                               'ibge_unidade_notifica': lambda x: normalize_number(x,fill=0),
                               'nome_unidade_notifica': normalize_text,
                               'nome_notificador': normalize_text,
                               'email_notificador': normalize_text,
                               'telefone_notificador': normalize_text
                           },
                           parse_dates = ['data_nascimento','data_1o_sintomas','data_cura_obito','data_coleta','data_recebimento','data_liberacao','data_notificacao','updated_at'],
                           date_parser = lambda x: pd.to_datetime(x, errors='coerce', format='%d/%m/%Y')
                        )

    def update(self):
        # notifica = self.read_notifica(self.pathfile)
        notifica = self.read_notifica(join('input','null.csv'))
        notifica = notifica.append(self.read_notifica(join('input','0.csv')))
        notifica = notifica.append(self.read_notifica(join('input','1.csv')))
        notifica = notifica.append(self.read_notifica(join('input','2.csv')))
        notifica = notifica.append(self.read_notifica(join('input','3.csv')))
        notifica = notifica.append(self.read_notifica(join('input','5.csv')))

        municipios = static.municipios[['ibge','municipio','uf']].copy()
        municipios['municipio'] = municipios['municipio'].apply(normalize_text)

        regionais = static.regionais[['ibge','nu_reg']].copy()
        regionais = regionais.rename(columns={'ibge':'ibge_residencia','nu_reg':'rs'})

        municipios = municipios.rename(columns={'ibge':'ibge_residencia','municipio':'mun_resid','uf':'uf_resid'})
        notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_residencia')
        notifica = pd.merge(left=notifica, right=regionais, how='left', on='ibge_residencia')

        municipios = municipios.rename(columns={'ibge_residencia':'ibge_unidade_notifica','mun_resid':'mun_atend','uf_resid':'uf_atend'})
        notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_unidade_notifica')

        notifica['rs'] = notifica['rs'].apply(lambda x: normalize_number(x,fill='99'))
        notifica['rs'] = notifica['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)

        nao = set(['NAO','CONSTA','INFO','INFORMADO','CONTEM'])
        notifica.loc[ [True if set(nome_mae.split(" ")).intersection(nao) else False for nome_mae in notifica['nome_mae'] ], 'nome_mae'] = None

        notifica.loc[notifica['mun_resid'].notnull(), 'hash_idade_resid'] = notifica.loc[notifica['mun_resid'].notnull()].apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)
        notifica.loc[notifica['mun_atend'].notnull(), 'hash_idade_atend'] = notifica.loc[notifica['mun_atend'].notnull()].apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)
        notifica['hash_idade'] = notifica.apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade']))).hexdigest(), axis=1)

        notifica.loc[ notifica['nome_mae'].notnull(), 'hash_mae'] = notifica.loc[ notifica['nome_mae'].notnull() ].apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+normalize_hash(row['nome_mae']))).hexdigest(), axis=1)
        # notifica['hash_nasc'] = notifica.apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+row['data_nascimento'].dt.strftime('%d%m%Y'))).hexdigest(), axis=1)

        notifica.to_pickle(self.database)

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        self.__source = notifica

    def filter_date(self,date):
        self.__source = self.__source.loc[((self.__source['updated_at'] >= date) | (self.__source['data_liberacao'] >= date) | (self.__source['data_notificacao'] >= date))]

    def get_casos(self):
        return self.__source.copy()

    def get_obitos(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 2].copy()

    def get_recuperados(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 1].copy()

    def get_casos_ativos(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 3].copy()

    def get_obitos_nao_covid(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 4].copy()