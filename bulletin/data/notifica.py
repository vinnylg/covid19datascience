from os.path import dirname, join, isfile
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
import pandas as pd
from sys import exit

from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_municipios, normalize_igbe

class Notifica:
    def __init__(self, pathfile:str=join(dirname(__root__),'tmp','notifica.csv')):
        self.pathfile = pathfile
        self.__source = None
        self.checksum_file = join(dirname(__root__),'tmp','notifica_checksum')
        self.database = join(dirname(__root__),'tmp','notifica.pkl')

        if isfile(self.pathfile):
            saved_checksum = None

            if isfile(self.checksum_file):
                with open(self.checksum_file, "r") as checksum:
                    saved_checksum = checksum.read()
                    print(f"saved checksum: {saved_checksum}")


            with open(self.pathfile, "rb") as filein:
                bytes = filein.read()
                self.checksum = sha256(bytes).hexdigest()
                print(f"current checksum: {self.checksum}")

            if saved_checksum != self.checksum:
                print(f"Parece que o arquivo {self.pathfile} sofreu alterações, considere usar o método update")
            else:
                print(f"Tudo certo, nenhuma alteração detectada")

            if isfile(self.database):
                self.__source = pd.read_pickle(self.database)
                print(f"{self.database} carregado")
            else:
                print(f"{self.database} não encontrado, utilizando o método update")
                self.update(self)

        else:
            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def update(self):
        print(f"Atualizando o arquivo {self.database} com o {self.pathfile}...")
        notifica = pd.read_csv(self.pathfile,
                           converters = {
                               'paciente': normalize_text,
                               'idade': lambda x: normalize_number(x,fill=-1),
                               'ibge_residencia': lambda x: normalize_number(x,fill=-1),
                               'ibge_unidade_notifica': lambda x: normalize_number(x,fill=-1),
                               'exame': lambda x: normalize_number(x,fill=0),
                               'evolucao': lambda x: normalize_number(x,fill=3)
                           },
                           parse_dates = ['data_notificacao','updated_at','data_liberacao','data_1o_sintomas','data_cura_obito'],
                           date_parser = lambda x: pd.to_datetime(x, errors='coerce', format='%d/%m/%Y')
                        )

        notifica = notifica.loc[ notifica['ibge_residencia'] > 0 ]
        notifica = notifica.loc[ notifica['idade'] != -1 ]

        municipios = static.municipios[['ibge','municipio']].copy()
        municipios['municipio'] = municipios['municipio'].apply(normalize_text)

        regionais = static.regionais[['ibge','nu_reg']].copy()
        regionais = regionais.rename(columns={'ibge':'ibge_residencia','nu_reg':'rs'})

        exames = static.termos.loc[static.termos['tipo']=='exame',['codigo','valor']].copy()
        exames = exames.rename(columns={'codigo':'exame','valor':'nome_exame'})

        municipios = municipios.rename(columns={'ibge':'ibge_residencia','municipio':'mun_resid'})
        notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_residencia')
        notifica = pd.merge(left=notifica, right=regionais, how='left', on='ibge_residencia')

        municipios = municipios.rename(columns={'ibge_residencia':'ibge_unidade_notifica','mun_resid':'mun_atend'})
        notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_unidade_notifica')
        notifica = pd.merge(left=notifica, right=exames, how='left', on='exame')

        notifica['rs'] = notifica['rs'].apply(lambda x: normalize_number(x,fill='99'))
        notifica['rs'] = notifica['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)

        notifica = notifica.loc[notifica['mun_resid'].notnull()]

        notifica['hash'] = notifica.apply(lambda row: sha256(str.encode(row['paciente']+str(row['idade'])+row['mun_resid'])).hexdigest(), axis=1)
        notifica['hash_less'] = notifica.apply(lambda row: sha256(str.encode(row['paciente']+str(row['idade']-1)+row['mun_resid'])).hexdigest(), axis=1)
        notifica['hash_more'] = notifica.apply(lambda row: sha256(str.encode(row['paciente']+str(row['idade']+1)+row['mun_resid'])).hexdigest(), axis=1)

        notifica.to_pickle(self.database)

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        print(f"{self.database} salvo e {self.checksum_file} atualizado")

        self.__source = notifica

    def get_casos(self):
        try:
            return self.__source.copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_obitos(self):
        try:
            return self.__source.loc[self.__source['evolucao'] == 2].copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_recuperados(self):
        try:
            return self.__source.loc[self.__source['evolucao'] == 1].copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_casos_ativos(self):
        try:
            return self.__source.loc[self.__source['evolucao'] == 3].copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_obitos_nao_covid(self):
        try:
            return self.__source.loc[self.__source['evolucao'] == 4].copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")
