from os.path import dirname, join, isfile
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
import pandas as pd
from sys import exit

from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_labels, normalize_number, normalize_municipios, normalize_igbe

class CasosConfirmados:
    def __init__(self, pathfile:str=join(dirname(__root__),'tmp','Casos confirmados.xlsx')):
        self.pathfile = pathfile
        self.__source = None
        self.database = { 'casos': join(dirname(__root__),'tmp','casos.pkl'), 'obitos': join(dirname(__root__),'tmp','obitos.pkl')}
        self.checksum_file = join(dirname(__root__),'tmp','casos_confirmados_checksum')

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

            if isfile(self.database['casos']) and isfile(self.database['obitos']):
                casos = pd.read_pickle(self.database['casos'])
                obitos = pd.read_pickle(self.database['obitos'])
                self.__source = { 'casos': casos, 'obitos': obitos }
                print(f"{self.database} carregado")
            else:
                print(f"{self.database} não encontrado, utilizando o método update")
                self.update(self)

        else:
            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")


    def update(self):
        print(f"Atualizando o arquivo {self.database} com o {self.pathfile}...")

        casos = pd.read_excel(self.pathfile,'Casos confirmados',usecols='D,F,G')
        casos.columns = [ normalize_labels(x) for x in casos.columns ]

        casos['nome'] = casos['nome'].apply(normalize_text)
        casos['idade'] = casos['idade'].apply(normalize_number)
        casos['mun_resid'] = casos['mun_resid'].apply(normalize_text)

        casos['hash'] = casos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade'])+row['mun_resid'])).hexdigest(), axis=1)

        obitos = pd.read_excel(self.pathfile,'Obitos',usecols='D,F,G')
        obitos.columns = [ normalize_labels(x) for x in obitos.columns ]

        obitos['nome'] = obitos['nome'].apply(normalize_text)
        obitos['idade'] = obitos['idade'].apply(normalize_number)
        obitos['mun_resid'] = obitos['municipio'].apply(normalize_text)

        obitos['hash'] = obitos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade'])+row['municipio'])).hexdigest(), axis=1)

        casos.to_pickle(self.database['casos'])
        obitos.to_pickle(self.database['obitos'])

        with open(self.checksum_file, "w") as checksum:
            print('caralho entrei aqui')
            checksum.write(self.checksum)

        print(f"{self.database} salvo e {self.checksum_file} atualizado")

        self.__source = { 'casos': casos, 'obitos': obitos }

    def get_casos(self):
        try:
            return self.__source['casos'].copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_obitos(self):
        try:
            return self.__source['obitos'].copy()
        except e:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")
    #
    # def get_recuperados(self):
    #     try:
    #         return self.__source.loc[self.__source['evolucao'] == 1].copy()
    #     except e:
    #         exit("Fonte de dados não encontrada, primeiro utilize o método update")
    #
    # def get_casos_ativos(self):
    #     try:
    #         return self.__source.loc[self.__source['evolucao'] == 3].copy()
    #     except e:
    #         exit("Fonte de dados não encontrada, primeiro utilize o método update")
    #
    # def get_obitos_nao_covid(self):
    #     try:
    #         return self.__source.loc[self.__source['evolucao'] == 4].copy()
    #     except e:
    #         exit("Fonte de dados não encontrada, primeiro utilize o método update")
