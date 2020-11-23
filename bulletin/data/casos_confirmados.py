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
                self.update()

        else:
            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def shape(self):
        return (len(self.__source['casos']),len(self.__source['obitos']))

    def update(self):
        print(f"Atualizando o arquivo {self.database} com o {self.pathfile}...")

        casos = pd.read_excel(self.pathfile,'Casos confirmados',usecols='B,D,F,G')
        casos = pd.read_excel(self.pathfile,
                            'Casos confirmados',
                            usecols='B,D,F,G',
                            converters = {
                               'Nome': normalize_text,
                               'Idade': normalize_number,
                               'IBGE_RES_PR': normalize_igbe,
                               'Mun Resid': normalize_municipios
                            })

        casos.columns = [ normalize_labels(x) for x in casos.columns ]

        casos = casos.loc[casos['mun_resid'] != 'EXCLUIR']

        municipios = static.municipios.copy()[['ibge','uf','municipio']]
        municipios['municipio'] = municipios['municipio'].apply(normalize_text)

        casosPR = casos.loc[casos['ibge_res_pr'] != -1].copy()
        municipiosPR = municipios.loc[municipios['uf']=='PR']
        casosPR = pd.merge(left=casosPR, right=municipiosPR, how='left', left_on='ibge_res_pr', right_on='ibge')


        casosFora = casos.loc[casos['ibge_res_pr'] == -1].copy()
        municipiosFora = municipios.loc[municipios['uf']!='PR']
        casosFora = pd.merge(left=casosFora, right=municipiosFora, how='left', left_on='mun_resid', right_on='municipio')

        casos = casosPR.append(casosFora, ignore_index=True).sort_values(by='nome')
        casos = casos.drop(columns=(['ibge_res_pr']))

        casos['hash'] = casos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade'])+row['mun_resid'])).hexdigest(), axis=1)
        casos['hash_less'] = casos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade']-1)+row['mun_resid'])).hexdigest(), axis=1)
        casos['hash_more'] = casos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade']+1)+row['mun_resid'])).hexdigest(), axis=1)

        obitos = pd.read_excel(self.pathfile,
                            'Obitos',
                            usecols='B,D,F,G,I',
                            converters = {
                               'Nome': normalize_text,
                               'Idade': normalize_number,
                               'IBGE_RES_PR': normalize_igbe,
                               'Município': normalize_municipios
                            })

        obitos.columns = [ normalize_labels(x) for x in obitos.columns ]

        obitos = obitos.loc[obitos['municipio'] != 'EXCLUIR']

        obitos['hash'] = obitos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade'])+row['municipio'])).hexdigest(), axis=1)
        obitos['hash_less'] = obitos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade']-1)+row['municipio'])).hexdigest(), axis=1)
        obitos['hash_more'] = obitos.apply(lambda row: sha256(str.encode(row['nome']+str(row['idade']+1)+row['municipio'])).hexdigest(), axis=1)


        index_idade_less = obitos.loc[obitos['hash_less'].isin(casos['hash'])].index
        obitos.loc[index_idade_less,'idade'] -= 1
        obitos.loc[index_idade_less,'hash'] = obitos.loc[index_idade_less].apply(lambda row: sha256(str.encode(row['nome']+str(row['idade'])+row['municipio'])).hexdigest(), axis=1)

        index_idade_more = obitos.loc[obitos['hash_more'].isin(casos['hash'])].index
        obitos.loc[index_idade_more,'idade'] += 1
        obitos.loc[index_idade_more,'hash'] = obitos.loc[index_idade_more].apply(lambda row: sha256(str.encode(row['nome']+str(row['idade'])+row['municipio'])).hexdigest(), axis=1)

        obitos1 = obitos[['hash','data_do_obito']]
        casos = pd.merge(left=casos, right=obitos1, how='left', on='hash')

        casos.to_pickle(self.database['casos'])
        obitos.to_pickle(self.database['obitos'])

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        print(f"{self.database} salvo e {self.checksum_file} atualizado")

        self.__source = { 'casos': casos, 'obitos': obitos }

    def get_casos(self):
        try:
            return self.__source['casos'].copy()
        except:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_obitos(self):
        try:
            return self.__source['obitos'].copy()
        except:
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
