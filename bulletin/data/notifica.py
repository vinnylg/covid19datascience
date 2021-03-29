from os.path import dirname, join, isfile
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
import pandas as pd
from sys import exit

from bulletin import __file__ as __root__
from bulletin.utils import static
from bulletin.utils.normalize import normalize_text, normalize_number, normalize_municipios, normalize_igbe, normalize_hash

class Notifica:
    def __init__(self, pathfile:str=join(dirname(__root__),'tmp','notifica.csv'), force=False, hard=False):
        self.pathfile = pathfile
        self.__source = None
        self.checksum_file = join(dirname(__root__),'tmp','notifica_checksum')
        self.database = join(dirname(__root__),'tmp','notifica.pkl')
        self.errorspath = join('output','errors')

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
                print(f"Parece que o arquivo {self.pathfile} sofreu alterações, considere usar o método update ou o passe force=True")
                if force:
                    print(f"Utilizando o método update")
                    self.update()
            else:
                print(f"Tudo certo, nenhuma alteração detectada")
                if hard:
                    print(f"Utilizando forcadamente com método update")
                    self.update()

            if isfile(self.database):
                self.__source = pd.read_pickle(self.database)
                print(f"{self.database} carregado")
            else:
                print(f"{self.database} não encontrado, utilizando o método update")
                self.update()

        else:
            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def __len__(self):
        return len(self.__source)

    def shape(self):
        return (len(self.__source),len(self.__source.loc[self.__source['evolucao'] == 1]),len(self.__source.loc[self.__source['evolucao'] == 2]),len(self.__source.loc[self.__source['evolucao'] == 3]),len(self.__source.loc[self.__source['evolucao'] == 4]))

    def update(self):
        print(f"Atualizando o arquivo {self.database} com o {self.pathfile}...")
        notifica = pd.read_csv(self.pathfile,
                           converters = {
                               'paciente': normalize_text,
                               'origem': lambda x: normalize_number(x,fill=0),
                               'nome_mae': normalize_text,
                               'idade': lambda x: normalize_number(x,fill=0),
                               'pais_residencia': lambda x: normalize_number(x,fill=0),
                               'uf_residencia': lambda x: normalize_number(x,fill=0),
                               'ibge_residencia': lambda x: normalize_number(x,fill=0),
                               'uf_unidade_notifica': lambda x: normalize_number(x,fill=0),
                               'ibge_unidade_notifica': lambda x: normalize_number(x,fill=0),
                               'criterio_classificacao': lambda x: normalize_number(x,fill=0),
                               'exame': lambda x: normalize_number(x,fill=0),
                               'evolucao': lambda x: normalize_number(x,fill=3)
                           },
                           parse_dates = ['data_notificacao','updated_at','data_nascimento','data_liberacao','data_1o_sintomas','data_cura_obito'],
                           date_parser = lambda x: pd.to_datetime(x, errors='coerce', format='%d/%m/%Y')
                        )

        municipios = static.municipios[['ibge','municipio','uf']].copy()
        municipios['municipio'] = municipios['municipio'].apply(normalize_text)

        regionais = static.regionais[['ibge','nu_reg']].copy()
        regionais = regionais.rename(columns={'ibge':'ibge_residencia','nu_reg':'rs'})

        exames = static.termos.loc[static.termos['tipo']=='exame',['codigo','valor']].copy()
        origens = static.termos.loc[static.termos['tipo']=='origem',['codigo','valor']].copy()
        criterios = static.termos.loc[static.termos['tipo']=='criterio_classificacao',['codigo','valor']].copy()

        exames = exames.rename(columns={'codigo':'exame','valor':'nome_exame'})
        origens = origens.rename(columns={'codigo':'origem','valor':'nome_origem'})
        criterios = criterios.rename(columns={'codigo':'criterio_classificacao','valor':'nome_criterio_classificacao'})

        notifica = pd.merge(left=notifica, right=exames, how='left', on='exame')
        notifica = pd.merge(left=notifica, right=origens, how='left', on='origem')
        notifica = pd.merge(left=notifica, right=criterios, how='left', on='criterio_classificacao')

        notifica['nome_exame'] = notifica['nome_exame'].fillna('Não informado')

        municipios = municipios.rename(columns={'ibge':'ibge_residencia','municipio':'mun_resid','uf':'uf_resid'})
        notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_residencia')
        notifica = pd.merge(left=notifica, right=regionais, how='left', on='ibge_residencia')

        municipios = municipios.rename(columns={'ibge_residencia':'ibge_unidade_notifica','mun_resid':'mun_atend','uf_resid':'uf_atend'})
        notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_unidade_notifica')

        notifica['rs'] = notifica['rs'].apply(lambda x: normalize_number(x,fill='99'))
        notifica['rs'] = notifica['rs'].apply(lambda x: str(x).zfill(2))

        # notifica.loc[notifica['mun_resid'].isnull()].to_excel('sem_municipio_residencia.xlsx', index=None)
        # notifica.loc[ notifica['data_liberacao'].isnull() ].to_excel('sem_data_liberacao.xlsx', index=None)
        # notifica.loc[ notifica['ibge_residencia'] <= 0  ].to_excel('sem_ibge_residencia.xlsx', index=None)
        # notifica.loc[ notifica['sexo'].isnull()  ].to_excel('sem_sexo.xlsx', index=None)
        # notifica.loc[ notifica['idade'] == -1 ].to_excel('sem_idade.xlsx', index=None)

        # notifica = notifica.loc[ notifica['data_liberacao'].notnull() ]
        # notifica = notifica.loc[ notifica['ibge_residencia'] > 0 ]
        # notifica = notifica.loc[ notifica['sexo'].notnull() ]
        # notifica = notifica.loc[ notifica['idade'] != -1 ]
        notifica = notifica.loc[notifica['mun_resid'].notnull()]

        # notifica.loc[notifica['data_liberacao'].isnull(), 'data_liberacao'] = notifica.apply(lambda row: row['data_notificacao'] if row['criterio_classificacao'] not in [1,4] else pd.NaT, axis=1)
        notifica.loc[notifica['data_liberacao'].isnull(), 'data_liberacao'] = notifica.apply(lambda row: row['data_notificacao'], axis=1)

#         notifica.loc[(notifica['rs'].isnull()) & (notifica['mun_resid'].notnull()), 'mun_resid'] = notifica.loc[(notifica['rs'].isnull()) & (notifica['mun_resid'].notnull()), 'mun_resid'] + '/' + notifica.loc[(notifica['rs'].isnull()) & (notifica['mun_resid'].notnull()), 'uf_resid']

        notifica['hash'] = notifica.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        notifica['hash_less'] = notifica.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
        notifica['hash_more'] = notifica.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

        notifica.to_pickle(self.database)

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        print(f"{self.database} salvo e {self.checksum_file} atualizado")

        self.__source = notifica

    def filter_date(self,date):
        self.__source = self.__source.loc[((self.__source['updated_at'] >= date) | (self.__source['data_liberacao'] >= date) | (self.__source['data_notificacao'] >= date))]

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
