"""@package Notifica
Este pacote contém a classe Notifica, com métodos referentes à leitura, normalização, e obtenção dos dados provenientes do banco de dados Notifica Covid-19 Paraná
"""

#Importações de pacotes de terceiros
from os.path import dirname, join, isfile, isdir
from datetime import datetime, date, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
from sys import exit
import pandas as pd

#Importação de pacotes gerados
from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_hash, normalize_cpf
from bulletin.commom.utils import isvaliddate

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

class Notifica:
    """! Classe do Notifica
        Quando instanciada pode ser usada para ler novos dados, salvar, carregar.
        Os dados serão normalizados após a leitura, sendo salvos como deve
        Além disso, possibilida a extração dos casos, obitos, recuperados e ativos
    """

    def __len__(self):
        ###! Retorna o tamanho do DataFrame carregado.
        try:
            return len(self.__source)
        except:
            raise Exception('Não é possivel calcular o tamanho de algo inexistente')

    def __init__(self, pathfile=''):
        """! Construdor da classe Notifica.
            Inicializa os atributos, cria os diretórios auxiliares, verifica mudanças no arquivo.
            @param pathfile Caminho relativo ao arquivo csv que contém os dados baixados do Metabase
        """

        ## Referencia ao DataFrame lido do arquivo csv. Atribudo privado, não pode ser lido nem alterado fora da classe
        self.__source = None
        ## Realiza a leitura do arquivo csv e o calculo do hash, usado para verifiar mudanças no arquivo
        self.checksum = None
        ## Recebe caminho do arquivo csv passado como parametro
        self.pathfile = pathfile
        ## Arquivo onde é salvo o hash referente ao arquivo csv
        self.checksum_file = join(dirname(__root__),'resources','database','notifica_checksum')
        ## Arquivo onde é salvo o DataFrame normalizado para uma leitura mais rapida
        self.database = join(dirname(__root__),'resources','database','notifica.pkl')
        ## Diretório aonde é salvo eventuais erros de dado
        self.errorspath = join('output','errors','notifica',datetime.today().strftime('%d_%m_%Y'))

        if not isdir(self.errorspath):
            makedirs(self.errorspath) #Cria o diretório para os erros, separando os por data

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database)) #Cria o diretório para salvar os dados

        if isfile(self.checksum_file):
            with open(self.checksum_file, "r") as checksum:
                saved_checksum = checksum.read() # Realiza a leitura do checksum salvo, se houver

        if isfile(self.pathfile):
            with open(self.pathfile, "rb") as filein:
                bytes = filein.read()
                self.checksum = sha256(bytes).hexdigest() # Realiza a leitura do arquivo csv passado e calcula seu checksum

            if saved_checksum != self.checksum:
                print(f"{self.pathfile} sofreu alterações") #Sinaliza em caso de alterações

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def shape(self):
        ###! Retorna a quantidade de casos totais, casos recuperados, óbitos, casos ativos e óbitos por outras causas.
        try:
            return (len(self.__source),len(self.__source.loc[self.__source['cod_evolucao'] == 1]),len(self.__source.loc[self.__source['cod_evolucao'] == 2]),len(self.__source.loc[self.__source['cod_evolucao'] == 3]),len(self.__source.loc[self.__source['cod_evolucao'] == 4]))
        except:
            raise Exception('Não é possivel calcular o tamanho de algo inexistente')

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def read(self,pathfile,append=False):
        """! Método para realizar a leitura de um ou mais csv.
            @param pathfile Caminho relativo ao arquivo csv que contém os dados baixados do Metabase
            @param append Bool que define se ao ler mais de um arquivo será sobrescrito ou agregado
        """
        print(f"reading {pathfile}")
        notifica = pd.read_csv(pathfile,
                           dtype = {
                               'id': 'int32',
                           },
                           converters = {
                               'paciente': normalize_text,
                               'nome_mae': normalize_text,
                               'cpf': normalize_cpf,
                               'cod_tipo_paciente': lambda x: normalize_number(x, fill=99),
                               'tipo_paciente': normalize_text,
                               'sexo': normalize_text,
                               'cod_raca_cor': lambda x: normalize_number(x, fill=99),
                               'raca_cor': normalize_text,
                               'cod_etnia': lambda x: normalize_number(x, fill=0),
                               'etnia': normalize_text,
                               'uf_residencia': lambda x: normalize_number(x, fill=99),
                               'ibge_residencia': lambda x: normalize_number(x, fill=999999),
                               'cod_classificacao_final': lambda x: normalize_number(x, fill=0),
                               'classificacao_final': normalize_text,
                               'cod_criterio_classificacao': lambda x: normalize_number(x, fill=4),
                               'criterio_classificacao': normalize_text,
                               'cod_evolucao': lambda x: normalize_number(x, fill=3),
                               'evolucao': normalize_text,
                            #    'numero_do': lambda x: normalize_number(x, fill=0),
                               'co_seq_exame': lambda x: normalize_number(x, fill=0),
                               'cod_metodo': lambda x: normalize_number(x, fill=3),
                               'metodo': normalize_text,
                               'cod_exame': lambda x: normalize_number(x, fill=0),
                               'exame': normalize_text,
                               'cod_resultado': lambda x: normalize_number(x, fill=0),
                               'resultado': normalize_text,
                               'cod_status_notificacao': lambda x: normalize_number(x, fill=0),
                               'status_notificacao': normalize_text,
                               'cod_origem': lambda x: normalize_number(x,fill=0),
                               'origem': normalize_text,
                               'uf_unidade_notifica': lambda x: normalize_number(x,fill=99),
                               'ibge_unidade_notifica': lambda x: normalize_number(x,fill=999999),
                               'nome_unidade_notifica': normalize_text,
                               'nome_notificador': normalize_text,
                               'email_notificador': normalize_text,
                               'telefone_notificador': normalize_text
                           },
                           parse_dates = ['data_nascimento','data_1o_sintomas','data_cura_obito','data_coleta','data_recebimento','data_liberacao','data_notificacao','updated_at'],
                           date_parser = lambda x: pd.to_datetime(x, errors='coerce', format='%d/%m/%Y')
                        )

        if isinstance(self.__source, pd.DataFrame) and append:
            #se não for nulo e append for verdadeiro, concantena csv passado com csv que já está em source
            self.__source = self.__source.append(notifica, ignore_index=True)
        else:
            #senão adiciona csv no source
            self.__source = notifica

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def load(self):
        try:
            self.__source = pd.read_pickle(self.database)
        except:
            raise Exception(f"Arquivo {self.database} não encontrado")

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def save(self, df=None):
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            notifica = df
        elif isinstance(self.__source, pd.DataFrame) and len(self.__source) > 0:
            notifica = self.__source
        else:
            raise Exception('Não é possível salvar um DataFrame inexistente, realize a leitura antes ou passe como para esse método')

        notifica.to_pickle(self.database)

        with open(self.checksum_file, "w") as checksum:
            try:
                checksum.write(self.checksum)
            except:
                print('checksum não criado')

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def normalize(self):
        if not isinstance(self.__source, pd.DataFrame):
            raise Exception('Não é possível normalizar um arquivo inexistente, realize a leitura antes')

        print('Normalizando dados')
        notifica = self.__source

        notifica.loc[((notifica['tipo_paciente'].isnull()) | (notifica['tipo_paciente'] == '')),'cod_tipo_paciente'] = 99
        notifica.loc[((notifica['tipo_paciente'].isnull()) | (notifica['tipo_paciente'] == '')),'tipo_paciente'] = 'IGNORADO'

        mask = notifica['data_nascimento'].apply(lambda x: isvaliddate(x, begin=date(1900,1,1)))
        notifica.loc[~mask, 'data_nascimento'] = pd.NaT

        mask = notifica['data_1o_sintomas'].apply(isvaliddate)
        notifica.loc[~mask, 'data_1o_sintomas'] = pd.NaT

        mask = notifica['data_cura_obito'].apply(isvaliddate)
        notifica.loc[~mask, 'data_cura_obito'] = pd.NaT

        mask = notifica['data_coleta'].apply(isvaliddate)
        notifica.loc[~mask, 'data_coleta'] = pd.NaT

        mask = notifica['data_recebimento'].apply(isvaliddate)
        notifica.loc[~mask, 'data_recebimento'] = pd.NaT

        mask = notifica['data_liberacao'].apply(isvaliddate)
        notifica.loc[~mask, 'data_liberacao'] = pd.NaT

        mask = notifica['data_notificacao'].apply(isvaliddate)
        notifica.loc[~mask, 'data_notificacao'] = pd.NaT

        mask = notifica['updated_at'].apply(isvaliddate)
        notifica.loc[~mask, 'updated_at'] = pd.NaT

        notifica.loc[(notifica['data_nascimento'].notnull()) & (notifica['data_notificacao'].notnull()),'idade'] = notifica.loc[(notifica['data_nascimento'].notnull()) & (notifica['data_notificacao'].notnull())].apply(lambda row: row['data_notificacao'].year - row['data_nascimento'].year - ((row['data_notificacao'].month, row['data_notificacao'].day) < (row['data_nascimento'].month, row['data_nascimento'].day)), axis=1)
        notifica.loc[notifica['data_nascimento'].isnull(),'idade'] = -99
        notifica['idade'] = notifica['idade'].apply(int)

        notifica.loc[((notifica['raca_cor'].isnull()) | (notifica['raca_cor'] == '')),'cod_raca_cor'] = 99
        notifica.loc[((notifica['raca_cor'].isnull()) | (notifica['raca_cor'] == '')),'raca_cor'] = 'IGNORADO'

        notifica.loc[((notifica['etnia'].isnull()) | (notifica['etnia'] == '')),'cod_etnia'] = 0

        notifica.loc[notifica['uf_residencia'] == 0,'uf_residencia'] = 99
        notifica.loc[notifica['ibge_residencia'] == 0,'ibge_residencia'] = 999999

        notifica.loc[((notifica['classificacao_final'].isnull()) | (notifica['classificacao_final'] == '')),'classificacao_final'] = 'IGNORADO'

        notifica.loc[((notifica['criterio_classificacao'].isnull()) | (notifica['criterio_classificacao'] == '')),'cod_criterio_classificacao'] = 4
        notifica.loc[((notifica['criterio_classificacao'].isnull()) | (notifica['criterio_classificacao'] == '')),'criterio_classificacao'] = 'NAO SE APLICA'

        notifica.loc[((notifica['evolucao'].isnull()) | (notifica['evolucao'] == '')),'cod_evolucao'] = 3
        notifica.loc[((notifica['evolucao'].isnull()) | (notifica['evolucao'] == '')),'evolucao'] = 'NAO SE APLICA'

        notifica.loc[((notifica['metodo'].isnull()) | (notifica['metodo'] == '')),'cod_metodo'] = 3
        notifica.loc[((notifica['metodo'].isnull()) | (notifica['metodo'] == '')),'metodo'] = 'NAO INFORMADO'

        notifica.loc[((notifica['exame'].isnull()) | (notifica['exame'] == '')),'cod_exame'] = 0
        notifica.loc[((notifica['exame'].isnull()) | (notifica['exame'] == '')),'exame'] = 'NAO INFORMADO'

        notifica.loc[((notifica['resultado'].isnull()) | (notifica['resultado'] == '')),'cod_resultado'] = 0
        notifica.loc[((notifica['resultado'].isnull()) | (notifica['resultado'] == '')),'resultado'] = 'NAO INFORMADO'

        notifica.loc[((notifica['status_notificacao'].isnull()) | (notifica['status_notificacao'] == '')),'status_notificacao'] = 'IGNORADO'

        notifica.loc[((notifica['origem'].isnull()) | (notifica['origem'] == '')),'cod_origem'] = 0
        notifica.loc[((notifica['origem'].isnull()) | (notifica['origem'] == '')),'origem'] = 'NAO INFORMADO'

        notifica.loc[notifica['uf_unidade_notifica'] == 0,'uf_unidade_notifica'] = 99
        notifica.loc[notifica['ibge_unidade_notifica'] == 0,'ibge_unidade_notifica'] = 999999

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

        nao = set(['NAO','CONSTA','INFO','INFORMADO','CONTEM',''])
        notifica.loc[ [True if set(nome_mae.split(" ")).intersection(nao) else False for nome_mae in notifica['nome_mae'] ], 'nome_mae'] = None

        notifica.loc[notifica['mun_resid'].notnull() & (notifica['idade']!=-99), 'hash_idade_resid'] = notifica.loc[notifica['mun_resid'].notnull()].apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        notifica.loc[notifica['mun_atend'].notnull() & (notifica['idade']!=-99), 'hash_idade_atend'] = notifica.loc[notifica['mun_atend'].notnull()].apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_atend']), axis=1)
        notifica.loc[ notifica['nome_mae'].notnull(), 'hash_mae'] = notifica.loc[ notifica['nome_mae'].notnull() ].apply(lambda row: normalize_hash(row['paciente'])+normalize_hash(row['nome_mae']), axis=1)
        notifica.loc[notifica['data_nascimento']!=pd.NaT, 'hash_nasc'] = notifica.loc[notifica['data_nascimento']!=pd.NaT].apply(lambda row: normalize_hash(row['paciente'])+str(row['data_nascimento']).replace('-',''), axis=1)

        self.__source = notifica
        print('Finished')

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def get_casos(self):
        return self.__source.copy()

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def get_obitos(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 2].copy()

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def get_recuperados(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 1].copy()

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def get_casos_ativos(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 3].copy()

#-------------------------------------------------------------------------------------------------------------------------------------------------------------#

    def get_obitos_nao_covid(self):
        return self.__source.loc[self.__source['cod_evolucao'] == 4].copy()