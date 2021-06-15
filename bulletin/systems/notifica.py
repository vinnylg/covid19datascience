# -----------------------------------------------------------------------------------------------------------------------------#
# Classe de tratamento de dados do Notifica Covid-19 Paraná
# Todos os direitos reservados ao autor
# -----------------------------------------------------------------------------------------------------------------------------#
import os
from os.path import dirname, join, isdir, isfile
from datetime import datetime, date
from os import makedirs
import pandas as pd

from bulletin import root, default_input, default_output
from bulletin.utils import static
from bulletin.utils.timer import Timer
from bulletin.utils.utils import isvaliddate
from bulletin.utils.normalize import date_hash, normalize_cpf, normalize_text, normalize_number, normalize_hash, normalize_campo_aberto, normalize_do, normalize_cns
from bulletin.utils.static import Municipios



municipios = Municipios()
municipios['mun_resid'] = municipios['municipio'].apply(normalize_text)
municipios.loc[municipios['uf']!='PR','mun_resid'] = municipios.loc[municipios['uf']!='PR','municipio'].apply(normalize_text) + '/' + municipios['uf']

# ----------------------------------------------------------------------------------------------------------------------
class Notifica:

    # ----------------------------------------------------------------------------------------------------------------------
    def __init__(self,database='notifica'):
        self.df = None
        self.database = join(root,'database', f"{database}.pkl")

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))
            
        if isfile(join(root,'resources','tables','notificacao_schema.csv')):
            notificacao_schema = pd.read_csv(join(root,'resources','tables','notificacao_schema.csv'))
            self.schema = notificacao_schema
            self.dtypes = dict(notificacao_schema.loc[notificacao_schema['converters'].isna(),['column','dtypes']].values)
            self.converters = dict([[column,eval(converters)] for column, converters in notificacao_schema.loc[(notificacao_schema['converters'].notna()),['column','converters']].values])
        else:
            raise Exception(f"notificacao_schema.csv not found in {join(root,'resources','tables')}")

    # ----------------------------------------------------------------------------------------------------------------------
    def __len__(self):
        if isinstance(self.df,pd.DataFrame) and len(self.df) > 0: 
            return len(self.df)
        else:
            return -1

    # ----------------------------------------------------------------------------------------------------------------------
    def shape(self):
        if isinstance(self.df,pd.DataFrame) and len(self.df) > 0:   
            return tuple([len(self.df.loc[self.df['evolucao'] == evolucao]) for evolucao in [1, 2, 3, 4]])
        else:
            return -1

    # ----------------------------------------------------------------------------------------------------------------------
    @Timer('reading Notifica')
    def read(self, pathfile, append=False, normalize=True):
        print(f"Reading {pathfile}")
        notifica = pd.read_csv(
                pathfile,
                # dtype=self.dtypes,
                converters=self.converters
        )

        if normalize:
            notifica = self.__normalize(notifica)
        
        if isinstance(self.df, pd.DataFrame) and append:
            print(f"Appending")
            self.df = self.df.append(notifica)
        else:
            print(f"Attrb")
            self.df = notifica

        print(len(self.df))

    # ----------------------------------------------------------------------------------------------------------------------
    def load(self):
        try:
            self.df = pd.read_pickle(self.database)
        except ValueError:
            raise Exception(f"{ValueError}\nArquivo {self.database} não encontrado")

    # ----------------------------------------------------------------------------------------------------------------------
    def save(self, df=None):
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            new_df = df
            self.df = new_df
        elif isinstance(self.df, pd.DataFrame) and len(self.df) > 0:
            new_df = self.df
        else:
            raise Exception(
                    'Não é possível salvar um DataFrame inexistente, '
                    'realize a leitura antes ou passe como para esse método'
            )

        new_df.to_pickle(self.database)

    # ----------------------------------------------------------------------------------------------------------------------
    # Normaliza strings, datas e códigos. Anula valores incorretos.
    def __normalize(self, notifica):
        if not (isinstance(notifica, pd.DataFrame) and len(notifica) > 0):
            raise Exception(f"Dataframe: {notifica} vazio")

        print('normalize notifica')

        # Seleciona melhor data de diagnóstico dentre as datas validas na ordem de prioridade: 
        # data_coleta -> data_recebimento -> data_liberacao -> data_notificacao
        notifica['data_diagnostico'] = notifica['data_notificacao']
        notifica.loc[notifica['data_liberacao'].notnull(), 'data_diagnostico'] = notifica.loc[notifica['data_liberacao'].notnull(), 'data_liberacao']
        notifica.loc[notifica['data_recebimento'].notnull(), 'data_diagnostico'] = notifica.loc[notifica['data_recebimento'].notnull(), 'data_recebimento']
        notifica.loc[notifica['data_coleta'].notnull(), 'data_diagnostico'] = notifica.loc[notifica['data_coleta'].notnull(), 'data_coleta']

        # Calcula a idade com base na data de notificação
        notifica.loc[(notifica['data_nascimento'].notnull()) & (notifica['data_notificacao'].notnull()), 'idade'] = \
            notifica.loc[(notifica['data_nascimento'].notnull()) & (notifica['data_notificacao'].notnull())].apply(
                    lambda row: row['data_notificacao'].year - row['data_nascimento'].year - (
                            (row['data_notificacao'].month, row['data_notificacao'].day) <
                            (row['data_nascimento'].month, row['data_nascimento'].day)
                    ), axis=1
            )
        notifica.loc[notifica['data_nascimento'].isnull(), 'idade'] = -99
        notifica['idade'] = notifica['idade'].apply(lambda x: normalize_number(x, fill=-99))

        # Atribui código 99/999999 ou valores correspondentes à coluna nos campos não especificados ou nulos
        notifica.loc[notifica['uf_residencia'] == 0, 'uf_residencia'] = 99
        notifica.loc[notifica['ibge_residencia'] == 0, 'ibge_residencia'] = 999999
        notifica.loc[notifica['uf_unidade_notifica'] == 0, 'uf_unidade_notifica'] = 99
        notifica.loc[notifica['ibge_unidade_notifica'] == 0, 'ibge_unidade_notifica'] = 999999
        
        notifica['nome_mae'] = notifica['nome_mae'].apply(normalize_campo_aberto)

        notifica.loc[notifica['evolucao']==5, 'evolucao'] = 2
        notifica.loc[notifica['evolucao']==0, 'evolucao'] = 3

        notifica['hash'] = (notifica['paciente'].apply(normalize_hash) +
                          notifica['idade'].astype(str) +
                          notifica['ibge_residencia'].astype(str) )

        notifica['hash_atend'] = (notifica['paciente'].apply(normalize_hash) +
                          notifica['idade'].astype(str) +
                          notifica['ibge_unidade_notifica'].astype(str) )

        notifica.loc[notifica['nome_mae'].notna(),'hash_mae'] = ( notifica.loc[notifica['nome_mae'].notna(),'paciente'].apply(normalize_hash) +
                                                                  notifica.loc[notifica['nome_mae'].notna(),'nome_mae'].apply(normalize_hash) )

        notifica.loc[notifica['data_nascimento'].notna(),'hash_nasc'] = ( notifica.loc[notifica['data_nascimento'].notna(),'paciente'].apply(normalize_hash) +
                                                                          notifica.loc[notifica['data_nascimento'].notna(),'data_nascimento'].apply(date_hash) )
        
        return notifica

    # ----------------------------------------------------------------------------------------------------------------------
    def verify_changes(self, database):
        pass

    # ----------------------------------------------------------------------------------------------------------------------
    def get_casos(self):
        return self.df.copy()

    # ----------------------------------------------------------------------------------------------------------------------
    def get_obitos(self):
        return self.df.loc[self.df['evolucao'] == 2].copy()

    # ----------------------------------------------------------------------------------------------------------------------
    def get_recuperados(self):
        return self.df.loc[self.df['evolucao'] == 1].copy()

    # ----------------------------------------------------------------------------------------------------------------------
    def get_casos_ativos(self):
        return self.df.loc[self.df['evolucao'] == 3].copy()

    # ----------------------------------------------------------------------------------------------------------------------
    def get_obitos_nao_covid(self):
        return self.df.loc[self.df['evolucao'] == 4].copy()
