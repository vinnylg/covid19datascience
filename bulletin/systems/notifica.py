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
import glob
from pathlib import Path




# ----------------------------------------------------------------------------------------------------------------------
class Notifica:
    municipios = Municipios()
    municipios['mun_resid'] = municipios['municipio'].apply(normalize_text)
    municipios.loc[municipios['uf']!='PR','mun_resid'] = municipios.loc[municipios['uf']!='PR','municipio'].apply(normalize_text) + '/' + municipios['uf']

    # ----------------------------------------------------------------------------------------------------------------------
    def __init__(self,database='notifica'):
        self.df = None
        self.database_dir = join(root, 'database', 'notifica')
        self.database = database

        if not isdir(self.database_dir):
            makedirs(self.database_dir)

        self.databases = lambda: sorted([ Path(path).stem for path in glob.glob(join(self.database_dir,"*.pkl"))])
        print(self.databases())

            
        if isfile(join(root,'resources','tables','notificacao_schema.csv')):
            notificacao_schema = pd.read_csv(join(root,'resources','tables','notificacao_schema.csv'))
            self.schema = notificacao_schema
            self.dtypes = dict(notificacao_schema.loc[notificacao_schema['converters'].isna(),['column','dtypes']].values)
            self.converters = dict([[column,eval(converters)] for column, converters in notificacao_schema.loc[(notificacao_schema['converters'].notna()),['column','converters']].values])
        else:
            raise Exception(f"notificacao_schema.csv not found in {join(root,'resources','tables')}")


    def __len__(self):
        return len(self.df)

    def __str__(self):
        return self.database

    def check_duplicates(self):
        assert not self.df is None

        self.df['duplicated'] = False
        for col in [ col for col in self.df.columns if 'hash' in col ]:
            print('duplicated in ',col,':',len(self.df.loc[(self.df[col].notna())&(self.df.duplicated(col,keep=False))]))
            self.df.loc[(self.df[col].notna())&(self.df.duplicated(col,keep=False)),'duplicated'] = True

    def hashes(self):
        assert not self.df is None

        for col in [ col for col in self.df.columns if 'hash' in col ]:
            del self.df[col]
        
        self.df.loc[self.df['nome_mae'].notna(),'hash_mae'] = ( self.df.loc[self.df['nome_mae'].notna(),'paciente'].apply(normalize_hash) +
                                                                  self.df.loc[self.df['nome_mae'].notna(),'nome_mae'].apply(normalize_hash) )

        self.df.loc[self.df['data_nascimento'].notna(),'hash_nasc'] = ( self.df.loc[self.df['data_nascimento'].notna(),'paciente'].apply(normalize_hash) +
                                                                          self.df.loc[self.df['data_nascimento'].notna(),'data_nascimento'].apply(date_hash) )
                                                                          
        self.df['hash_resid'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].astype(str) + self.df['ibge_residencia'].astype(str)
        self.df['hash_atend'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].astype(str) + self.df['ibge_unidade_notifica'].astype(str)

        self.df.loc[self.df['data_diagnostico'].notna(),'hash_diag'] = self.df.loc[self.df['data_diagnostico'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['data_diagnostico'].notna(),'data_diagnostico'].apply(date_hash)
        self.df.loc[self.df['data_liberacao'].notna(),'hash_lib'] = self.df.loc[self.df['data_liberacao'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['data_liberacao'].notna(),'data_liberacao'].apply(date_hash)

    def fix_dtypes(self):
        assert not self.df is None
        
        cols = pd.DataFrame(zip(self.df.columns,self.df.dtypes),columns=['col','dtype']).set_index('col')
        floats = cols.loc[cols['dtype']=='float64']
        if len(floats):
            for col in floats.index:
                self.df[col] = self.df[col].fillna(-1).apply(int)

                
    # ----------------------------------------------------------------------------------------------------------------------
    @Timer('reading Notifica')
    def read(self, pathfile, append=False, normalize=True):
        print(f"Reading {pathfile}")
        notifica = pd.read_csv(
                pathfile,
                dtype=self.dtypes,
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
        self.fix_dtypes()

    #gambiarra
    def fix_dtypes(self):
        if self.df is None:
            raise Exception(f"self.df is none")
        
        cols = pd.DataFrame(zip(self.df.columns,self.df.dtypes),columns=['col','dtype']).set_index('col')
        floats = cols.loc[cols['dtype']=='float64']
        if len(floats):
            for col in floats.index:
                self.df[col] = self.df[col].fillna(-1).astype(int)

    @Timer('saving Notifica to pkl')
    def save(self,database=None,replace=False):
        self.fix_dtypes()

        if not database is None:
            self.database = database

        if self.database in self.databases() and not replace:
            raise Exception(f"{self.database} already saved, set replace=True to replace")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")

        self.df.to_pickle(pathfile)
            
            
    @Timer('loading Notifica from pkl')
    def load(self, database=None):        
        if not database is None:
            self.database = database

        if not self.database in self.databases():
            raise Exception(f"{self.database} not found")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")
        self.df = pd.read_pickle(pathfile)
        self.fix_dtypes()

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
        
        return notifica

    # ----------------------------------------------------------------------------------------------------------------------
    def update(self, new_notifica):
        novas_notificacoes = new_notifica.loc[~new_notifica['id'].isin(self.df['id'])]
        print(f"novas_notificacoes {len(novas_notificacoes)}")
        possiveis_atualizacoes = new_notifica.loc[new_notifica['id'].isin(self.df['id'])]
        print(f"possiveis_atualizacoes {len(possiveis_atualizacoes)}")

        novas_notificacoes = novas_notificacoes.set_index('id')
        self.df = self.df.set_index('id')
        self.df = self.df.append(novas_notificacoes)

        self.df.update(possiveis_atualizacoes)
        self.df = self.df.reset_index()