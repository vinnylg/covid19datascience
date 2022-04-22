#-----------------------------------------------------------------------------------------------------------------------------#
# Classe de tratamento de dados do SIVEP-Gripe Paraná
# Todos os direitos reservados ao autor
#-----------------------------------------------------------------------------------------------------------------------------#

from os.path import isdir, join
from os import makedirs
from glob import glob
import pandas as pd
from pathlib import Path
from simpledbf import Dbf5
from bulletin import default_input, root, tables_path
from bulletin.utils.timer import Timer
from bulletin.utils.normalize import date_hash, normalize_hash

class Sivep:

    def __init__(self, database=None):
        self.df = None
        self.database_dir = join(root, 'database', 'sivep')
        self.databases = lambda: sorted([ Path(path).stem for path in glob(join(self.database_dir,"*.pkl"))])
        print(self.databases())

        self.database = database if not database is None else self.databases()[-1] if len(self.databases()) > 0 else "sraghospitalizado"
        
        notificacao_schema = pd.read_csv(join(root,'systems','information_schema.csv'))
        notificacao_schema = notificacao_schema.loc[notificacao_schema['notifica']==1]

        self.schema = notificacao_schema
        self.tables = dict([(Path(x).stem,pd.read_csv(join(tables_path,x))) for x in glob(join(tables_path,"*.csv"))])
        self.cols = dict(notificacao_schema.loc[notificacao_schema['sivep'].notna(),['sivep','column']].values)
        self.fillna = dict(notificacao_schema.loc[(notificacao_schema['sivep'].notna()) & (notificacao_schema['fillna'].notna()),['sivep','fillna']].values)
        self.dtypes = dict(notificacao_schema.loc[notificacao_schema['sivep'].notna(),['sivep','dtypes']].values)
        self.sivep_parser = dict([[column,eval(sivep_parser)] for column, sivep_parser in notificacao_schema.loc[(notificacao_schema['sivep_parser'].notna()),['sivep','sivep_parser']].values])
        self.converters = dict([[column,eval(converters)] for column, converters in notificacao_schema.loc[(notificacao_schema['converters'].notna()),['sivep','converters']].values])

        if not isdir(self.database_dir):
            makedirs(self.database_dir)

    # Transform sivep values to notifica values
    @Timer(init_msg="Remap sivep values to notifica values...",end="\n\n")
    
    def normalize(self):
        for col in self.schema.loc[self.schema['converters'].notna(),'sivep']:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self.converters[col])
                
        for col in self.schema.loc[self.schema['dtypes']=='datetime64[ns]','sivep']:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce', format='%d/%m/%Y')
        self.fix_dtypes()

    def to_notifica(self):
        self.normalize()

        self.df.loc[self.df['TP_ANTIVIR'] == 1, 'ANTIVIRAL'] = 3
        self.df.loc[self.df['TP_ANTIVIR'] == 2, 'ANTIVIRAL'] = 1
        self.df.loc[(self.df['TP_ANTIVIR'] == 3) & (self.df['ANTIVIRAL'] != 1), 'ANTIVIRAL'] = 9
        self.df.loc[(self.df['TP_ANTIVIR'] == -1) & (self.df['ANTIVIRAL'] != 1), 'ANTIVIRAL'] = -1
        
        self.df.loc[(self.df['PERD_PALA'] == 2) & (self.df['PERD_OLFT'] != 1), 'PERD_OLFT'] = 2
        self.df.loc[self.df['PERD_PALA'] == 1, 'PERD_OLFT'] = 1
        self.df.loc[(self.df['PERD_PALA'] == 9) & (self.df['PERD_OLFT'] != 1), 'PERD_OLFT'] = 9
        self.df.loc[(self.df['PERD_PALA'] == -1) & (self.df['PERD_OLFT'] != 1), 'PERD_OLFT'] = -1
        
        self.df.loc[(self.df['TP_IDADE'] == 1) | (self.df['TP_IDADE'] == 2), 'NU_IDADE_N'] = 0
        
        for col in self.df.columns.values:
            if col not in self.schema.loc[self.schema['sivep'].notna(),'sivep'].values:
                del self.df[col]

        self.sivep_parser['CS_SEXO'] = {'M':1, 'F':2, 'I':3}
        self.df = self.df.replace(self.sivep_parser)  
        self.df = self.df.rename(columns=self.cols)

        # Seleciona melhor data de diagnóstico dentre as datas validas na ordem de prioridade: 
        # data_coleta -> data_recebimento -> data_liberacao -> data_notificacao
        with Timer(init_msg="Seleciona melhor data de diagnóstico dentre as datas validas"):
            self.df['data_diagnostico'] = self.df['data_notificacao']
            self.df.loc[self.df['data_liberacao'].notnull(), 'data_diagnostico'] = self.df.loc[self.df['data_liberacao'].notnull(), 'data_liberacao']
            self.df.loc[self.df['data_coleta'].notnull(), 'data_diagnostico'] = self.df.loc[self.df['data_coleta'].notnull(), 'data_coleta']

        self.hashes()

    def __len__(self):
        return len(self.df)


    def __str__(self):
        return self.database


    def fix_dtypes(self):
        assert not self.df is None
        
        for col in self.schema.loc[(self.schema['sivep'].notna()) & (self.schema['fillna'].notna()),'sivep'].values:
            if col != 'CS_SEXO':
                self.df[col] = self.df[col].fillna(self.fillna[col]).astype(self.dtypes[col])
                    
    @Timer()
    def check_duplicates(self,keep=False):
        assert not self.df is None

        self.df['duplicated'] = ''
        for col in [ col for col in self.df.columns if ('hash' in col) or (col in ['cpf','cns']) ]:
            duplicated = (self.df[col].notna())&(self.df.duplicated(col,keep=keep))
            print(f"duplicated(keep={str(keep)}) in {col}: {duplicated.sum()}")
            self.df.loc[duplicated,'duplicated'] =  self.df.loc[duplicated,'duplicated'].apply( lambda l: utils.strlist(l,col) )
        print("")

    @Timer()
    def hashes(self):
        assert not self.df is None
        #self.fix_dtypes()

        for col in [ col for col in self.df.columns if 'hash' in col ]:
            del self.df[col]
        
        with Timer('hash nome_mae'):
            try:
                self.df.loc[self.df['nome_mae'].notna(),'hash_mae'] = ( self.df.loc[self.df['nome_mae'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['nome_mae'].notna(),'nome_mae'].apply(normalize_hash) )
            except:
                print('sem nome_mae')

            
        with Timer('hash data_nascimento'):
            try:
                self.df.loc[self.df['data_nascimento'].notna(),'hash_nasc'] = ( self.df.loc[self.df['data_nascimento'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['data_nascimento'].notna(),'data_nascimento'].apply(date_hash) )
            except:
                print('sem data_nascimento')
                
        with Timer('hash hash_resid'):
            try:
                self.df['hash_resid'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].astype(str) + self.df['ibge_residencia'].astype(str)
            except:
                print('sem hash_resid')

        with Timer('hash hash_atend'):
            try:
                self.df['hash_atend'] = self.df['paciente'].apply(normalize_hash) + self.df['idade'].astype(str) + self.df['ibge_unidade_notifica'].astype(str)
            except:
                print('sem hash_atend')
                
        with Timer('hash data_diagnostico'):
            try:
                self.df.loc[self.df['data_diagnostico'].notna(),'hash_diag'] = ( self.df.loc[self.df['data_diagnostico'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['data_diagnostico'].notna(),'data_diagnostico'].apply(date_hash) )  
            except:
                print('sem data_diagnostico')

        with Timer('hash data_liberacao'):
            try:
                self.df.loc[self.df['data_liberacao'].notna(),'hash_lib'] = ( self.df.loc[self.df['data_liberacao'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['data_liberacao'].notna(),'data_liberacao'].apply(date_hash) )
            except:
                print('sem data_liberacao')


    @Timer()
    def read(self, pathfile, append=False, format='dbf'):
        print(f"Reading {pathfile}")

        if (format == 'dbf'):
            df = Dbf5(pathfile, codec = 'latin-1').to_dataframe()
        else:
            df = pd.read_excel(pathfile)

        if isinstance(self.df, pd.DataFrame) and append:
            print(f"Appending {pathfile}")
            self.df = self.df.append(df, ignore_index = True)
        else:
            print(f"Attrb {pathfile}")
            self.df = df

        print(f"{len(self.df)} linhas com {len(self.df.columns)} colunas lidas")
    

    def read_all_database_files_dbf(self):
        last_digit = ['0', '1', '2']

        for x in last_digit:
            try:
                self.read(join(default_input,'sivep',f"SRAGHOSPITALIZADO202{x}.dbf"), append=True, format='dbf')
            except: 
                pass

    def read_all_database_files_excel(self):
        last_digit = ['0', '1', '2']

        for x in last_digit:
            try:
                self.read(join(default_input,'sivep',f"SRAGHOSPITALIZADO202{x}.xlsx"), append=True, format='xlsx')
            except: 
                pass
            
    @Timer()
    def load(self, database=None, compress=True):
        if not database is None:
            self.database = database

        if not self.database in self.databases():
            raise Exception(f"{self.database} not found")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")
        
        if compress:
            self.df = pd.read_pickle(pathfile,'bz2')    
        else:
            self.df = pd.read_pickle(pathfile)

    @Timer()
    def save(self,database=None,replace=False, compress=True):
        assert not self.df is None
   
        if not database is None:
            self.database = database

        if self.database in self.databases() and not replace:
            raise Exception(f"{self.database} already saved, set replace=True to replace")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")

        if compress:
            self.df.to_pickle(pathfile,'bz2')
        else:
            self.df.to_pickle(pathfile)
    
    #Métodos usados após to_notifica()
    
    def shape(self):
        return (len(self.df.loc[(self.df['classificacao_final'] == 2)]),
                len(self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 2)]),
                len(self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 1)]),
                len(self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 3)]))
    
    def get_casos(self):
        return self.df.loc[(self.df['classificacao_final'] == 2)].copy()

    def get_obitos(self):
        return self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 2)].copy()

    def get_recuperados(self):
        return self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 1)].copy()

    def get_casos_ativos(self):
        return self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 3)].copy()

    def get_obitos_nao_covid(self):
        return self.df.loc[(self.df['classificacao_final'] == 2) & (self.df['evolucao'] == 4) ].copy()

