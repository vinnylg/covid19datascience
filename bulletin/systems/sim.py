#-----------------------------------------------------------------------------------------------------------------------------#
# Classe de tratamento de dados do SIM Paraná
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

class Sim:
    
    def __init__(self, database=None):
        self.df = None
        self.database_dir = join(root, 'database', 'sim')
        self.databases = lambda: sorted([ Path(path).stem for path in glob(join(self.database_dir,"*.pkl"))])
        print(self.databases())

        self.database = database if not database is None else self.databases()[-1] if len(self.databases()) > 0 else "dopr"
        
        notificacao_schema = pd.read_csv(join(root,'systems','information_schema.csv'))
        notificacao_schema = notificacao_schema.loc[notificacao_schema['notifica']==1]

        self.schema = notificacao_schema
        self.tables = dict([(Path(x).stem,pd.read_csv(join(tables_path,x))) for x in glob(join(tables_path,"*.csv"))])
        self.cols = dict(notificacao_schema.loc[notificacao_schema['sim'].notna(),['sim','column']].values)
        self.fillna = dict(notificacao_schema.loc[(notificacao_schema['sim'].notna()) & (notificacao_schema['fillna'].notna()),['sim','fillna']].values)
        self.dtypes = dict(notificacao_schema.loc[notificacao_schema['sim'].notna(),['sim','dtypes']].values)
        self.sim_parser = dict([[column,eval(sim_parser)] for column, sim_parser in notificacao_schema.loc[(notificacao_schema['sim_parser'].notna()),['sim','sim_parser']].values])
        self.converters = dict([[column,eval(converters)] for column, converters in notificacao_schema.loc[(notificacao_schema['converters'].notna()),['sim','converters']].values])

        if not isdir(self.database_dir):
            makedirs(self.database_dir)

    def normalize(self):
        self.df['NUMERODO'] = self.df['NUMERODO'].str.cat(self.df['NUMERODV'])
        
        for col in self.schema.loc[self.schema['converters'].notna(),'sim']:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self.converters[col])
                
        for col in self.schema.loc[self.schema['dtypes']=='datetime64[ns]','sim']:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce', format='%d%m%Y')
        self.fix_dtypes()

    def to_notifica(self):
        self.normalize()
      
        self.df['IDADE'] = self.df['IDADE'].fillna('999').astype(self.dtypes['IDADE']).sub(400)
        self.df.loc[self.df['IDADE']<0, 'IDADE'] = 0
        self.df.loc[self.df['IDADE']==599, 'IDADE'] = -99
        
        self.df.loc[(
            self.df['LINHAA'].str.contains('B342') |
            self.df['LINHAB'].str.contains('B342') |
            self.df['LINHAC'].str.contains('B342') |
            self.df['LINHAD'].str.contains('B342') |
            self.df['LINHAII'].str.contains('B342') |
            self.df['CAUSABAS'].str.contains('B342')
        ), 'evolucao'] = 2
        
        self.df.loc[self.df['evolucao']!=2, 'evolucao'] = 4
        self.df['evolucao'] = self.df['evolucao'].astype('int')
        
        for col in self.df.columns.values:
            if col not in self.schema.loc[self.schema['sim'].notna(),'sim'].values:
                if col != 'evolucao':
                    del self.df[col]

        self.sim_parser['SEXO'] = {'M':1, 'F':2, 'I':3}
        self.df = self.df.replace(self.sim_parser)  
        self.df = self.df.rename(columns=self.cols)
        self.hashes()

    def __len__(self):
        return len(self.df)


    def __str__(self):
        return self.database


    def fix_dtypes(self):
        assert not self.df is None
        
        for col in self.schema.loc[(self.schema['sim'].notna()) & (self.schema['fillna'].notna()),'sim'].values:
            if col != 'SEXO':
                self.df[col] = self.df[col].fillna(self.fillna[col]).astype(self.dtypes[col])

    @Timer()
    def read(self, pathfile, append=False):
        print(f"Reading {pathfile}")
        df = Dbf5(pathfile, codec = 'latin-1').to_dataframe()

        if isinstance(self.df, pd.DataFrame) and append:
            print(f"Appending {pathfile}")
            self.df = self.df.append(df, ignore_index = True)
        else:
            print(f"Attrb {pathfile}")
            self.df = df

        print(f"{len(self.df)} linhas com {len(self.df.columns)} colunas lidas")
    
    def read_all_database_files(self):
        last_digit = ['0', '1', '2']

        for x in last_digit:
            self.read(join(default_input,'sim',f"DOPR202{x}.dbf"), append=True)

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

    #Métodos usados após to_notifica()
    
    def shape(self):
        return (len(self.df.loc[self.df['evolucao'] == 2]),
                len(self.df.loc[self.df['evolucao'] == 4]))

    def get_obitos(self):
        return self.df.loc[self.df['evolucao'] == 2].copy()

    def get_obitos_nao_covid(self):
        return self.df.loc[self.df['evolucao'] == 4].copy()

