from base64 import decode
from os import makedirs
from os.path import isdir, join
from pathlib import Path
import pandas as pd
import glob
import abc
import yaml

import logging

from bulletin import root, tables_path, default_input, default_output, hoje, menor_data_nascimento, data_comeco_pandemia, logging_file
from bulletin.utils.normalize import *
from bulletin.utils import utils

logger = logging.getLogger(__name__)

class System(abc.ABC):
    @abc.abstractmethod
    def __init__(self, name:str, database:str=None):
        """
        __init__ Inicia variaveis e esquemas do sistema


        Args:
            name (str): nome do sistema
            database (str, optional): nome do banco que será utilizado. Defaults to None.
        """
        self.__name__ = name

        with open(join(root,'config.yaml')) as f:
            try:
                self.config = yaml.safe_load(f)[name]
                logger.info(f"load configs")
            except:
                self.config = None

        self.database_dir = join(root, 'database', name)

        if not isdir(self.database_dir):
            makedirs(self.database_dir)

        self.databases = lambda: sorted([ Path(path).stem for path in glob.glob(join(self.database_dir,"*.pkl"))])
        self.database = database

        notificacao_schema = pd.read_csv(join(root,'systems','information_schema.csv'))
        notificacao_schema = notificacao_schema.loc[notificacao_schema[name].notna()]

        self.schema = notificacao_schema
        self.dtypes = dict(notificacao_schema.loc[notificacao_schema['converters'].isna(),['column','dtypes']].values)
        self.converters = dict([[column,eval(converters)] for column, converters in notificacao_schema.loc[(notificacao_schema['converters'].notna()),['column','converters']].values])

        self.tables = dict([(x,pd.read_csv(join(tables_path,x+'.csv'))) for x in self.schema.loc[self.schema['constraint_name'].notna(),'foreign_table_name']])

        self.df = None # pd.DataFrame to store and process data

        self.default_input = join(default_input, name)
        if not isdir(self.default_input):
            makedirs(self.default_input)

        self.default_output = join(default_output, name)
        if not isdir(self.default_output):
            makedirs(self.default_output)

        logger.info(f"The system {self.__str__()} was been init ")

    def __len__(self):
        return len(self.df)
    
    def __str__(self):
        try:
            return f"{self.__name__.capitalize()}{'(' if not self.database is None else ''}{self.database}{')' if not self.database is None else ''}"
        except:
            return f"{self.__name__.capitalize()}"

    def __repr__(self):
        return self.__str__()

    def shape(self):
        assert not self.df is None
        return self.df.shape

    @abc.abstractmethod
    def download_all():
        """
        download_all Download all database

        For init or rebase database.

        Args:
            load_downloaded (bool, optional): _description_. Defaults to False.
        """
        pass

    @abc.abstractmethod
    def download_update():
        """
        download_update _summary_

        For update alright downloaded database
        """
        pass

    def read(self, pathfile, append=False):
        """
        read _summary_

        _extended_summary_

        Args:
            pathfile (_type_): _description_
            append (bool, optional): _description_. Defaults to False.
        """

        if isinstance(pathfile,list):
            for path in pathfile:
                self.read(path,True)
        else:
            logger.info(f"Reading file: {pathfile}")
            df = pd.read_csv(
                    pathfile,
                    low_memory=False,
                    dtype=str
            )
            logger.info(f"{len(df)} rows and {len(df.columns)} columns was been read")

            if isinstance(self.df, pd.DataFrame) and append:
                self.df = self.df.append(df, ignore_index=True)
                logger.info(f"Which was been appended in {self.__str__()}.df (with new shape of {self.df.shape})")
            else:
                logger.info(f"Which was been attributed to {self.__str__()}.df")
                self.df = df


    def save(self,database:str=None,replace:bool=False, compress:bool=True):
        """
        save _summary_

        _extended_summary_

        Args:
            database (_type_, optional): _description_. Defaults to None.
            replace (bool, optional): _description_. Defaults to False.
            compress (bool, optional): _description_. Defaults to True.

        Raises:
            Exception: _description_
        """
        assert not self.df is None
   
        self.database = database if not database is None else self.database

        logger.info(f"Saving database: {self.database}, replace={replace}, compress={compress}")

        if self.database in self.databases() and not replace:
            raise Exception(f"{self.database} already saved, set replace=True to replace")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")

        if compress:
            self.df.to_pickle(pathfile,'bz2')
        else:
            self.df.to_pickle(pathfile)

    def load(self, database:str=None, compress=True):
        """
        load _summary_

        _extended_summary_

        Args:
            database (_type_, optional): _description_. Defaults to None.
            compress (bool, optional): _description_. Defaults to True.

        Raises:
            Exception: _description_
        """
        self.database = database if not database is None else self.database

        logger.info(f"Loading database: {self.database}, compress={compress}")

        if not self.database in self.databases():
            raise Exception(f"{self.database} not found")
        
        pathfile = join(self.database_dir,f"{self.database}.pkl")
        
        if compress:
            self.df = pd.read_pickle(pathfile,'bz2')    
        else:
            self.df = pd.read_pickle(pathfile)    

        logger.info(f"{self.database} was loaded, with {len(self.df)} rows and {len(self.df.columns)} columns")

    def process_update(self,new_parts:list,system):
        """_summary_

        Args:
            new_parts (list): List of paths for news files
            system (System): Any of inherited System class

        Returns:
            _type_: _description_
        """
        new_system = system()
        new_system.database = f"{self.__name__}_update_raw_{hoje.strftime('%Y_%m_%d')}"
        logger.info(f"Generated {new_system} for update")
        new_system.read(new_parts)
        new_system.save(replace=True, compress=True)
        new_system.normalize()
        # new_system.save(database=f"{self.__name__}_update_{hoje.strftime('%Y_%m_%d')}",replace=True)
        return new_system

    def update(self, new_system,on='id'):
        """_summary_

        Args:
            new_system (_type_): _description_
            on (str, optional): _description_. Defaults to 'id'.
        """
        logger.info(f"updating {str(self)}")

        novas_notificacoes = new_system.df.loc[~new_system.df[on].isin(self.df[on])].set_index(on)
        logger.info(f"novas_notificacoes {len(novas_notificacoes)}")

        possiveis_atualizacoes = new_system.df.loc[new_system.df[on].isin(self.df[on])].drop_duplicates(on,keep='last').set_index(on)
        logger.info(f"possiveis_atualizacoes {len(possiveis_atualizacoes)}")

        self.df = self.df.set_index(on)
        self.df = pd.concat([self.df,novas_notificacoes], axis=0)
        self.df.update(possiveis_atualizacoes)
        self.df = self.df.reset_index()
        # self.normalize()

    def get_multiindex(self, inplace:bool=False):
        """
        get_multiindex _summary_

        _extended_summary_

        Args:
            inplace (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """
        assert not self.df is None

        multiindex = pd.merge(self.df.columns.to_frame(None,'column'),self.schema[['group_name','column']],on='column',how='left')[['group_name','column']]
        multiindex.loc[multiindex['column'].str.contains('hash'),'group_name'] = 'hashes'
        multiindex.loc[multiindex['group_name'].isna(),'group_name'] = 'outros'
        
        return pd.MultiIndex.from_frame(multiindex)

    def converters_function(self):
        """
        converters_function _summary_

        _extended_summary_
        """
        logger.info(f"applying converters_func")

        assert not self.df is None
        for col in self.schema.loc[self.schema['converters'].notna(),'column']:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self.converters[col])

    def normalize_dates(self):
        """
        normalize_dates _summary_

        _extended_summary_

        Args:
            format (str, optional): _description_. Defaults to '%d/%m/%Y'.
            errors (str, optional): _description_. Defaults to 'coerce'.
        """
        assert not self.df is None
        logger.info(f"applying normalize_dates")
        for col in self.schema.loc[(self.schema['dtypes']=='datetime64[ns]'),'column']:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype('str').str[:10]
                self.df[col] = pd.to_datetime(self.df[col],format='%d/%m/%Y',errors='ignore')
                self.df[col] = pd.to_datetime(self.df[col],format='%Y-%m-%d',errors='coerce')
                
                if col == 'data_nascimento':
                    self.df.loc[(self.df[col]<menor_data_nascimento)|(self.df[col]>hoje), col] = pd.NaT
                else:
                    self.df.loc[(self.df[col]<data_comeco_pandemia)|(self.df[col]>hoje), col] = pd.NaT

    def fillna(self):
        """
        fillna _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"applying fillna")
        for col, dtype, fillna in self.schema.loc[self.schema['fillna'].notna(),['column', 'dtypes','fillna']].itertuples(False):
            try:
                # logger.debug(col)
                # logger.debug(fillna)
                self.df.loc[self.df[col].isna(),col] = fillna
                self.df[col] = self.df[col].astype(dtype)
            except:
                self.df[col] = self.df[col].apply(lambda x: normalize_number(x,fill=fillna))                

    def fill_category(self):
        """
        fill_category _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"applying fill_category")
        for col, dtype, ftable, fkey, fillna in self.schema.loc[self.schema['foreign_table_name'].notna() & self.schema['fillna'].notna(),['column', 'dtypes', 'foreign_table_name','foreign_column_name','fillna']].itertuples(False):
            if (col in self.df.columns):
                categorias = self.tables[ftable][fkey].values
                for categoria_errada in [ valor for valor in self.df[col].unique() if not valor in categorias ]:
                    self.df.loc[self.df[col].isna(), col] = fillna
                    self.df.loc[self.df[col]==categoria_errada,col] = fillna
                    self.df[col] = self.df[col].astype(eval(dtype))

    def calculate_idade(self):
        """
        calculate_idade _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"calculate idade")
        self.df.loc[(self.df['data_nascimento'].notnull()) & (self.df['data_notificacao'].notnull()), 'idade'] = (
            self.df.loc[(self.df['data_nascimento'].notnull()) & (self.df['data_notificacao'].notnull())].apply(
                    lambda row: row['data_notificacao'].year - row['data_nascimento'].year - (
                            (row['data_notificacao'].month, row['data_notificacao'].day) <
                            (row['data_nascimento'].month, row['data_nascimento'].day)
                    ), axis=1
            )
        )

    def fix_dtypes(self):
        """
        fix_dtypes _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"fixing dtypes")
        for col, dtype in self.schema.loc[self.schema['dtypes']=='int',['column', 'dtypes']].itertuples(False):
            if (col in self.df.columns):
                self.df[col] = self.df[col].astype(eval(dtype))

    def normalize(self):
        """
        normalize _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"init normalize")

        self.normalize_labels()
        self.fillna()
        self.converters_function()
        self.normalize_dates()
        self.fill_category()
        self.calculate_idade()
        self.hashes()

        logger.info('Everthing is normalized')

    def check_duplicates(self,columns:list=['cpf','cns','hash_mae','hash_nasc'],keep=False) -> pd.DataFrame:
        """
        check_duplicates _summary_

        _extended_summary_

        Args:
            columns (list, optional): _description_. Defaults to ['cpf','cns','hash_mae','hash_nasc'].
            keep (bool, optional): _description_. Defaults to False.

        Returns:
            pd.DataFrame: _description_
        """
        assert not self.df is None
        logger.info(f"checking duplicates")

        columns = [ col for col in columns if col in self.df.columns ]

        self.df['duplicated'] = False

        df = self.df.copy()

        df['duplicated_cols'] = ''
        df['duplicated_ids'] = ''

        for col in columns:
            criterio = df[col].notna() & df.duplicated(col,keep=keep)
            
            df.loc[criterio,'duplicated'] = True
            df.loc[criterio,f"duplicated_cols"] =  df.loc[criterio,'duplicated_cols'].apply( lambda l: utils.strlist(l,col) )
            df.loc[criterio,f"{col}_duplicated_ids"] = df.loc[criterio,['id',col]].groupby(col)['id'].transform(lambda x: ",".join(x.astype(str).values))

        duplicados = df.loc[df['duplicated'],['id','duplicated','duplicated_cols'] + sorted([ col for col in df.columns if 'cns' in col or 'cpf' in col or 'hash' in col ]) ].set_index('id')

        group_duplicados = duplicados[ [ col for col in duplicados.columns if 'ids' in col ]].fillna('0')
        group_duplicados.iloc[:,:-1] += ','
        group_duplicados['ids'] = group_duplicados.sum(1).str.split(',').apply(lambda x: sorted(list(map(int,set(x) - {'0'}))))

        duplicados = pd.merge(duplicados[['duplicated','duplicated_cols']],group_duplicados[['ids']],left_index=True,right_index=True,how='left')

        duplicados['ids_len'] = duplicados['ids'].apply(len)
        duplicados['ids_str'] = duplicados['ids'].apply(lambda x: ",".join(map(str,x)))
        duplicados['duplicated_ncols'] = duplicados['duplicated_cols'].apply(lambda x: len(x.split(',')))

        for col in [ col for col in df.columns if 'duplicated_' in col ]:
            del df[col]

        self.df['duplicated'] == False
        self.df.update(df.loc[df['duplicated'],'duplicated'])

        del df
        
        logger.info(f"{len(duplicados)} notifications can be duplicates or reinfections")
        duplicados.to_pickle(join(self.database_dir,'duplicates.pkl'))

        return duplicados


    def hashes(self):
        """
        hashes _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"generate hashes")

        for col in [ col for col in self.df.columns if 'hash' in col ]:
            del self.df[col]
        
        if 'nome_mae' in self.df.columns:
            self.df.loc[self.df['nome_mae'].notna(),'hash_mae'] = ( self.df.loc[self.df['nome_mae'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['nome_mae'].notna(),'nome_mae'].apply(normalize_hash) )

        if 'data_nascimento' in self.df.columns:
            self.df.loc[self.df['data_nascimento'].notna(),'hash_nasc'] = ( self.df.loc[self.df['data_nascimento'].notna(),'paciente'].apply(normalize_hash) + self.df.loc[self.df['data_nascimento'].notna(),'data_nascimento'].apply(date_hash) )
         

    def replace(self,column:str,inplace:bool=True):
        """
        replace _summary_

        _extended_summary_

        Args:
            column (_type_): _description_
            inplace (bool, optional): _description_. Defaults to True.

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        assert not self.df is None
        
        if column not in self.schema.loc[self.schema['foreign_table_name'].notna(),'column'].values:
            raise Exception(f"{column} hasn't foreign table")
        
        foreign_table_name = self.schema.loc[self.schema['column']==column, 'foreign_table_name'].values[0]
        foreign_column_name = self.schema.loc[self.schema['column']==column, 'foreign_column_name'].values[0]
        
        logger.info(f"Replacing column {column} values in table {foreign_table_name}")

        dict_change = {column: self.tables[foreign_table_name].set_index(foreign_column_name).iloc[:,0].to_dict()}
        
        if inplace:
            self.df.replace(dict_change, inplace=True)
        else:
            return self.df.replace(dict_change, inplace=False)[[column]]

    def normalize_labels(self):
        """
        normalize_labels _summary_

        _extended_summary_
        """
        assert not self.df is None
        logger.info(f"normalize columns labels")
        normalized_labels = self.schema.set_index(self.__name__)['column'].to_dict()
        #normalize_labels.index = normalize_labels.index.apply(trim_overspace)
        self.df = self.df.rename(columns=normalized_labels)
        

    def describe(self):
        """
        describe _summary_

        _extended_summary_
        """
        assert not self.df is None

    def merge(self):
        """
        merge _summary_

        _extended_summary_
        """
        assert not self.df is None
