# from datetime import datetime, timedelta, date
# import gc
import logging
import pandas as pd
from os import makedirs
from pathlib import Path
from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta

from bulletin import root, default_input, default_output
from bulletin.notifica import Notifica
from bulletin.utils.normalize import normalize_number, normalize_labels, normalize_hash, date_hash, normalize_ibge, normalize_text
from bulletin.utils.timer import Timer

# ----------------------------------------------------------------------------------------------------------------------
class CasosConfirmados:

    def __init__(self):
        self.df = None
        self.database = join(root, 'database', 'casos_confirmados.pkl')
        print(f"database: {self.database}")
    
    def __len__(self):
        return len(self.df)

    def __str__(self):
        return self.database

    @Timer('reading from xlsx')
    def read_excel(self, arquivo: str = join(default_input,'Casos confirmados PR.xlsx'), append: str = False):
        if not isfile(arquivo):
            raise Exception(f"Arquivo {arquivo} não encontrado")

        print(f"arquivo: {arquivo}")

        tmp_df = pd.read_excel(arquivo,'Casos confirmados')
        
        tmp_df.columns = [normalize_labels(x) for x in tmp_df.columns]
        tmp_df = tmp_df.loc[tmp_df['excluir'] != 'SIM']
        
        tmp_df = tmp_df.rename(columns={'ibge_res_pr':'ibge7'})
        
        tmp_df['ibge7'] = tmp_df['ibge7'].apply(lambda x: normalize_number(x, fill='9999999'))
        tmp_df['rs'] = tmp_df['rs'].apply(lambda x: normalize_number(x, fill='99'))
        
        tmp_df['hash'] = (tmp_df['nome'].apply(normalize_hash) +
                          tmp_df['idade'].astype(str) +
                          tmp_df['mun_resid'].apply(normalize_hash))

        tmp_df['hash_less'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x - 1)) +
                                tmp_df['mun_resid'].apply(normalize_hash))

        tmp_df['hash_more'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x + 1)) +
                                tmp_df['mun_resid'].apply(normalize_hash))

        tmp_df['hash_atend'] = (tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].astype(str) +
                                tmp_df['mun_atend'].apply(normalize_hash))

        tmp_df['hash_less_atend'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x - 1)) +
                                tmp_df['mun_atend'].apply(normalize_hash))

        tmp_df['hash_more_atend'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x + 1)) +
                                tmp_df['mun_atend'].apply(normalize_hash))


        tmp_df['hash_diag'] = (tmp_df['nome'].apply(normalize_hash) + tmp_df['dt_diag'].apply(date_hash))

        tmp_df.loc[tmp_df['data_obito'].notna(), 'hash_obito'] = tmp_df.loc[tmp_df['data_obito'].notna()].apply(
                lambda row: normalize_hash(row['nome']) + date_hash(row['data_obito']), axis=1
        )

        if isinstance(self.df, pd.DataFrame) and append:
            self.df = self.df.append(tmp_df)
        else:
            self.df = tmp_df
            
    @Timer('saving Casos Confirmados to pkl')
    def save(self,df=None):
        if isinstance(df,pd.DataFrame):
            df.to_pickle(self.database)
        else:
            self.df.to_pickle(self.database)

    @Timer('loading Casos Confirmados from pkl')
    def load(self):
        if not isfile(self.database):
            raise Exception(f"{self.database} not found, can you use read_excel for generate new database or import from")
        
        self.df = pd.read_pickle(self.database)

    def export(self, output_file):
        pass

    def get_obitos(self):
        return self.df.loc[self.df['evolucao'] == 2].copy()

    def get_casos(self):
        return self.df.copy()

    def novos_casos(self, casos_notifica):
        casos = self.df
        return casos_notifica.loc[~(
                (casos_notifica['hash_resid'].isin(casos['hash'])) |
                (casos_notifica['hash_resid'].isin(casos['hash_less'])) |
                (casos_notifica['hash_resid'].isin(casos['hash_more'])) |
                (casos_notifica['hash_diag'].isin(casos['hash_diag']))
        )]

    def novos_obitos(self, obitos_notifica):
        obitos = self.df.loc[self.df['obito'] == 'SIM']
        return obitos_notifica.loc[~(
                (obitos_notifica['hash_resid'].isin(obitos['hash'])) |
                (obitos_notifica['hash_resid'].isin(obitos['hash_less'])) |
                (obitos_notifica['hash_resid'].isin(obitos['hash_more'])) |
                (obitos_notifica['hash_diag'].isin(obitos['hash_diag'])) |
                (obitos_notifica['hash_obito'].isin(obitos['hash_obito']))
        )]
    
    
