import pandas as pd
from os.path import join

import logging
logger = logging.getLogger(__name__)

from bulletin import hoje
from bulletin.systems import System
from bulletin.services import Metabase
from bulletin.utils.functions import mean_distance_of_dates


class Notifica(System):
    def __init__(self,database:str='notifica'):
        super().__init__('notifica',database)
        self.mb = None
        self.casos_confirmados_mask_analiseData = lambda : (self.df['classificacao_final'].isin([1,2,3])) & (self.df['excluir_ficha'].isin([1,2])) & (self.df['status_notificacao'].isin([1,2,3]))
        self.casos_confirmados_mask_duplicated = lambda : (self.df['classificacao_final']==2) & (self.df['excluir_ficha']==2) & (self.df['status_notificacao'].isin([1,2,3]))


    def load(self, database:str=None, compress:bool=False):
        super().load(database=database,compress=compress)

    def save(self,database:str=None,replace:bool=False, compress:bool=False):
        super().save(database=database,replace=replace,compress=compress)

    def normalize(self):
        self.df.loc[self.df['evolucao']==5,'evolucao'] = 2
        super().normalize()

    def connect_metabase(self):
        self.mb = Metabase(self.config)

    def download_all(self, load_downloaded:bool=False):
        logger.info(f"Download all")
        if self.mb is None: 
            self.connect_metabase()

        mb = self.mb
        mb.generate_notifica_query('notifica', where='True', replace=True)
        parts = mb.download_notificacao('notifica', load=load_downloaded)
        self.read(parts)
        self.save('notifica_raw',compress=True,replace=True)
        self.normalize()
        self.save('notifica',compress=True,replace=True)

    def download_update(self,days:int=1,load:bool=False):
        logger.info(f"Download updates")
        if self.mb is None: 
            self.connect_metabase()
            
        intervalo = f"(data_notificacao >= NOW() - INTERVAL '{days} DAY') or (data_liberacao >= NOW() - INTERVAL '{days} DAY') or (updated_at >= NOW() - INTERVAL '{days} DAY') or (data_coleta >= NOW() - INTERVAL '{days} DAY') or (data_encerramento >= NOW() - INTERVAL '{days} DAY') or (data_cura_obito >= NOW() - INTERVAL '{days} DAY')"
        self.mb.generate_notifica_query('update_notifica', where=intervalo, replace=True)
        parts = self.mb.download_notificacao('update_notifica', load=load)
        return parts


    def update(self, parts):
        news = super().process_update(parts,Notifica)
        news.analise_data_diagnostico()
        super().update(news)


    def analise_data_diagnostico(self,datas=['data_notificacao','data_1o_sintomas','data_cadastro','data_coleta','data_recebimento','data_liberacao']):
        logger.info(f"Initialize analise_data_diagnostico. Keep calm this can take a long time")
        
        self.df.set_index('id',inplace=True)
        
        try:
            del self.df['data_diagnostico']
        except:
            pass

        try:
            del self.df['data_diagnostico_label']
        except:
            pass


        #DATAS PARA TIRAR A DATA MÉDIA E ENCONTRAR DATA MENOS DISTANTE DAS DEMAIS
        analise_data_diagnostico = self.df.loc[self.casos_confirmados_mask_analiseData(),datas].apply(
            mean_distance_of_dates,
            axis = 1
        )

        self.df.insert(len(self.df.columns),'data_diagnostico',pd.NaT)
        self.df.update(analise_data_diagnostico[['data_min_distance']].rename(columns={'data_min_distance':'data_diagnostico'}))

        self.df.insert(len(self.df.columns),'data_diagnostico_label',None)
        self.df.update(analise_data_diagnostico[['idx_min_distance']].rename(columns={'idx_min_distance':'data_diagnostico_label'}))
        
        self.df.reset_index(inplace=True)


        #SURTO/2
        columns = ['data_cadastro','data_coleta','data_recebimento','data_liberacao']
        days_diff = 3

        for year in [2020, 2021]:
            
            col = 'data_1o_sintomas'
            self.df['diff_days'] = (abs(self.df['data_diagnostico'] - self.df[col]).dt.days).fillna(0).astype(int)

            #ALTERAÇÃO
            self.df.loc[(self.df['data_diagnostico'].dt.year==year) 
                    & (self.df['data_diagnostico'] < self.df[col]),'data_diagnostico'] = \
            self.df.loc[(self.df['data_diagnostico'].dt.year==year) 
                    & (self.df['data_diagnostico'] < self.df[col]), col]
            
            for col in columns:

                self.df['diff_days'] = (abs(self.df['data_diagnostico'] - self.df[col]).dt.days).fillna(0).astype(int)

                #ALTERAÇÃO
                self.df.loc[(self.df['data_diagnostico'].dt.year==year) 
                        & (self.df['data_diagnostico'] < self.df[col]) 
                        & (self.df['diff_days'] >= days_diff),'data_diagnostico'] = \
                self.df.loc[(self.df['data_diagnostico'].dt.year==year) 
                        & (self.df['data_diagnostico'] < self.df[col]) 
                        & (self.df['diff_days'] >= days_diff), col]
        self.df = self.df.drop(columns=['diff_days'])


        logger.info(f"Finish analise_data_diagnostico.")
        analise_data_diagnostico.to_pickle(join(self.database_dir,'analise_data_diagnostico.pkl'))
        return analise_data_diagnostico

