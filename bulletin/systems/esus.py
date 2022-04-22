# -----------------------------------------------------------------------------------------------------------------------------#
# Classe de tratamento de dados do e-SUS esus
# Todos os direitos reservados ao autor
# -----------------------------------------------------------------------------------------------------------------------------#
import pandas as pd
from os.path import join, isdir
from os import makedirs
from datetime import datetime,date

import logging
logger = logging.getLogger(__name__)

from bulletin import hoje, data_comeco_vacinacao
from bulletin.systems import System
from bulletin.services.connections import get_db_conn
from bulletin.utils.normalize import trim_overspace

# ----------------------------------------------------------------------------------------------------------------------
class eSUS(System):
    def __init__(self,database:str='doses_aplicadas'):
        super().__init__('esus',database)

    def load(self, database:str=None, compress:bool=False):
        super().load(database=database,compress=compress)

    def save(self,database:str=None,replace:bool=False, compress:bool=False):
        super().save(database=database,replace=replace,compress=compress)
    # ---------------------------------------------------------------------------------------------------------------------


    def normalize_dates(self):
        for col in ['data_importacao_rnds', 'data_aplicacao', 'dt_deleted']:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str[:10]
                self.df[col] = pd.to_datetime(self.df[col],format='%d/%m/%Y',errors='ignore')
                self.df[col] = pd.to_datetime(self.df[col],format='%Y-%m-%d',errors='coerce')
                self.df.loc[(self.df[col]<data_comeco_vacinacao)|(self.df[col]>hoje), col] = pd.NaT

        super().normalize_dates()


    def fill_category(self):
        pass

    def calculate_idade(self):
        pass
    
    # Normaliza strings, datas e códigos. Anula valores incorretos.
    def normalize(self):
        self.df.rename(columns={
            'estabelecimento_municipio_codigo':'ibge_atendimento',
            'vacina_dataAplicacao': 'data_aplicacao',
            'vacina_descricao_dose': 'dose',
            'vacina_fabricante_nome': 'fabricante',
            'vacina_categoria_nome': 'categoria',
            'vacina_grupoAtendimento_nome': 'grupo_atendimento'
        }, inplace=True)
        
        super().normalize()

        self.df.loc[self.df['telefone_paciente'].notna(),'telefone_paciente'].apply(lambda value: "".join(filter(lambda x: str(x).isnumeric(), value.split(',')[0] )))        
        
        logger.info(f"Normalize vacina nome")
        self.df['vacina_nome'] = self.df['vacina_nome'].str.upper()
        self.df.loc[self.df['vacina_nome'].str.contains('COVISHIELD') | self.df['vacina_nome'].str.contains('ASTRAZENECA'),'vacina_nome'] = 'ASTRAZENECA'
        self.df.loc[self.df['vacina_nome'].str.contains('JANSSEN'),'vacina_nome'] = 'JANSSEN'
        self.df.loc[self.df['vacina_nome'].str.contains('PFIZER'),'vacina_nome'] = 'PFIZER'
        self.df.loc[self.df['vacina_nome'].str.contains('CORONAVAC'),'vacina_nome'] = 'CORONAVAC'

        logger.info(f"Normalize vacina dose")
        self.df['vacina_numDose'] = self.df['vacina_numDose'].astype(int)
        self.df.loc[self.df['vacina_numDose']==1, 'dose'] = '1ª Dose'
        self.df.loc[self.df['vacina_numDose']==2, 'dose'] = '2ª Dose'
        self.df.loc[self.df['vacina_numDose'].isin([8,9]), 'dose'] = 'Única'
        self.df.loc[self.df['vacina_numDose']==37, 'dose'] = 'Adicional'
        self.df.loc[self.df['vacina_numDose']==38, 'dose'] = 'Reforço'

        self.df.loc[self.df['dose'].isna(),'status'] = 'entered-in-error'
        self.df.loc[self.df['dt_deleted'].notna(),'status'] = 'entered-in-error'

    def download_all(self):
        logger.info(f"Download all")
        parts = []
        with get_db_conn(self.config) as conn:
            query = f'''
                SELECT reltuples::bigint AS estimate
                FROM   pg_class
                WHERE  oid = 'public.imunizacao_covid'::regclass;
            '''
            query = trim_overspace(query)
            logger.info(f"Requesting query: {trim_overspace(query)}")

            estimate_len = pd.read_sql(query, conn) 

            logger.info(f'Estimate size {estimate_len.iloc[0].values[0]}')

            for data_inicio in pd.date_range(date(2021,1,1),date.today(), freq='M').union([pd.to_datetime(date.today())]):
                query = f'''
                    SELECT * FROM imunizacao_covid WHERE TO_CHAR("vacina_dataAplicacao",'MM/YYYY') = '{data_inicio.strftime('%m/%Y')}'
                '''
                query = trim_overspace(query)
                logger.info(f"Requesting query: {trim_overspace(query)}")
                df = pd.read_sql(query, conn)
                logger.info(f"Download finish! Shape: {df.shape}")

                filename = join(self.default_input,f"{data_inicio.strftime('%m_%Y')}.csv")
                parts.append(filename)
                logger.info(f"Saving in {filename}")
                df.to_csv(filename,index=False)

        self.read(parts)
        self.save('doses_aplicadas_raw',compress=True,replace=True)
        self.normalize()
        self.save('doses_aplicadas',compress=True,replace=True)

    def download_update(self,interval:int=1, load_downloaded = False):
        if not load_downloaded:
            logger.info(f"Download updates")
            with get_db_conn(self.config) as conn:
                logger.info(f"connect with db {self.config['database']}")
                query = f'''
                    SELECT * FROM imunizacao_covid
                    WHERE ("vacina_dataAplicacao" >= NOW() - INTERVAL '{interval} DAY') 
                    OR ("data_importacao_rnds" >= NOW() - INTERVAL '{interval} DAY') 
                    OR ("dt_deleted" >= NOW() - INTERVAL '{interval} DAY') 
                '''
                query = trim_overspace(query)
                logger.info(query)
                df = pd.read_sql(query, conn)
                filename = join(self.default_input,f"update_{hoje.strftime('%Y_%m_%d')}.csv")
                df.to_csv(filename,index=False)
                return filename
        else:
            logger.info(f"Load updates")
            filename = join(self.default_input,f"update_{hoje.strftime('%Y_%m_%d')}.csv")
            return filename


    def update(self, parts:list):
        news = super().process_update(parts,eSUS)
        logger.info(news.df['data_aplicacao'])
        super().update(news,'document_id')
        