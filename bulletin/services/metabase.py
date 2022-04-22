import requests
import pandas as pd
import json
import urllib3
from os import makedirs
from os.path import isdir, isfile, join
from bulletin import default_input, root
from retry import retry
import re
import glob
from pathlib import Path 

import logging
logger = logging.getLogger(__name__)

from bulletin.utils.normalize import trim_overspace

urllib3.disable_warnings()

class Metabase:

    def __init__(self,config,limit:int=10**5):
        self.limit = limit   
        self.not_att_tables = ['sexo']

        self.config = config

        self.session()

        logger.info(f"Cookie:'{ self.cookie[:5]}{''.join(['-' if x=='-' else '*' for x in  self.cookie])}{ self.cookie[-5:]}'")
        
        self.output = join(default_input,'notifica','metabase')
        self.sql_path = join(root,'resources','sql')
        self.tables_path = join(root,'resources','tables')
        
        if not isdir(self.output):
            makedirs(self.output)
        
        if not isdir(self.sql_path):
            makedirs(self.sql_path)
                
        if not isdir(self.tables_path):
            makedirs(self.tables_path)
    
        self.sql_files = lambda: [ Path(path).stem for path in glob.glob(join(self.sql_path,"*.sql"))]
    

    def information_schema_columns(self):
        columns_sql = ''' select table_name, column_name, udt_name from information_schema.columns order by table_name '''
        columns = pd.read_csv(self.download(columns_sql, join(self.tables_path,'columns.csv')))
        for x, y in zip(['int', 'date','time','char','text','float'],['int','datetime','datetime','str','str','float']):
            columns.loc[columns['udt_name'].str.contains(x),'udt_name'] = y
        return columns
    
    def constraint(self):
        constraint_sql = ''' 
            SELECT 
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name 
                JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name 
            WHERE constraint_type = 'FOREIGN KEY' 
        '''
        return pd.read_csv(self.download(constraint_sql,join(self.tables_path,'constraint.csv')))
    
    def notificacao_information_schema(self, replace=False):
        if replace:
            notificacao_sql = ''' select table_name, column_name, udt_name from information_schema.columns where table_name = 'notificacao' order by table_name '''

            notificacao = pd.read_csv(self.download(notificacao_sql, join(self.tables_path,'notificacao.csv')))
            for x, y in zip(['int', 'date','time','char','text','float'],['int','datetime','datetime','str','str','float']):
                notificacao.loc[notificacao['udt_name'].str.contains(x),'udt_name'] = y

            notificacao = pd.merge(notificacao,self.constraint(),on=['table_name','column_name'],how='left').fillna('')
            notificacao = notificacao.rename(columns={'udt_name':'dtypes','column_name':'column'})

            notificacao.to_csv(join(root,'resources','tables','notificacao_schema.csv'), index=False)
            return notificacao

        elif isfile(join(root,'systems','information_schema.csv')):
            notificacao = pd.read_csv(join(root,'systems','information_schema.csv'))
            return notificacao

        else:
            return self.notificacao_information_schema(True)

    def update_notificacao_schema(self,schema):
        schema.to_csv(join(root,'systems','information_schema.csv'))
        

    def download_tables(self):
        notificacao = self.notificacao_information_schema()
        notificacao = notificacao.loc[notificacao['notifica']==1]

        columns = self.information_schema_columns()
        tables = [ x for x in notificacao['foreign_table_name'].unique() if (str(x).lower() not in ['nan','none']) and (len(x) > 0) ] 
        for table in tables:
            if table not in self.not_att_tables:
                if table in columns['table_name'].unique():
                    self.download(f'SELECT * FROM public.{table}', join(self.tables_path,f'{table}.csv'))
        
    
    def generate_notifica_query(self,name,where='TRUE',replace=False):
        try:
            notificacao_schema = pd.read_csv(join(root,'systems','information_schema.csv'))
            notificacao_schema = notificacao_schema.loc[notificacao_schema['notifica'].notna()]
            logger.info(f"Select {len(notificacao_schema)} columns")
            sql = 'SELECT '
            for idx, row in notificacao_schema.iterrows():
                if row['dtypes'] == 'datetime64[ns]':
                    sql += f"to_char({row['column']},'DD/MM/YYYY') AS {row['column']}"
                else:
                    sql += f"{row['column']}"
                    
                if idx != notificacao_schema.index[-1]:
                    sql += ', '
                    
            sql += f" FROM notificacao WHERE {where} ORDER BY 1 LIMIT ALL OFFSET 0"
            
            pathfile = join(self.sql_path,f"{name}.sql")
            if (not isfile(pathfile)) or replace:  
                with open(pathfile,'w') as out:
                    out.write(sql)
            
            return sql
            
        except:
            raise Exception(f"{join(root,'systems','information_schema.csv')} not found")

        
    def read_query(self, query_name):
        if query_name not in self.saved_csv:
            raise Exception(f"Query {query_name}.csv not found in {self.output}")
            
        return join(self.output,f"{query_name}.csv")

    
    def get_downloaded_part(self,pathfile):
        pathfile = glob.glob(pathfile)[-1]
        return pathfile
    
    def download_notificacao(self, query_name='diario', load=False, normalize=False):
        logger.info(f"Download {query_name}")
        
        if query_name not in self.sql_files():
            raise Exception(f"Query {query_name}.sql not found in {self.sql_path}")

        with open(join(self.sql_path,f"{query_name}.sql")) as file:
            raw_sql = trim_overspace(" ".join([ row.replace('\n',' ') for row in file.readlines() if not '--' in row ]))

        sql = {}
        sql['select'] = re.search('SELECT (.*) FROM', raw_sql).group(1)
        sql['from'] = re.search('FROM (.*) WHERE', raw_sql).group(1)
        sql['where'] = re.search('WHERE (.*) ORDER', raw_sql).group(1)
        sql['order'] = re.search('ORDER (.*) LIMIT', raw_sql).group(1)
        sql['limit'] = re.search('LIMIT (.*) OFFSET', raw_sql).group(1)
        sql['offset'] = re.search('OFFSET (.*)', raw_sql).group(1)
        
        if not load:
            query_size = pd.read_csv(self.download(f"select count(*) from {sql['from']} where {sql['where']}", join(self.output,f"{query_name}_len.csv"))).iloc[0,0]
        else:
            query_size = pd.read_csv(self.get_downloaded_part(join(self.output,f"{query_name}_len.csv"))).iloc[0,0]
        
        logger.info(f"query_size: {query_size}")
                
        parts = []
        for part, offset in enumerate(range(0,query_size,self.limit)):
            part_query = f"select {sql['select']} from {sql['from']} where {sql['where']} order by 1 limit {self.limit} offset {offset}"
            part_filename = join(self.output,f"{query_name}_{offset}_{offset+self.limit}.csv")
            
            if load and isfile(part_filename):
                logger.info(f"found downloaded query <- {part_filename}")
                parts.append(part_filename)
            else:
                logger.info(f"request download query -> {part_filename}")
                parts.append(self.download(part_query,part_filename))

        # logger.info(f"{''.join(['-' for x in range(100)])}")
        return parts
    
    @retry(Exception, delay=10,tries=5)
    def session(self,config:dict=None):
        config = self.config if config is None else config
        header = {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "sec-ch-ua": "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\"",
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "referrer": "https://metabase.saude.pr.gov.br/auth/login?redirect=%2F",
            "referrerPolicy": "strict-origin-when-cross-origin",
            "mode": "cors",
            "credentials": "include"
        }

        data = config
        data['remember'] = True
        data = json.dumps(data)
        logger.info(f"Requesting session")
                
        res = requests.post("https://metabase.saude.pr.gov.br/api/session",
                                headers = header,
                                data = data,
                                verify=False
                            )

        if res.status_code in [ 200, 202 ] :
            logger.info(f"Success code {res.status_code }")
        else:
            logger.info(f"Error code {res.status_code }")
            raise Exception()

        cookie = json.loads(res.text)['id']

        self.cookie = cookie    
    
    @retry(Exception, max_delay=60, tries=-1)
    def download(self,sql,pathfile):
        
        header = {
            'Host':'metabase.saude.pr.gov.br',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0',
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language':'pt-BR',
            'Accept-Encoding':'gzip, deflate, br',
            'Content-Type':'application/x-www-form-urlencoded',
            'Origin':'https://metabase.saude.pr.gov.br',
            'Connection':'keep-alive',
            'Referer':'https://metabase.saude.pr.gov.br/question',
            'Cookie': f"metabase.SESSION={self.cookie}",
            'Upgrade-Insecure-Requests':'1'
        }
        

        query = {
            "database": 2,
            "type": "native",
            "native": {
                "query": sql
            }
        }

        query = json.dumps(query)
        logger.info(f"POST https://metabase.saude.pr.gov.br/api/dataset/csv")
        logger.debug(query)
        
        res = requests.post("https://metabase.saude.pr.gov.br/api/dataset/csv",
                                headers = header,
                                data = {'query': query},
                                verify=False,
                                stream=True,
                                timeout=600
                            )
        
        if res.status_code in [ 200, 202 ] :
            logger.info(f"success code {res.status_code }")
        else:
            logger.info(f"error code {res.status_code }")
            if res.status_code == 401:
                self.session()
            raise Exception()
            
        with open(pathfile,'wb') as out:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    out.write(chunk)
                    out.flush()
        
        try:
            logger.info(f"Download finish!\tColumns size: {len(pd.read_csv(pathfile,nrows=1).columns)}")
        except:
            raise Exception(f"download error")
            
        return pathfile
