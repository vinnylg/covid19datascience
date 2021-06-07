import requests
import pandas as pd
import json
import io
import os
import urllib3
import cgi
from clint.textui import progress
from os import makedirs, listdir
from os.path import join, dirname, isdir, isfile
from bulletin import root, default_input, default_output
from retry import retry
import re
import glob
from pathlib import Path 
import shutil
from datetime import datetime

from bulletin.utils.normalize import trim_overspace
from bulletin.utils.timer import Timer

urllib3.disable_warnings()

class Metabase:

    def __init__(self, limit:int=2**18):
        self.limit = limit    
        print(f"limit: {limit}")
        
        if not isfile(join(default_input,'cookie_file')):
            print('login_session')
            raise Exception('not_implemented') #!TODO
            
        with open(join(default_input,'cookie_file'),'r') as cookie_file:
            self.cookie = cookie_file.read()

            
        print(f"Cookie:'{ self.cookie[:5]}{''.join(['-' if x=='-' else '*' for x in  self.cookie])}{ self.cookie[-5:]}'")
        
        self.output = join(default_input,'queries')
        self.tmp_queries = join(self.output,'tmp')
        self.sql_path = join(root,'resources','sql')
        self.tables_path = join(root,'resources','tables')
        
        if not isdir(self.output):
            makedirs(self.output)
            
        if not isdir(self.tmp_queries):
            makedirs(self.tmp_queries)
        
        if not isdir(self.sql_path):
            makedirs(self.sql_path)
                
        if not isdir(self.tables_path):
            makedirs(self.tables_path)
    
        self.list_sql_files()
        self.list_query_results()
    
    def list_sql_files(self):
        self.sql_files = [ Path(path).stem for path in glob.glob(join(self.sql_path,"*.sql"))]
        print("\nsql_files:")
        for i in range(len(self.sql_files)): print(f"\t{i}: {self.sql_files[i]}")
    
    def list_query_results(self):
        self.query_resuls = [ Path(path).stem for path in glob.glob(join(self.output,"*.csv"))]
        print("\nsql_results:")
        for i in range(len(self.query_resuls)): print(f"\t{i}: {self.query_resuls[i]}")
    
    def information_schema_columns(self):
        columns_sql = ''' select table_name, column_name, udt_name from information_schema.columns order by table_name '''
        columns = pd.read_csv(self.download(columns_sql, join(self.tables_path,'columns.csv')))
        for x, y in zip(['int', 'date','time','char','text','float'],['int','datetime','datetime','str','str','float']):
            columns.loc[columns['udt_name'].str.contains(x),'udt_name'] = y
        
        return columns
    
    def constraint(self):
        constraint_sql = ''' SELECT tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' '''

        return pd.read_csv(self.download(constraint_sql,join(self.tables_path,'constraint.csv')))
    
    def notificacao_information_schema(self):
        if not isfile(join(root,'resources','tables','notificacao_schema.csv')):
            notificacao_sql = ''' select table_name, column_name, udt_name from information_schema.columns where table_name = 'notificacao' order by table_name '''

            notificacao = pd.read_csv(self.download(notificacao_sql, join(self.tables_path,'notificacao.csv')))
            for x, y in zip(['int', 'date','time','char','text','float'],['int','datetime','datetime','str','str','float']):
                notificacao.loc[notificacao['udt_name'].str.contains(x),'udt_name'] = y

            return pd.merge(notificacao,self.constraint(),on=['table_name','column_name'],how='left').fillna('')
        else:
            notificacao = pd.read_csv(join(root,'resources','tables','notificacao_schema.csv'))
            return notificacao.loc[notificacao['usecols']==1]
        

    @Timer('Download tables')
    def download_tables(self):
        notificacao = self.notificacao_information_schema()
        columns = self.information_schema_columns()
        tables = [ x for x in notificacao['foreign_table_name'].unique() if (str(x).lower() not in ['nan','none']) and (len(x) > 0) ] 
        for table in tables:
            if table in columns['table_name'].unique():
                self.download(f'SELECT * FROM public.{table}', join(self.tables_path,f'{table}.csv'))
        
    
    def generate_notifica_query(self, usecols=True):
        if isfile(join(root,'resources','tables','notificacao_schema.csv')):
            notificacao_schema = pd.read_csv(join(root,'resources','tables','notificacao_schema.csv'))
            if usecols:
                notificacao_schema = notificacao_schema.loc[notificacao_schema['usecols']==1]
            
            print(f"Select {len(notificacao_schema)} columns")
            sql = 'SELECT '
            for idx, row in notificacao_schema.iterrows():
                if row['dtypes'] == 'datetime':
                    sql += f"to_char({row['column']},'DD/MM/YYYY') AS {row['column']}"
                else:
                    sql += f"{row['column']}"
                    
                if idx != notificacao_schema.index[-1]:
                    sql += ', '
                    
            sql += ' FROM notificacao WHERE true ORDER BY 1 LIMIT ALL OFFSET 0'
            return sql
        
        return None
        

        
    @Timer('Load downloaded query')
    def read_query(self, query_name):
        if query_name not in self.saved_csv:
            raise Exception(f"Query {query_name}.csv not found in {self.output}")
            
        return join(self.output,f"{filename}.csv")

    
    def get_downloaded_part(self,pathfile):
        pathfile = "_".join(pathfile.split('_')[:-1]) + '*.csv'
        pathfile = glob.glob(pathfile)[-1]
        return pathfile
    
    @Timer('Download query')
    def download_query(self, query_name='diario', dtype=None, converters=None, load=False):
        print(f"download_query({query_name})")
        if query_name not in self.sql_files:
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
            query_size = pd.read_csv(self.download(f"select count(*) from {sql['from']} where {sql['where']}", join(self.output,f"{query_name}_len_{datetime.today().strftime('%Y%m%d%H%M')}.csv"))).iloc[0,0]
        else:
            query_size = pd.read_csv(self.get_downloaded_part(join(self.output,f"{query_name}_len_{datetime.today().strftime('%Y%m%d%H%M')}.csv"))).iloc[0,0]
        
        print(f"\nquery_size: {query_size}\n")
                
        parts = []
        for part, offset in enumerate(range(0,query_size,self.limit)):
            part_query = f"select {sql['select']} from {sql['from']} where {sql['where']} order by 1 limit {self.limit} offset {offset}"
            part_filename = join(self.tmp_queries,f"{query_name}_{offset}_{offset+self.limit}_{datetime.today().strftime('%Y%m%d%H%M')}.csv")
            
            if not load:
                print(f"select ... limit {self.limit} offset {offset}")
                parts.append(self.download(part_query,part_filename))
            else:
                parts.append(self.get_downloaded_part(part_filename))

        output_path = join(self.output,f"{query_name}.csv")
        result_query = pd.DataFrame()
        for part in parts: 
            print(f"Appending {part}")
            result_query = result_query.append(pd.read_csv(part,dtype=dtype,converters=converters))
            
        print(f"saving all in {output_path}")
        result_query.to_csv(output_path,index=False)      

        return output_path     
                                
                                                                                             
    @retry(Exception, delay=10, tries=5)
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

        res = requests.post("https://metabase.saude.pr.gov.br/api/dataset/csv",
                                headers = header,
                                data = {'query': query},
                                verify=False,
                                stream=True
                            )

        if res.status_code in [ 200, 202 ] :
            print(f"Success code {res.status_code }")
        else:
            print(f"Error code {res.status_code }")
            raise Exception()
            
            
        print(f'Saving query in {pathfile}')
        with open(pathfile,'wb') as out:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    out.write(chunk)
                    out.flush()
        
        try:
            print(f"Download finish, time elapsed: {res.elapsed}\n")
        except:
            raise Exception(f"download error")
            
        return pathfile