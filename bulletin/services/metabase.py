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

from bulletin.utils.normalize import trim_overspace
from bulletin.utils.timer import Timer

urllib3.disable_warnings()

class Metabase:

    def __init__(self, limit:int=2**18):
        self.limit = limit

        self.cookie = self.request_cookie()
        self.output = join(default_input,'queries')
        self.sql_dir = join(root,'resources','sql')
        self.tables = join(root,'resources','tables')
        
        if not isdir(self.output):
            makedirs(self.output)
        
        if not isdir(self.sql_dir):
            makedirs(self.sql_dir)
                
        if not isdir(self.tables):
            makedirs(self.tables)
        
        self.saved_queries = [ Path(path).stem for path in glob.glob(join(self.sql_dir,"*.sql"))]
        
        print("\nsaved_queries:",self.saved_queries,'\n')
    
    
    def request_cookie(self):
        my_cookie = ''
        if not isfile(join(default_input,'cookie_file')):
            print('login_session')
            raise Exception('not_implemented')
            
        with open(join(default_input,'cookie_file'),'r') as cookie_file:
            my_cookie = cookie_file.read()
        
        print(f"Cookie:'{my_cookie[:5]}{''.join(['-' if x=='-' else '*' for x in my_cookie])}{my_cookie[-5:]}'")
        return my_cookie#input('enter with cookie')                                                                 
                                                             
    
    @Timer('Download tables')
    def download_tables(self, update=False):
        
        notificacao_sql = '''
            select 
                table_name, column_name, udt_name 
            from 
                information_schema.columns 
            where 
                table_name = 'notificacao' 
            order by 
                table_name
        '''

        columns_sql = '''
            select 
                table_name, column_name, udt_name 
            from 
                information_schema.columns 
            order by 
                table_name
        '''
        
        constraint_sql = '''
            SELECT
                tc.constraint_name, tc.table_name, kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY'
        '''
        
        notificacao = pd.read_csv(self.download(notificacao_sql, join(self.tables,'notificacao.csv')))
        notificacao.loc[notificacao['udt_name'].str.contains('int'),'udt_name'] = 'int'
        notificacao.loc[notificacao['udt_name'].str.contains('date'),'udt_name'] = 'datetime'
        notificacao.loc[notificacao['udt_name'].str.contains('time'),'udt_name'] = 'datetime'
        notificacao.loc[notificacao['udt_name'].str.contains('char'),'udt_name'] = 'str'
        notificacao.loc[notificacao['udt_name'].str.contains('text'),'udt_name'] = 'str'
        notificacao.loc[notificacao['udt_name'].str.contains('float'),'udt_name'] = 'float'
        
        columns = pd.read_csv(self.download(columns_sql, join(self.tables,'columns.csv')))
        columns.loc[columns['udt_name'].str.contains('int'),'udt_name'] = 'int'
        columns.loc[columns['udt_name'].str.contains('date'),'udt_name'] = 'datetime'
        columns.loc[columns['udt_name'].str.contains('time'),'udt_name'] = 'datetime'
        columns.loc[columns['udt_name'].str.contains('char'),'udt_name'] = 'str'
        columns.loc[columns['udt_name'].str.contains('text'),'udt_name'] = 'str'
        columns.loc[columns['udt_name'].str.contains('float'),'udt_name'] = 'float'
        
#         notificacao = columns.loc[columns['table_name']=='notificacao']
#         columns = columns.loc[columns['table_name']!='notificacao']
        
        constraint = pd.read_csv(self.download(constraint_sql,join(self.tables,'constraint.csv')))
    
        notificacao = pd.merge(notificacao,constraint,on=['table_name','column_name'],how='left').fillna('')
        
        return notificacao, columns, constraint
            
    @Timer('Download query')
    def download_query(self, query_name='diario', remove_parts=True):
        if query_name not in self.saved_queries:
            raise Exception(f"Query {query_name}.sql not found in {self.sql_dir}")

        with open(join(self.sql_dir,f"{query_name}.sql")) as file:
            raw_sql = trim_overspace(" ".join([ row.replace('\n',' ') for row in file.readlines() if not '--' in row ]))

        sql = {}
        sql['select'] = re.search('SELECT (.*) FROM', raw_sql).group(1)
        sql['from'] = re.search('FROM (.*) WHERE', raw_sql).group(1)
        sql['where'] = re.search('WHERE (.*) ORDER', raw_sql).group(1)
        sql['order'] = re.search('ORDER (.*) LIMIT', raw_sql).group(1)
        sql['limit'] = re.search('LIMIT (.*) OFFSET', raw_sql).group(1)
        sql['offset'] = re.search('OFFSET (.*)', raw_sql).group(1)
        
        query_size = pd.read_csv(self.download(f"select count(*) from {sql['from']} where {sql['where']}", join(self.output,f"{query_name}_len.csv"),False)).iloc[0,0]
        print(f"\nquery_size: {query_size}\n")
                
        parts = []
        for offset in range(0,query_size,self.limit):
            print(f"select ... limit {self.limit} offset {offset}")
        
            parts.append(
                self.download(
                    f"select {sql['select']} from {sql['from']} where {sql['where']} order by 1 limit {self.limit} offset {offset}",join(self.output,f"{query_name}_{offset}.csv")
                )
            )

        output_path = join(self.output,f"{query_name}.csv")
    
        if len(parts) > 1: 
            output_query = pd.concat([ pd.read_csv(part) for part in parts ])
            print(f"saving all in {output_path}")
            output_query.to_csv(output_path,index=False)
            if remove_parts:
                print(f"removing {' '.join(parts)}")
                for part in parts: os.remove(part)         
        else:
            os.rename(parts[0],output_path)
        
        return output_path     
                                
                                                                                             
    @retry(Exception, delay=10, tries=-1)
    def download(self,sql,pathfile, log=True):
        
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
            if log: print(f"Success code {res.status_code }")
        else:
            if log: print(f"Error code {res.status_code }")
            raise Exception()

        with open(pathfile,'wb') as out:
#             if log: print(f'Saving in {pathfile}')
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    out.write(chunk)
                    out.flush()

        try:
            if log: print(f"Download finish, time elapsed: {res.elapsed}")
            if log: print(f"downloaded shape {pd.read_csv(pathfile,low_memory=False).shape}\n")
        except:
            raise Exception(f"download error")


        return pathfile