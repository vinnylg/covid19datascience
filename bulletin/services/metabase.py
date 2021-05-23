from bulletin.utils.normalize import trim_overspace
import requests
import pandas as pd
import json
import io
import urllib3
import cgi
from clint.textui import progress
from os import makedirs, listdir
from os.path import join, dirname, isdir
from bulletin import root, default_input, default_output
from retry import retry
import re
import glob
from pathlib import Path    

urllib3.disable_warnings()

class Metabase:
    def __init__(self, limit:int=2**18):
        self.limit = limit

        self.cookie = self.__request_cookie()
        self.output = join(default_input,'queries')
        self.sql_dir = join(dirname(root),'bulletin','resources','notifica')
        
        if not isdir(self.output):
            makedirs(self.output)
        
        if not isdir(self.sql_dir):
            makedirs(self.sql_dir)
            
        self.saved_queries = [ Path(path).stem for path in glob.glob(join(self.sql_dir,"*.sql"))]
        
        print("\nsaved_queries:",self.saved_queries)
            
        
    def download_query(self, query_name='diario'):
        if query_name not in self.saved_queries:
            raise Exception(f"Query {query_name}.sql not found in {self.sql_dir}")

        with open(join(self.sql_dir,f"{query_name}.sql")) as file:
            raw_sql = trim_overspace(" ".join([ row.replace('\n',' ') for row in file.readlines() if not '--' in row ])).lower()

        sql = {}
        sql['select'] = re.search('select (.*) from', raw_sql).group(1)
        sql['from'] = re.search('from (.*) where', raw_sql).group(1)
        sql['where'] = re.search('where (.*) order', raw_sql).group(1)
        sql['order'] = re.search('order (.*) limit', raw_sql).group(1)
        sql['limit'] = re.search('limit (.*) offset', raw_sql).group(1)
        sql['offset'] = re.search('offset (.*)', raw_sql).group(1)
        
        query_size = pd.read_csv(self.__download(f"select count(*) from {sql['from']} where {sql['where']}", f"{query_name}_len.csv")).iloc[0,0]
        print(f"\nquery_size: {query_size}\n")
        queries = []
        for offset in range(0,query_size,self.limit):
            print(f"select ... limit {self.limit} offset {offset}")
            queries.append(self.__download(f"select {sql['select']} from {sql['from']} where {sql['where']} order by 1 limit {self.limit} offset {offset}",f"{query_name}_{offset}.csv"))

        print(queries)
        
        pathfile = join(self.output,f"{query_name}.csv")
        
        res = pd.concat([ pd.read_csv(pathfile,low_memory=False) for pathfile in queries ])
        res.to_csv(pathfile,index=False)
        print(pathfile)               
            
        return pathfile     
                                                                                             
    def __request_cookie(self):
        return input('enter with cookie')                                                                     
                                                                                             
                                                                                             
    @retry(Exception, delay=10, tries=-1)
    def __download(self,sql,filename):
        
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
            'Cookie': self.cookie,
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

        pathfile = join(self.output,filename)

        with open(pathfile,'wb') as out:
            print(f'Saving in {pathfile}')
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    out.write(chunk)
                    out.flush()

        try:
            print(f"Download finish, time elapsed: {res.elapsed}")
            print(f"downloaded shape {pd.read_csv(pathfile,low_memory=False).shape}\n")
        except:
            print(f"download error")
            raise Exception()


        return pathfile