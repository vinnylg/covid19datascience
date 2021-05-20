from bulletin.utils.normalize import trim_overspace
import requests
import pandas as pd
import json
import io
import urllib3
import cgi
from clint.textui import progress
from os import makedirs
from os.path import join, dirname, isdir
from bulletin import root, default_input, default_output
from retry import retry

urllib3.disable_warnings()

class Metabase:
    def __init__(self):
        self.cookie = input('Paste cookie and press enter')

    @retry(Exception, delay=10, tries=-1)
    def download_metabase(self,filename='diario.csv',sqlfile='diario.sql',sql=None):
        output_queries = join(default_input,'queries')

        if not isdir(output_queries):
            makedirs(output_queries)

        print(f"{self.cookie[:3]}...{self.cookie[:-3]}")

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

        with open(join(dirname(__root__),'resources','notifica','sql',sqlfile)) as file:
            sql = " ".join([ row.replace('\n',' ') for row in file.readlines() if not '--' in row ])
            sql = trim_overspace(f"{sql} WHERE {where} ORDER BY id ASC LIMIT {limit} OFFSET {offset}")

        print(f"Requesting {where}")

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

        pathfile = join(inputdir,filename)

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