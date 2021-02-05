from bulletin.commom.normalize import trim_overspace
import requests
import pandas as pd
import json
import io
import urllib3
import cgi
from clint.textui import progress
from os import makedirs
from os.path import join, dirname, isdir
from bulletin import __file__ as __root__

urllib3.disable_warnings()


def download_metabase(filename=None, where='nt.classificacao_final = 2 AND nt.excluir_ficha = 2 AND nt.status_notificacao IN (1, 2)', limit='ALL', offset='0'):
    if not isdir(join('input','queries')):
        makedirs(join('input','queries'))

    header = {
        'Host':'metabase.appsesa.pr.gov.br',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language':'pt-BR',
        'Accept-Encoding':'gzip, deflate, br',
        'Content-Type':'application/x-www-form-urlencoded',
        'Origin':'https://metabase.appsesa.pr.gov.br',
        'Connection':'keep-alive',
        'Referer':'https://metabase.appsesa.pr.gov.br/question',
        'Cookie':'metabase.SESSION=8ef94e23-f391-49ad-8dc0-0899fbc33ccc',
        'Upgrade-Insecure-Requests':'1'
    }

    with open(join(dirname(__root__),'metabase','notifica.sql')) as file:
        sql = " ".join([ row.replace('\n',' ') for row in file.readlines() if not '--' in row ])
        sql = trim_overspace(f"{sql} WHERE {where} ORDER BY id ASC LIMIT {limit} OFFSET {offset}")

    query = {
        "database": 2,
        "type": "native",
        "native": {
            "query": sql
        }
    }

    query = json.dumps(query)

    print(f'Requesting notifica where {where}')

    res = requests.post("https://metabase.appsesa.pr.gov.br/api/dataset/csv",
                            headers = header,
                            data = {'query': query},
                            verify=False,
                            stream=True
                        )

    if res.status_code in [ 200, 202 ] :
        print(f"Success code {res.status_code }")
    else:
        raise Exception(f"Error code {res.status_code }")

    if not filename:
        _, params = cgi.parse_header(res.headers['Content-Disposition'])
        pathfile = join('input','queries',params['filename'])
    else:
        pathfile = join('input','queries',filename)

    with open(join(pathfile),'wb') as out:
        print(f'Saving in {pathfile}')
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                out.write(chunk)
                out.flush()

    print(f"Download finish, time elapsed: {res.elapsed}\n")

    return pathfile
