


import json
import numpy as np
import pandas as pd
from sys import exit, stderr, stdin, stdout
from time import sleep

from tqdm import tqdm
from os.path import dirname, join, isfile, isdir
from os import makedirs
import subprocess
from datetime import datetime, timedelta, date

from bulletin.data.notifica import Notifica
from bulletin.commom.utils import Timer, get_better_notifica
from bulletin.commom.normalize import normalize_hash
from bulletin.commom.static import meses
from bulletin.metabase.request import download_metabase

pd.set_option('display.max_columns', None)

output = "pacientes"

if not isdir(output):
    makedirs(output)

# tqdm.pandas()

timerAll = Timer()
timer = Timer()


baixar_notifica = True if input("Enter para continuar, S para baixar notifica") == 'S' else False
ler_notifica = True if input("Enter para continuar, S para ler notifica") == 'S' else False

timer.start()
print('notifica')
notifica = Notifica()
if ler_notifica or baixar_notifica:
    if baixar_notifica:
        notifica.download_todas_notificacoes()
    if ler_notifica:
        notifica.read_todas_notificacoes()
        notifica.save()
else:
    notifica.load()

timer.stop()
timer.ftime()
print(notifica.shape())



casosn = notifica.get_casos()
# casosn = casosn.loc[(casosn['mun_resid']=='CURITIBA') & (casosn['classificacao_final']==2)]

print(casosn.groupby('classificacao_final')[['id']].count().append(pd.DataFrame(index=['total'],columns=['id'],data=[len(casosn)])))

timer.start()
print('soundex')
pacientes = { 'pacientes': casosn['paciente'].apply(normalize_hash).values.tolist() }
with open('pacientes/pacientes.json', 'w') as json_file:
  json.dump(pacientes, json_file)

subprocess.call(("node", 'soundex.js'))

with open('pacientes/pacientes_soundex.json', 'r') as json_file:
  pacientes = json.load(json_file)

casosn['soundex'] = pacientes
casosn = casosn.sort_values('soundex')

timer.stop()
timer.ftime()


timer.start()
print('casos_agrupados_count')

keys = ['soundex','paciente','idade','ibge_unidade_notifica','ibge_residencia','data_nascimento','nome_mae','cpf','id']

casosn_hash = casosn[keys+['status_notificacao','classificacao_final','metodo','evolucao']]
casos_agrupados = casosn_hash.sort_values('soundex').groupby(keys[:-1])
casos_agrupados_count = casos_agrupados[['id']].count().rename(columns={'id':'count'}).sort_values('count', ascending=False)
casos_agrupados_count.loc[casos_agrupados_count['count']>1].to_csv('pacientes/casos_agrupados_count.csv')

timer.stop()
timer.ftime()



timer.start()
print(f'pacientes')

pacientes = pd.DataFrame(index=casos_agrupados_count.index,columns=['ids'], data=[])

for idx,df in casos_agrupados:
    pacientes.loc[idx,'ids'] = [ x for x in df['id'].values ]
    # pacientes.loc[idx,'status_notificacao'] = [ x for x in df['status_notificacao'].values ]
    # pacientes.loc[idx,'classificacao_final'] = [ x for x in df['classificacao_final'].values ]
    # pacientes.loc[idx,'metodo'] = [ x for x in df['metodo'].values ]
    # pacientes.loc[idx,'evolucao'] = [ x for x in df['evolucao'].values ]

pacientes.to_csv('pacientes/pacientes.csv')
timer.stop()
print(f'pacientes ({len(pacientes)}): ')
timer.ftime()



timer.start()
print(f'pacientes_duplicados')

pacientes_duplicados = pacientes.loc[ [True if len(ids) > 1 else False for ids in pacientes['ids']]]
pacientes_duplicados.to_csv('pacientes/pacientes_duplicados.csv')

timer.stop()
print(f'pacientes_duplicados ({len(pacientes_duplicados)}): ')
timer.ftime()


