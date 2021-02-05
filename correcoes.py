#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from sys import exit
import numpy as np
import pandas as pd
from os.path import dirname, join, isfile, isdir
from os import makedirs
from datetime import datetime, timedelta, date

from bulletin.data.notifica import Notifica
from bulletin.data.casos_confirmados import CasosConfirmados
from bulletin.commom.utils import Timer, get_better_notifica
from bulletin.commom.static import meses
from bulletin.metabase.request import download_metabase

pd.set_option('display.max_columns', None)

output = join("output","correcoes","notifica")

if not isdir(output):
    makedirs(output)

timerAll = Timer()
timer = Timer()


# In[ ]:


timer.start()
classificacao_final = ['0','1','2','3','5']
# classificacao_final = ['2']

notifica = Notifica()
notifica.read(download_metabase(filename='null.csv',where=f"classificacao_final IS NULL"))
# notifica.read(join('input','queries','null.csv'))
for cf in classificacao_final:
    notifica.read(download_metabase(filename=f"{cf}.csv",where=f"classificacao_final = {cf}"), append=True)
    # notifica.read(join('input','queries',f"{cf}.csv"), append=True)

notifica.save()
# notifica.load()
print(timer.ftime())
notifica.shape()


# In[ ]:


timer.start()
casos_confirmados = CasosConfirmados()
# casos_confirmados.read()
# casos_confirmados.save()
casos_confirmados.load()
print(timer.ftime())
casos_confirmados.shape()


# In[ ]:


casosn = notifica.get_casos()
casosc = casos_confirmados.get_casos()
casosn.groupby('classificacao_final')[['id']].count()


# In[ ]:


casos_duplicados = casosn.loc[(casosn['cpf'].notnull()) & (casosn.duplicated('cpf',keep=False))].sort_values('cpf')
print(f"Total de pacientes com mais de uma ocorrencia: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('cpf'))} pacientes pelo CPF que estavam com mais de uma ocorrencia")

all_casos_duplicados = set()
all_duplicados_mantidos = set()
manter = []

for hash, group in casos_duplicados.groupby('cpf'):
    idx = get_better_notifica(group)
    manter.append(idx)


casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_cpf.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()


# In[ ]:


casosn = casosn.drop(index=casos_duplicados)


# In[ ]:


casos_duplicados = casosn.loc[(casosn['nome_mae'].notnull()) & (casosn.duplicated('hash_mae',keep=False))].sort_values('nome_mae')
# casos_duplicados_nome_mae.loc[casos_duplicados_nome_mae['nome_mae']=='0']

print(f"Total de pacientes com mais de uma ocorrencia pelo nome+nome_mae: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('nome_mae'))} pacientes pelo nome+nome_mae que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('nome_mae'):
    idx = get_better_notifica(group)
    manter.append(idx)


casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_nome_mae.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()


# In[ ]:


casosn = casosn.drop(index=casos_duplicados)


# In[ ]:


casos_duplicados = casosn.loc[(casosn['hash_nasc'].notnull()) & (casosn.duplicated('hash_nasc',keep=False))].sort_values('data_nascimento')
# casos_duplicados_nome_mae.loc[casos_duplicados_nome_mae['nome_mae']=='0']

print(f"Total de pacientes com mais de uma ocorrencia pelo data_nascimento: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_nasc'))} pacientes pelo data_nascimento que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_nasc'):
    idx = get_better_notifica(group)
    manter.append(idx)


casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_data_nascimento.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()


# In[ ]:


casosn = casosn.drop(index=casos_duplicados)


# In[ ]:


casos_duplicados = casosn.loc[(casosn['hash_resid'].notnull()) & (casosn.duplicated('hash_resid',keep=False))].sort_values('paciente')

print(f"Total de pacientes com mais de uma ocorrencia pelo hash_resid: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_resid'))} pacientes pelo hash_resid que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_resid'):
    idx = get_better_notifica(group)
    manter.append(idx)


casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_nome_idade_mun_resid.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()


# In[ ]:


casosn = casosn.drop(index=casos_duplicados)


# In[ ]:


casos_duplicados = casosn.loc[(casosn['hash_atend'].notnull()) & (casosn.duplicated('hash_atend',keep=False))].sort_values('paciente')
# casos_duplicados_nome_mae.loc[casos_duplicados_nome_mae['nome_mae']=='0']

print(f"Total de pacientes com mais de uma ocorrencia pelo hash_atend: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_atend'))} pacientes pelo hash_atend que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_atend'):
    idx = get_better_notifica(group)
    manter.append(idx)


casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_nome_idade_mun_atend.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()


# In[ ]:


casosn = casosn.drop(index=casos_duplicados)


# In[ ]:


print(f"Das {notifica.shape()[0]} notificações baixadas, {notifica.shape()[0] - casosn.shape[0]} ({len(all_casos_duplicados)}) foram excluidas por serem do mesmo paciente, sendo mantida a que melhor pontuou nos critérios. Sendo assim, o novo total de notificações existente é de {casosn.shape[0]}")
allcasosn = notifica.get_casos()
allcasosn.loc[all_casos_duplicados,'id'].to_csv(join(output,'casos_duplicados_removidos.csv'), index=False)
allcasosn.loc[all_duplicados_mantidos,'id'].to_csv(join(output,'casos_duplicados_mantidos.csv'), index=False)


# In[ ]:


idx_casos_confirmados = casosc[casosc['hash_resid'].isin(casosn['hash_resid'])].index.tolist()
idx_casos_confirmados += casosc[casosc['hash_atend'].isin(casosn['hash_atend'])].index.tolist()
idx_casos_confirmados += casosc[casosc['hash_diag'].isin(casosn['hash_diag'])].index.tolist()

idx_casos_confirmados = set(idx_casos_confirmados)
casos_confirmados_nao_notifica = casosc.loc[ set(casosc.index.tolist()) - idx_casos_confirmados ].sort_values('ordem')
casos_confirmados_nao_notifica.to_excel(join(output,'casos_confirmados_nao_notifica.xlsx'),index=False)
casos_confirmados_nao_notifica.shape


# In[ ]:


idx_casos_notifica = casosn[casosn['hash_resid'].isin(casosc['hash_resid'])].index.tolist()
idx_casos_notifica += casosn[casosn['hash_atend'].isin(casosc['hash_atend'])].index.tolist()
idx_casos_notifica += casosn[casosn['hash_diag'].isin(casosc['hash_diag'])].index.tolist()

idx_casos_notifica = set(idx_casos_notifica)
casos_confirmados_notifica = casosn.loc[ idx_casos_notifica ].sort_values('id')
casos_confirmados_notifica.to_excel(join(output,'casos_confirmados_notifica.xlsx'),index=False)
print(timerAll.stop())
casos_confirmados_notifica.shape


# In[ ]:


notifica_nao_casos_confirmados = casosn.loc[set(casosn.index.tolist()) - set(idx_casos_notifica)]


# In[ ]:


notifica_nao_casos_confirmados = notifica_nao_casos_confirmados.loc[notifica_nao_casos_confirmados['cod_classificacao_final'] == 2]
notifica_nao_casos_confirmados = notifica_nao_casos_confirmados.loc[notifica_nao_casos_confirmados['cod_status_notificacao'].isin([1,2])]

notifica_nao_casos_confirmados['month'] = notifica_nao_casos_confirmados.apply(lambda x: x['data_notificacao'].month, axis=1)
nnccm = notifica_nao_casos_confirmados.groupby(by='month')[['id']].count()
nnccm.index = [ meses[int(x)-1] for x in nnccm.index] 
nnccm


# In[ ]:


notifica_nao_casos_confirmados.to_excel(join(output,'notifica_nao_casos_confirmados.xlsx'), index=False)
notifica_nao_casos_confirmados.shape

