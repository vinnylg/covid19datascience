# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from sys import exit
from time import sleep
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


# %%
casos_confirmados = CasosConfirmados()

if input("Enter para continuar, S para ler casos confirmados") == 'S':
    casos_confirmados.read()
    casos_confirmados.save()
else:
    casos_confirmados.load()

notifica = Notifica()
if input("Enter para continuar, S para baixar e ler notifica") == 'S':
    notifica.download_todas_notificacoes()
    notifica.read_todas_notificacoes()
    notifica.save()
else:
    notifica.load()

dropar = True if input("1) Dropar; ~1) Marcar") == "1" else False

print(casos_confirmados.shape())
print(notifica.shape())


# %%
casosn = notifica.get_casos()
casosn['duplicado'] = 0
casosn['manter'] = 0
casosc = casos_confirmados.get_casos()
casosn.groupby('classificacao_final')[['id']].count()


# %%
casos_duplicados = casosn.loc[(casosn['cpf'].notnull()) & (casosn.duplicated('cpf',keep=False))].sort_values('cpf')
casosn.loc[casos_duplicados.index, 'duplicado'] = 1 

print(f"Total de pacientes com mais de uma ocorrencia: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('cpf'))} pacientes pelo CPF que estavam com mais de uma ocorrencia")

all_casos_duplicados = set()
all_duplicados_mantidos = set()
manter = []

for hash, group in casos_duplicados.groupby('cpf'):
    idx = get_better_notifica(group)
    manter.append(idx)


casosn.loc[manter, 'manter'] = 1 

casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_cpf.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()
if dropar:
    casosn = casosn.drop(index=casos_duplicados)


# %%
casos_duplicados = casosn.loc[(casosn['nome_mae'].notnull()) & (casosn.duplicated('hash_mae',keep=False))].sort_values('nome_mae')
casosn.loc[casos_duplicados.index, 'duplicado'] = 1 

print(f"Total de pacientes com mais de uma ocorrencia pelo nome+nome_mae: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('nome_mae'))} pacientes pelo nome+nome_mae que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('nome_mae'):
    idx = get_better_notifica(group)
    manter.append(idx)

casosn.loc[manter, 'manter'] = 1 

casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_nome_mae.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()
if dropar:
    casosn = casosn.drop(index=casos_duplicados)


# %%
casos_duplicados = casosn.loc[(casosn['hash_nasc'].notnull()) & (casosn.duplicated('hash_nasc',keep=False))].sort_values('data_nascimento')
casosn.loc[casos_duplicados.index, 'duplicado'] = 1 

print(f"Total de pacientes com mais de uma ocorrencia pelo data_nascimento: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_nasc'))} pacientes pelo data_nascimento que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_nasc'):
    idx = get_better_notifica(group)
    manter.append(idx)

casosn.loc[manter, 'manter'] = 1 

casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_data_nascimento.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()
if dropar:
    casosn = casosn.drop(index=casos_duplicados)


# %%
casos_duplicados = casosn.loc[(casosn['hash_resid'].notnull()) & (casosn.duplicated('hash_resid',keep=False))].sort_values('paciente')
casosn.loc[casos_duplicados.index, 'duplicado'] = 1 

print(f"Total de pacientes com mais de uma ocorrencia pelo hash_resid: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_resid'))} pacientes pelo hash_resid que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_resid'):
    idx = get_better_notifica(group)
    manter.append(idx)

casosn.loc[manter, 'manter'] = 1 

casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_nome_idade_mun_resid.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()
if dropar:
    casosn = casosn.drop(index=casos_duplicados)


# %%
casos_duplicados = casosn.loc[(casosn['hash_atend'].notnull()) & (casosn.duplicated('hash_atend',keep=False))].sort_values('paciente')
casosn.loc[casos_duplicados.index, 'duplicado'] = 1 

print(f"Total de pacientes com mais de uma ocorrencia pelo hash_atend: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_atend'))} pacientes pelo hash_atend que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_atend'):
    idx = get_better_notifica(group)
    manter.append(idx)

casosn.loc[manter, 'manter'] = 1 

casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_nome_idade_mun_atend.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()
if dropar:
    casosn = casosn.drop(index=casos_duplicados)


# %%
casos_duplicados = casosn.loc[(casosn['hash_diag'].notnull()) & (casosn.duplicated('hash_diag',keep=False))].sort_values('paciente')
casosn.loc[casos_duplicados.index, 'duplicado'] = 1 

print(f"Total de pacientes com mais de uma ocorrencia pelo hash_diag: {len(casos_duplicados)}")
print(f"{len(casos_duplicados.groupby('hash_diag'))} pacientes pelo hash_diag que estavam com mais de uma ocorrencia")

manter = []
for hash, group in casos_duplicados.groupby('hash_diag'):
    idx = get_better_notifica(group)
    manter.append(idx)

casosn.loc[manter, 'manter'] = 1 

casos_duplicados = set(casos_duplicados.index.tolist())
casos_duplicados = casos_duplicados - set(manter)

all_duplicados_mantidos |= set(manter)
all_casos_duplicados |= casos_duplicados
casosn.loc[casos_duplicados].to_csv(join(output,'pacientes_duplicados_hash_diag.csv'), index=False)

casosn.loc[casos_duplicados].groupby('classificacao_final')[['id']].count()
if dropar:
    casosn = casosn.drop(index=casos_duplicados)


# %%
notifica.save(casosn)


