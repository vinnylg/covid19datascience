import sys
from sys import exit
import glob
import numpy as np
import pandas as pd
from os.path import join, isfile
from datetime import date, datetime
from unidecode import unidecode

from xlsxwriter.worksheet import (
    Worksheet, cell_number_tuple, cell_string_tuple)
    
from time import time, sleep
from contextlib import contextmanager
import functools
import threading

from simpledbf import Dbf5

def get_tb_vacinados():
    if isfile('tb_vacinados.feather'):
        return pd.read_feather('tb_vacinados.feather')
    else:
        return None

def get_vacinados(load=False):
    if load and isfile('vacinados.feather'):
        vacinados = pd.read_feather('vacinados.feather')
    else:
        path = r'vacinados/'
        all_files = glob.glob(path + "*.csv")

        li = []

        for filename in all_files:
            df = pd.read_csv(
                filename,
                sep=';',
                dtype={
                    'paciente_cns': str,
                    'estabelecimento_municipio_codigo': str,
                    'paciente_endereco_coIbgeMunicipio': str
                },

                converters={
                    'paciente_cpf': normalize_cpf,
                    'paciente_dataNascimento': lambda x: normalize_date(x,date(1900,1,1)),
                    'vacina_dataAplicacao': normalize_date
                }
            ).rename(columns={
                    'paciente_cns': 'cns',
                    'paciente_nome': 'paciente',
                    'paciente_cpf': 'cpf',
                    'paciente_nome_mae': 'nome_mae',
                    'paciente_enumSexoBiologico': 'sexo',
                    'paciente_dataNascimento': 'data_nascimento',
                    'vacina_dataAplicacao': 'data_aplicacao',
                    'vacina_descricao_dose': 'dose',

                    'vacina_fabricante_nome': 'fabricante',
                    'paciente_endereco_coIbgeMunicipio': 'ibge',
                    'estabelecimento_municipio_codigo': 'ibge_atendimento'
                })

            li.append(df)
            

        vacinados = pd.concat(li, axis=0, ignore_index=True)

        vacinados.loc[ vacinados['nome_mae'].notnull(), 'hash_mae'] = vacinados.loc[ vacinados['nome_mae'].notnull() ].apply(lambda row: normalize_hash(row['paciente'])+normalize_hash(row['nome_mae']), axis=1)

        vacinados['hash_nasc'] = vacinados.apply(lambda row: normalize_hash(row['paciente'])+date_hash(row['data_nascimento'],date(1900,1,1)), axis=1)

        vacinados.to_feather('vacinados.feather')
    
    return vacinados

'''

SELECT 
    nt.id,
    nt.cns,
    nt.cpf,
    nt.paciente,
    nt.nome_mae,
    nt.data_nascimento,
    nt.ibge_residencia,
    nt.data_notificacao,
    nt.data_cadastro,
    nt.data_coleta,
    nt.data_recebimento,
    nt.data_liberacao,
    CASE
        WHEN nt.evolucao = 1 THEN 'CURA'
        WHEN nt.evolucao = 2 THEN 'OBITO'
        WHEN nt.evolucao = 5 THEN 'OBITO'
        ELSE 'ATIVO'
    END as evolucao,
    nt.data_cura_obito,
    nt.data_notificacao
FROM public.notificacao nt
WHERE
    nt.classificacao_final = 2
    AND nt.status_notificacao IN (1,2)
    AND nt.excluir_ficha = 2
    AND nt.uf_residencia = '41'
    AND nt.evolucao IN (1,2)

'''
def get_casos_confirmados(load=False):
    if load and isfile('casos_confirmados.feather'):
        casos_confirmados = pd.read_feather('casos_confirmados.feather')
    else:
        casos_confirmados = pd.read_csv(
            'casos_confirmados.csv',
            sep=',',
            dtype={
                'cns': str,
                'id': int,
                'ibge_residencia': str
            },
            converters={
                'paciente': normalize_text,
                'cpf': normalize_cpf,
                'data_nascimento': lambda x: normalize_date(x,date(1900,1,1)),
                'data_notificacao': normalize_date,
                'data_cadastro': normalize_date,
                'data_coleta': normalize_date,
                'data_recebimento': normalize_date,
                'data_liberacao': normalize_date,
                'data_cura_obito': normalize_date
            }
        ).rename(columns={'ibge_residencia':'ibge'})

        casos_confirmados.loc[(casos_confirmados['data_nascimento'].notnull()) & (casos_confirmados['data_notificacao'].notnull()), 'idade'] = casos_confirmados.loc[(casos_confirmados['data_nascimento'].notnull()) & (casos_confirmados['data_notificacao'].notnull())].apply(
                    lambda row: row['data_notificacao'].year - row['data_nascimento'].year - (
                            (row['data_notificacao'].month, row['data_notificacao'].day) <
                            (row['data_nascimento'].month, row['data_nascimento'].day)
                    ), axis=1
            )
        casos_confirmados.loc[casos_confirmados['data_nascimento'].isnull(), 'idade'] = -99

        casos_confirmados['data_diagnostico'] = casos_confirmados['data_notificacao']
        casos_confirmados.loc[casos_confirmados['data_liberacao'].notnull(), 'data_diagnostico'] = casos_confirmados.loc[casos_confirmados['data_liberacao'].notnull(), 'data_liberacao']
        casos_confirmados.loc[casos_confirmados['data_recebimento'].notnull(), 'data_diagnostico'] = casos_confirmados.loc[casos_confirmados['data_recebimento'].notnull(), 'data_recebimento']
        casos_confirmados.loc[casos_confirmados['data_coleta'].notnull(), 'data_diagnostico'] = casos_confirmados.loc[casos_confirmados['data_coleta'].notnull(), 'data_coleta']

        casos_confirmados.loc[casos_confirmados['nome_mae'].isnull(),'nome_mae'] = 'NAO CONSTA'

        nao = set(['NAO','CONSTA','INFO','INFORMADO','CONTEM',''])
        #Anula campo com alguma negação na coluna nome_mae
        casos_confirmados.loc[ [True if set(nome_mae.split(" ")).intersection(nao) else False for nome_mae in casos_confirmados['nome_mae'] ], 'nome_mae'] = None

        casos_confirmados.loc[ casos_confirmados['nome_mae'].notnull(), 'hash_mae'] = casos_confirmados.loc[ casos_confirmados['nome_mae'].notnull() ].apply(lambda row: normalize_hash(row['paciente'])+normalize_hash(row['nome_mae']), axis=1)

        casos_confirmados.loc[ casos_confirmados['data_nascimento'].notnull(), 'hash_nasc'] = casos_confirmados.loc[ casos_confirmados['data_nascimento'].notnull() ].apply(lambda row: normalize_hash(row['paciente'])+date_hash(row['data_nascimento'],date(1900,1,1)), axis=1)

        casos_confirmados.reset_index().to_feather('casos_confirmados.feather')
    
    return casos_confirmados 

to_datetime = lambda x: pd.to_datetime(x,format='%d/%m/%Y')

def get_srag(load=False):
    if load and isfile('srag.pkl'):
        srag = pd.read_pickle('srag.pkl')
    else:
        srag1 = Dbf5('SRAGHOSPITALIZADO2020.dbf', codec = 'cp860').to_dataframe()
        print(len(srag1))
        srag2 = Dbf5('SRAGHOSPITALIZADO2021.dbf', codec = 'cp860').to_dataframe()
        print(len(srag2))
        srag = pd.concat([srag1,srag2])
        print(len(srag))
        srag = srag[['NU_NOTIFIC', 'DT_NOTIFIC', 'NU_CPF', 'NM_PACIENT', 'CS_SEXO', 'DT_NASC', 'NU_IDADE_N', 'NM_MAE_PAC', 'SG_UF', 'CO_MUN_RES', 'FATOR_RISC', 'HOSPITAL', 'DT_INTERNA', 'UTI', 'DT_COLETA', 'PCR_RESUL', 'DT_PCR', 'CLASSI_FIN', 'EVOLUCAO', 'DT_EVOLUCA', 'PCR_SARS2', 'AN_SARS2', 'NU_CNS', ]]

        srag.columns = [ x.lower() for x in srag.columns ]
        srag.loc[srag['nu_cns'].isin(['0','999999999999999','000000000000000']),'nu_cns'] = None        
        
        srag.loc[~srag['nm_mae_pac'].isna(),'hash_mae'] =  srag.loc[~srag['nm_mae_pac'].isna(),'nm_pacient'].apply(normalize_hash) + srag.loc[~srag['nm_mae_pac'].isna(),'nm_mae_pac'].apply(normalize_hash)
        
        srag['dt_nasc'] = srag['dt_nasc'].apply(to_datetime) 
        srag['dt_pcr'] = srag['dt_pcr'].apply(to_datetime) 
        srag['dt_evoluca'] = srag['dt_evoluca'].apply(to_datetime) 
        
        srag.loc[~srag['dt_nasc'].isna(),'hash_nasc'] =  srag.loc[~srag['dt_nasc'].isna(),'nm_pacient'].apply(normalize_hash) +  srag.loc[~srag['dt_nasc'].isna(),'dt_nasc'].apply(lambda x: date_hash(x,date(1900, 1, 1))) 
        
        srag['hash_resid'] = srag.apply(lambda x: normalize_hash(x['nm_pacient']) + str(x['co_mun_res']),axis=1)
        
        srag.to_pickle('srag.pkl')
        
    return srag 

def ftime_elapsed(start_time):
    time_elapsed = time() - start_time

    days, seconds = divmod(time_elapsed, 60 * 60 * 24)
    hours, seconds = divmod(seconds, 60 * 60)
    minutes, seconds = divmod(seconds, 60)
    seconds = round(seconds)

    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

def normalize_number(num, cast=int, error='fill', fill='-1'):
    try:
        return cast(num)
    except ValueError:
        if error == 'raise':
            raise Exception(ValueError)
        elif error == 'fill':
            return normalize_number(fill, cast, 'raise')

def normalize_text(text):
	if text == None:
		return None

	x = str(text).replace(".","").replace("\n","").replace(",","").replace("\t","").replace("''","'").replace("\"","'").upper()
	x = trim_overspace(x)
	x = unidecode(x)

	return x

def normalize_hash(text):
    return "".join(filter(lambda x: 'A' <= x <= 'Z', str(text).upper()))

def trim_overspace(text):
    parts = filter(lambda x: len(x) > 0, text.split(" "))
    return " ".join(parts)

def isvaliddate(dt, begin=date(2020, 3, 12), end=date.today()):
    if begin <= dt <= end:
        return True
    return False

def normalize_cpf(cpf):
    cpf = ''.join(filter(lambda x: '0' <= x <= '9', str(cpf)))
    cpf = str(cpf).zfill(11)
    digitos = list(map(int, cpf))

    if max(digitos) == min(digitos):
        return None

    validacao = sum(np.array(digitos[:9]) * np.array([10, 9, 8, 7, 6, 5, 4, 3, 2])) * 10 % 11

    if validacao != digitos[-2]:
        return None

    return cpf  # [:3] + '.' + cpf[3:6] + '.' + cpf[6:9] + '-' + cpf[9:]caralho

def normalize_date(raw_date,begin=date(2020, 3, 12)):
    if isinstance(raw_date,str) and len(raw_date) > 10:
        raw_date = raw_date[:10]

    dt = pd.to_datetime(raw_date, errors='coerce')
    return dt if isvaliddate(dt,begin) else None
    
def date_hash(raw_date,begin=date(2020, 3, 12)):
    try:
        if isvaliddate(raw_date,begin):
            return raw_date.strftime("%d%m%Y")
        else:
            return '9999999'
    except ValueError:
        return '9999999'

def get_better(x,y,typo=str):
    if isinstance(x,typo):
        if (typo == str and len(x)>1) or (typo == datetime):
            return x
    elif isinstance(y,typo):
        if (typo == str and len(y)>1) or (typo == datetime):
            return y
    else:
        return None

def get_better_2(x,y,typo=str):
    res = get_better(x,y)
    if isinstance(res,typo):
        return res
    else:
        raise Exception(f'x:{x}\ty:{y}')

def get_column_width(worksheet: Worksheet, column: int):
    """Get the max column width in a `Worksheet` column."""
    strings = getattr(worksheet, '_ts_all_strings', None)
    if strings is None:
        strings = worksheet._ts_all_strings = sorted(
            worksheet.str_table.string_table,
            key=worksheet.str_table.string_table.__getitem__)
    lengths = set()
    for _, colums_dict in worksheet.table.items():  # type: int, dict
        data = colums_dict.get(column)
        if not data:
            continue
        if type(data) is cell_string_tuple:
            iter_length = len(strings[data.string])
            if not iter_length:
                continue
            lengths.add(iter_length)
            continue
        if type(data) is cell_number_tuple:
            iter_length = len(str(data.number))
            if not iter_length:
                continue
            lengths.add(iter_length)
    if not lengths:
        return None
    return max(lengths) + 5

def set_column_autowidth(worksheet: Worksheet, column: int):
    """
    Set the width automatically on a column in the `Worksheet`.
    !!! Make sure you run this function AFTER having all cells filled in
    the worksheet!
    """
    maxwidth = get_column_width(worksheet=worksheet, column=column)
    if maxwidth is None:
        return
    worksheet.set_column(first_col=column, last_col=column, width=maxwidth + maxwidth * 0.25)

def auto_fit_columns(wk, df):
    for i, _ in enumerate(df.columns):
        set_column_autowidth(wk, i)

def compare_dates(a,b):
    if isinstance(a,date) and isinstance(b,date):
        if a > b:
            return a
        else:
            return b
    elif isinstance(a,date):
        return a
    else:
        return b

def fit_cols(writer,df,col):
    worksheet = writer.sheets[col]
    auto_fit_columns(worksheet,df)
    