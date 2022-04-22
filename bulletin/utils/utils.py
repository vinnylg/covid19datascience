import numpy as np
import pandas as pd
from datetime import date
from os import chdir
from os.path import exists, join
import pyminizip as pz
from os.path import join
import os
import yaml
import logging.config
import logging
import coloredlogs
from datetime import date, datetime, timedelta
import pandas
from glob import glob


from pathlib import Path

from os import makedirs
from os.path import join, isdir

from xlsxwriter.worksheet import (
    Worksheet, cell_number_tuple, cell_string_tuple)

from bulletin import root, parent

fdate = lambda x: x.strftime('%d/%m/%Y')

def strlist(l,x):
    ll = l.split(',')
    ll = list(filter(lambda x: len(x)>0, ll))
    if not x in ll:
        ll.append(x)
    return ",".join(ll)

def isvaliddate(dt: date, begin=date(2020, 3, 12), end=date.today()):
    if (not pd.isnull(dt)) and isinstance(dt,date):
        # if dt >= begin:
        if dt.date() >= begin:
            # if dt <= end:
            if dt.date() <= end:
                return dt

    return pd.NaT

def get_better_notifica(df):
    scores = np.zeros(len(df))

    for i, serie in enumerate(df.iterrows()):
        _, row = serie

        if row['idade'] != -99:
            scores[i] += 10

        if row['nome_mae']:
            scores[i] += 10

        if row['cpf']:
            scores[i] += 10

        if row['classificacao_final'] == 2:
            scores[i] += 1000

        if row['criterio_classificacao'] == 1:
            scores[i] += 5

        if row['evolucao'] == 2:
            if row['data_cura_obito'] != pd.NaT:
                scores[i] += 100

        if row['evolucao'] == 1:
            scores[i] += 10

        if row['metodo'] == 1:
            scores[i] += 10

        if row['status_notificacao'] in [1, 2]:
            scores[i] += 10

        if row['data_1o_sintomas'] != pd.NaT:
            scores[i] += 5

        if row['data_coleta'] != pd.NaT:
            scores[i] += 5

        if row['data_recebimento'] != pd.NaT:
            scores[i] += 5

        if row['data_liberacao'] != pd.NaT:
            scores[i] += 10

    i = np.argmax(scores)
    return df.iloc[i].name


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


# def relatorio_exclusoes(df):
#     with codecs.open(f"relatorio_exclusoes_{(today.strftime('%d/%m/%Y_%Hh').replace('/','_').replace(' ',''))}.txt","w","utf-8-sig") as relatorio:
#         relatorio.write()
#         for row in df.iterroes():
#             relatorio.write()

def fix_dtypes(df=None):
    if (df is None):
        raise Exception(f"df is none")
    
    cols = pd.DataFrame(zip(df.columns,df.dtypes),columns=['col','dtype']).set_index('col')
    floats = cols.loc[cols['dtype']=='float64']
    if len(floats):
        for col in floats.index:
            df[col] = df[col].fillna(-1).astype(int)

    return df


def create_backup(first_name, level=2):
    hoje = pd.to_datetime(date.today())

    backup_path = join(root, 'database', 'backups')
    file_name = f"{first_name}{hoje.strftime('%d_%m_%Y')}.zip"

    if (not exists(join(backup_path, file_name))):
        notifica_path = join(root, 'database', 'notifica')
        notifica_list = ['all_notifica_raw.pkl', 'notifica.pkl']

        chdir(notifica_path)

        pz.compress_multiple(notifica_list, [], join(backup_path, file_name), "password_notifica_pickle_2021", level)
        print(f"{first_name}{hoje.strftime('%d_%m_%Y')}.zip was created")

    else:
        print(f"{first_name}{hoje.strftime('%d_%m_%Y')}.zip already founded")

def setup_logging(default_path, name:str='bulletin', default_level=logging.DEBUG):
    """
    | **@author:** Prathyush SP
    | Logging Setup
    """
    path = default_path

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logdir = join(parent,'logs')
    if not isdir(logdir):
        makedirs(logdir)

    if os.path.exists(path):
        with open(path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                config['handlers']['info_file_handler']['filename'] = join(logdir, datetime.now().strftime(f"{name}_%Y_%m_%d_%H_%M_%S.log"))
                logging.config.dictConfig(config)
                coloredlogs.install()
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
                coloredlogs.install(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        coloredlogs.install(level=default_level)
        print('Failed to load configuration file. Using default configs')

    logger = logging.getLogger(name)
    logger.raiseExceptions = True

    logger.info('logging configured')
    return logger