import functools
import threading
import numpy as np
import pandas as pd
from time import time, sleep
from datetime import date, timedelta
from contextlib import contextmanager

from xlsxwriter.worksheet import (
    Worksheet, cell_number_tuple, cell_string_tuple)

def isvaliddate(dt: date, begin=date(2020, 3, 12), end=date.today()):
    if isinstance(dt,date):
        if dt >= begin:
            if dt <= end:
                return True

    return False

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

#
# def get_nome_sobrenome(paciente):
#     parts = paciente.split(' ')
#     if len(parts) >= 2:
#         return parts[0] + ' ' + parts[-1]
#     else:
#         print(paciente)
#         raise Exception('Sem Nome')
