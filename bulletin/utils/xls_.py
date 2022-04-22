import pandas as pd

from xlsxwriter.worksheet import (
    Worksheet, cell_number_tuple, cell_string_tuple)

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
            iter_length = len(strings[data.string]) + 5
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
    return max(lengths)

def set_column_autowidth(worksheet: Worksheet, column: int):
    """
    Set the width automatically on a column in the `Worksheet`.
    !!! Make sure you run this function AFTER having all cells filled in
    the worksheet!
    """
    maxwidth = get_column_width(worksheet=worksheet, column=column)
    if maxwidth is None:
        return
    worksheet.set_column(first_col=column, last_col=column, width=maxwidth)

def auto_fit_columns(wk,df):
    for i, _ in enumerate(df.columns):
        set_column_autowidth(wk,i)

def fit_cols(writer,df,sheetname):
    worksheet = writer.sheets[sheetname]
    auto_fit_columns(worksheet,df)


def format_cols(writer,df,sheet_name,formats,index=True):
    ws = writer.sheets[sheet_name]
    
    if index:
        if isinstance(df.index,pd.MultiIndex):
            cols_of_index = len(df.index.levels)
        else: 
            cols_of_index = 1
    else:
        cols_of_index = 0
    
    if isinstance(df.columns,pd.MultiIndex):
        col_idx = { k+cols_of_index:v for k,v in enumerate(df.columns.get_level_values(-1))}
    else:
        col_idx = { k+cols_of_index:v for k,v in enumerate(df.columns)}
    
    
    col_idx_format = {}
    for key, value in col_idx.items():
        col_idx_format[key] = formats[value]
        
        
    for col, cell_format in col_idx_format.items():
        width = get_column_width(ws,col)
        ws.set_column(col,col,width,cell_format)

    for col in range(0,cols_of_index):
        width = get_column_width(ws,col)
        ws.set_column(col,col,width)
