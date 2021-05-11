from statistics import mean

def str_len(str):
    try:
        row_l=len(str)
        utf8_l=len(str.encode('utf-8'))
        return (utf8_l-row_l)/2+row_l
    except:
        return None
    return None


def fit_cols(writer,df,sheetname):
    pass
#     worksheet = writer.sheets[sheetname]  # pull worksheet object
#     for idx, col in enumerate(df):  # loop through all columns
#         series = df[col]
#         max_len = mean((
#             series.astype(str).map(str_len).max(),  # len of largest item
#             str_len(str(series.name))  # len of column name/header
#             )) + 2  # adding a little extra space
#         worksheet.set_column(idx, idx, max_len)  
    