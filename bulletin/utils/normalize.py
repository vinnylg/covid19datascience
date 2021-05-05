import numpy as np
import pandas as pd
from unidecode import unidecode


def normalize_number(num, cast=int, error='fill', fill='-1'):
    try:
        return cast(num)
    except ValueError:
        if error == 'raise':
            raise Exception(ValueError)
        elif error == 'fill':
            return normalize_number(fill, cast, 'raise')


def normalize_hash(string: str):
    return "".join(filter(lambda char: 'A' <= char <= 'Z', str(string).upper()))


def trim_overspace(string):
    parts = filter(lambda part: len(part) > 0, string.split(" "))
    return " ".join(parts)


def normalize_labels(label):
    label = str(label).replace("'", " ").replace(".", "").replace("\n", "").replace(",", "").lower()
    label = trim_overspace(label).replace(" ", "_")
    label = unidecode(label)

    return label


def normalize_cpf(cpf):
    cpf = ''.join(filter(lambda x: '0' <= x <= '9', str(cpf)))
    cpf = str(cpf).zfill(11)
    digitos = list(map(int, cpf))

    if max(digitos) == min(digitos):
        return None

    validacao = sum(np.array(digitos[:9]) * np.array([10, 9, 8, 7, 6, 5, 4, 3, 2])) * 10 % 11

    if validacao != digitos[-2]:
        return None

    return cpf[:3] + '.' + cpf[3:6] + '.' + cpf[6:9] + '-' + cpf[9:]


def date_hash(date_):
    try:
        date = pd.to_datetime(date_)
        return date.strftime("%d%m%Y")
    except ValueError:
        return '9999999'


def replace_all(txt, replace_list):
    for replace_tuple in replace_list:
        txt = txt.replace(*replace_tuple)
    return txt

def normalize_text(txt):
    if not txt is None:
        txt = txt.upper()
        txt = unidecode(txt)
        txt = trim_overspace(txt)
        txt =  "".join([x if ('A' <= x <= 'Z') or (x in [' ','-','_','/','\'']) else '' for x in txt])
    else:
        txt = ''
    
    return txt


def normalize_municipios(mun):
    mun = normalize_text(mun)
    est = 'PR'
    if '/' in mun:
        mun, est = mun.split('/')

    mun = trim_overspace(mun)

    return mun, est


def normalize_ibge(ibge):
    ibge = str(ibge)
    if len(ibge) > 6:
        ibge = ibge[:len(ibge) - 1]

    return ibge
