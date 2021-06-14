import numpy as np
import pandas as pd
from unidecode import unidecode


def normalize_number(num, cast=int, error='fill', fill='-1'):
    try:
        return cast(num)
    except ValueError:
        if error == 'raise':
            raise Exception(ValueError)
        else:
            return normalize_number(fill, cast, 'raise')


def normalize_hash(string: str):
    return "".join(filter(lambda char: 'A' <= char <= 'Z', str(string).upper()))


def trim_overspace(string: str):
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

    return cpf # cpf[:3] + '.' + cpf[3:6] + '.' + cpf[6:9] + '-' + cpf[9:]


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

def normalize_ibge(ibge):
    ibge = str(ibge)
    if len(ibge) > 6:
        ibge = ibge[:len(ibge) - 1]

    return ibge

def normalize_campo_aberto(value):
    # Conjunto de negações encontradas
    nao = {'NAO', 'CONSTA', 'INFO', 'INFORMADO', 'CONTEM'}
    # Anula campo com alguma negação
    return value if (not pd.isnull(value)) and (not set(value.split(" ")).intersection(nao)) else None 
    
    
def normalize_do(do):
    if len(do) >= 8:
        return do.split('-')[0]
    else:
        return None

def normalize_cns(cns):
    cns = ''.join(filter(str.isdigit, str(cns)))
    
    if (len(cns) != 15) or ((sum([int(cns[i]) * (15 - i) for i in range(15)]) % 11) != 0):
        return None
    else:
        return cns
    
    