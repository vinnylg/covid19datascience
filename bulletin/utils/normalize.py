import numpy as np
import pandas as pd
from unidecode import unidecode


def normalize_number(num, cast=int, error='fill', fill='-1'):
    try:
        return cast(''.join(filter(lambda x: x.isdigit(),num)))
    except ValueError:
        if error == 'raise':
            raise Exception(ValueError)
        else:
            return normalize_number(fill, cast, 'raise')

def normalize_hash(string: str):
    return "".join(filter(lambda char: 'A' <= char <= 'Z', str(string).upper()))

def normalize_cbo(cbo):
    cbo = ''.join(filter(str.isdigit, str(cbo)))
    if (cbo == ''):
        cbo = '0'
    # elif len(cbo) > 4:
    #     cbo = cbo.zfill(6)[:4]
    return int(cbo)

def trim_overspace(string: str):
    parts = filter(lambda part: len(part) > 0, string.replace("\n", "").split(" "))
    return " ".join(parts)

def normalize_labels(label):
    label = str(label).replace("'", " ").replace(".", "").replace("\n", "").replace(",", "").lower()
    label = trim_overspace(label).replace(" ", "_")
    label = unidecode(label)
    return label

def normalize_cpf(cpf):
    cpf = ''.join(filter(str.isdigit, str(cpf)))
    cpf = str(cpf).zfill(11)
    digitos = list(map(int, cpf))

    ### if x==x | xxx.xxx.xxx-xx
    if max(digitos) == min(digitos):
        return None

    validacao1 = (sum(np.array(digitos[:9]) * np.array([10, 9, 8, 7, 6, 5, 4, 3, 2])) * 10 % 11) % 10
    validacao2 = (sum(np.array(digitos[:10]) * np.array([11,10, 9, 8, 7, 6, 5, 4, 3, 2])) * 10 % 11) % 10

    if (validacao1 != digitos[-2]) or (validacao2 != digitos[-1]):
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
    if pd.isnull(txt) or len(txt)==0:
        return None

    txt = str(txt).upper()
    txt = unidecode(txt)
    txt = trim_overspace(txt)
    txt =  "".join([x if ('A' <= x <= 'Z') or (x in [' ','-','_','/','\'']) else '' for x in txt])
    
    return txt

def normalize_ibge(ibge):
    ibge = str(ibge)
    if len(ibge) > 6:
        ibge = ibge[:len(ibge) - 1]

    return ibge

def normalize_nome_mae(value):
    if pd.isnull(value) or len(value)==0:
        return None
    
    # Conjunto de negações encontradas
    negacao = {'NAO', 'CONSTA', 'INFO', 'INFORMADO', 'CONTEM'}
    # Anula campo com alguma negação
    return value if not set(value.split(" ")).intersection(negacao) else None 

def normalize_do(do):
    do = ''.join(filter(str.isdigit, str(do)))
    return do if len(do) > 0 else None

def normalize_cns(cns):
    cns = ''.join(filter(str.isdigit, str(cns)))
    
    if (len(cns) != 15) or ((sum([int(cns[i]) * (15 - i) for i in range(15)]) % 11) != 0):
        return None
    else:
        return cns

def normalize_cep(cep):
    cep = ''.join(filter(str.isdigit, str(cep)))
    if len(cep) <= 8:
        return cep
    else:
        return None

__all__ = ['normalize_number','normalize_hash','normalize_cbo','trim_overspace','normalize_labels','normalize_cpf', 'date_hash', 'replace_all', 'normalize_text', 'normalize_ibge', 'normalize_nome_mae', 'normalize_do', 'normalize_cns', 'normalize_cep']
