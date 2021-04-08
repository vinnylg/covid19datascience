import datetime
import numpy as np
import pandas as pd
from unidecode import unidecode as unidecode

def normalize_hash(text):
	return "".join(filter(lambda x: x >= 'A' and x <= 'Z', str(text).upper()))

def trim_overspace(text):
	parts = filter(lambda x: len(x) > 0,text.split(" "))
	return " ".join(parts)

def normalize_cpf(cpf):
	cpf = ''.join(filter(lambda x: x >= '0' and x <= '9', str(cpf)))
	cpf = str(cpf).zfill(11)
	digitos = list(map(int,cpf))

	if max(digitos) == min(digitos):
		return None

	validacao = sum(np.array(digitos[:9]) * np.array([10,9,8,7,6,5,4,3,2])) * 10 % 11

	if validacao != digitos[-2]:
		return None

	return cpf#[:3] + '.' + cpf[3:6] + '.' + cpf[6:9] + '-' + cpf[9:]

def normalize_hash(text):
	return "".join(filter(lambda x: x >= 'A' and x <= 'Z', str(text).upper()))

def date_hash(date):
	try:
		date = pd.to_datetime(date)
		return date.strftime("%d%m%Y")
	except:
		return '9999999'

def normalize_text(text):
	if text == None:
		return None

	x = str(text).replace(".","").replace("\n","").replace(",","").replace("\t","").replace("''","'").replace("\"","'").upper()
	x = trim_overspace(x)
	x = unidecode(x)

	return x

def normalize_labels(text):
	x = str(text).replace("'"," ").replace(".","").replace("\n","").replace(",","").lower()
	x = trim_overspace(x).replace(" ","_")
	x = unidecode(x)

	return x

def normalize_number(num,cast=int,error='fill',fill='-1'):
	try:
		return cast(num)
	except ValueError:
		if error == 'raise':
			raise Exception(ValueError)
		elif error == 'fill':
			return normalize_number(fill,cast,'raise')

def normalize_municipios(mun):
	mun = normalize_text(mun)
	if '/' in mun:
		mun = mun.split('/')[0]

	mun = trim_overspace(mun)

	return (mun,est)

def normalize_ibge(ibge):
	ibge = str(ibge)
	if ibge:
		ibge = ibge[:len(ibge)-1]

	return ibge