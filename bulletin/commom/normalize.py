import datetime
import pandas as pd
from unidecode import unidecode as unidecode

from sys import exit
from bulletin.commom import static

def trim_overspace(text):
	parts = filter(lambda x: len(x) > 0,text.split(" "))
	return " ".join(parts)

def normalize_hash(text):
	return "".join(filter(lambda x: x >= 'A' and x <= 'Z', str(text).upper()))

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
	est = 'PR'

	if '/' in mun:
		mun, est = mun.split('/')

	mun = trim_overspace(mun)

	return (mun,est)

def normalize_igbe(ibge):
	if ibge:
		ibge = ibge[:len(ibge)-1]

	return ibge

# def normalize_date(date):
# 	try:
# 		date = normalize_text(date)
# 		date = date.split(' ')[0]

# 		if '-' in date:
# 			date = date.split('-')
# 		elif '/' in date:
# 			date = date.split('/')
# 		else:
# 			return pd.NaT

# 		if len(date) != 3:
# 			return pd.NaT

# 		if len(date[0]) == 4:
# 			year, month, day = tuple(map(int,date))
# 		elif len(date[2]) == 4:
# 			day, month, year = tuple(map(int,date))
# 		else:
# 			return pd.NaT

# 		begin = datetime.date('2020','03','1')
# 		date = datetime.date(year, month, day)
# 		today = datetime.date.today()

# 		if date > today or date < begin:
# 			date = pd.NaT

# 		return date
# 	except:
# 		return pd.NaT