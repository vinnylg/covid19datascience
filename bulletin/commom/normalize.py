from datetime import datetime
from unidecode import unidecode as unidecode

def trim_overspace(text):
	parts = filter(lambda x: len(x) > 0,text.split(" "))
	return " ".join(parts)

def normalize_text(text):
	x = str(text).replace("'"," ").replace(".","").replace("\n","").lower()
	x = trim_overspace(x).replace(" ","_")
	x = unidecode(x)

	if x == 'nan' or len(x) == 0 or x == '0' or x == 'nao_informado':
		return ''
	else:
		return x

def normalize_number(num,cast=int,error='fill',fill='-1'):
	try:
		return cast(num)
	except ValueError:
		if error == 'raise':
			raise Exception(ValueError)
		elif error == 'fill':
			return normalize_number(fill,cast,'raise')

def normalize_date(date):
	date = normalize_text(date)
	date = date.split('_')[0]

	if '-' in date:
		date = date.split('-')
	elif '/' in date:
		date = date.split('/')
	else:
		return None

	if len(date) != 3:
		raise Exception('incomplete date')

	if len(date[0]) == 4:
		year, month, day = tuple(map(int,date))
	elif len(date[2]) == 4:
		day, month, year = tuple(map(int,date))
	else:
		raise Exception('year not found')

	date = datetime(year, month, day)
	today = datetime.now()

	while date.year > today.year:
		year = year-1
		date = datetime(year, month, day)

	#aqui estou considerando que se uma data esta no mesmo mês porém num dia maior que o atual então o mês que está errado
	#porém isso é algo a ser analisado
	while date > today and (date.month > today.month or date.day > today.day):
		month = month - 1
		date = datetime(year, month, day)

	return date