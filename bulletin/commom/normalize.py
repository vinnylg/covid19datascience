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