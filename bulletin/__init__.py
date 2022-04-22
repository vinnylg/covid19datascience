from datetime import date, datetime, timedelta
import pandas
from glob import glob


from pathlib import Path

from os import makedirs
from os.path import join, isdir

root = Path(__file__).parent
parent = root.parent

logging_file = join(parent,'logging.yaml')

default_input = join(parent,'input')
default_output = join(parent,'output')
logdir = join(parent,'logs')
setup_log = join(parent,'logging.yaml')

if not isdir(default_input):
    makedirs(default_input)

if not isdir(default_output):
    makedirs(default_output)

if not isdir(logdir):
    makedirs(logdir)

agora = pandas.to_datetime(datetime.today())
hoje = pandas.to_datetime(date.today())
ontem = hoje - timedelta(1)
anteontem = ontem - timedelta(1)
ultimo_semana = hoje - timedelta(7)
ultimo_mes = hoje - timedelta(31)
ultimos_60_dias = hoje - timedelta(60)
ultimos_90_dias = hoje - timedelta(90)
data_comeco_pandemia = pandas.to_datetime(date(2020,3,1))
data_comeco_vacinacao = pandas.to_datetime(date(2021,2,1))
menor_data_nascimento = pandas.to_datetime(date(1900,1,1))

tables_path = join(root,'resources','tables')
tables = dict([(Path(x).stem,pandas.read_csv(join(tables_path,x))) for x in glob(join(tables_path,"*.csv"))])

dias_apos = [1,2,3,7,14,21,30,60,90]
dias_apos_label = ['hoje','1 dia','2 dias', '3-6 dias', '7-13 dias', '14-20 dias', '21-29 dias', '30-59 dias', '60-89 dias', '>= 90 dias']