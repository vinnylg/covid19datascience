# from bulletin.casos_confirmados import CasosConfirmados
# from bulletin.notifica import Notifica
from os.path import join, isdir
from os import makedirs

from pathlib import Path

root = Path(__file__).parent
print('root:',root)


parent = root.parent
print('parent:',parent)

default_input = join(parent,'input')

print('default_input:',default_input)
if not isdir(default_input):
    makedirs(default_input)


default_output = join(parent,'output')
print('default_output:',default_output)
if not isdir(default_output):
    makedirs(default_output)
