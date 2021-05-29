'''
5.2.1. Regular packages

Python defines two types of packages, regular packages and namespace packages. Regular packages are traditional packages as they existed in Python 3.2 and earlier. A regular package is typically implemented as a directory containing an __init__.py file. When a regular package is imported, this __init__.py file is implicitly executed, and the objects it defines are bound to names in the package’s namespace. The __init__.py file can contain the same Python code that any other module can contain, and Python will add some additional attributes to the module when it is imported.

'''

from os.path import join, isdir
from os import makedirs

from pathlib import Path

print('\n---------------------------------------------------------------\n')
#path from bulletin directory
root = Path(__file__).parent
print('root:',root)

#path from covid19datascience directory
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

print('\n---------------------------------------------------------------\n')