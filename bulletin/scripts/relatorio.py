#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from os.path import join

from bulletin.casos_confirmados import CasosConfirmados

novos_casos = pd.read_excel(join('output','novos_casos.xlsx'),dtype={'idade':int, 'rs':str})

novos_obitos = pd.read_excel(join('output','novos_obitos.xlsx'),dtype={'idade':int, 'rs':str})

CasosConfirmados().relatorio(novos_casos, novos_obitos)