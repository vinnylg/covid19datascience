#!/usr/bin/env python
# coding: utf-8

from sys import argv
import pandas as pd
from datetime import datetime, timedelta

from bulletin.data.notifica import Notifica
from bulletin.data.casos_confirmados import CasosConfirmados

hoje = datetime.today()
ontem = hoje - timedelta(1)
anteontem = ontem - timedelta(1)

force = False
if len(argv) == 2:
    if argv[1] == '--force':
        force = True

notifica = Notifica(force=force)
notifica.shape()

casos_confirmados = CasosConfirmados(force=force)
casos_confirmados.shape()

notifica.filter_date(anteontem)

notifica_novos_casos = notifica.get_casos()

notifica_novos_obitos = notifica.get_obitos()

novos_casos = casos_confirmados.novos_casos(notifica_novos_casos)

novos_obitos = casos_confirmados.novos_obitos(novos_casos, notifica_novos_obitos)

casos_confirmados.relatorio(novos_casos, novos_obitos)