#!/usr/bin/env python
# coding: utf-8

from sys import argv
from datetime import datetime, timedelta

from bulletin import Notifica
from bulletin import CasosConfirmados

download = True if 'S' in input('Download notifica? S/N') else False
read_cc = True if 'S' in input('Read ncasos_confirmadosotifica? S/N') else False


notifica = Notifica()
if download:
    diario = notifica.download_metabase('diario.sql','diario.csv')
    notifica.read(diario)
else:
    notifica.load()

casos_notifica = notifica.get_casos()
obitos_notifica = notifica.get_obitos()

casos_confirmados = CasosConfirmados()
if read_cc:
    casos_confirmados.read()
else:
    casos_confirmados.load()

novos_casos = casos_confirmados.novos_casos(casos_notifica)
novos_obitos = casos_confirmados.novos_obitos(obitos_notifica)
# recuperados = casos_notifica.loc[casos_notifica['']]
# adicionar(novos_casos,novos_obitos,recuperados,gal)
# check dash
# casos_confirmados.relatorio(novos_casos, novos_obitos)
# generate new casos confirmados
# generate regionais

