import numpy as np
import pandas as pd
from tqdm import trange
from hashlib import md5

from bulletin.data.notifica import Notifica

from bulletin.data.notifica import Notifica

baixar_notifica = True if input("Enter para continuar, S para baixar notifica") == 'S' else False
ler_notifica = True if input("Enter para continuar, S para ler notifica") == 'S' else False


notifica = Notifica()
if ler_notifica or baixar_notifica:
    if baixar_notifica:
        notifica.download_todas_notificacoes()
    if ler_notifica:
        notifica.read_todas_notificacoes()
        notifica.save()
else:
    notifica.load()

notificacoes = notifica.get_casos()
notificacoes = notificacoes.loc[(notificacoes['mun_resid']=='CURITIBA') & (notificacoes['classificacao_final']==2)]

notificacoes = notificacoes[['cpf','hash_mae','hash_nasc','hash_resid','hash_atend']]
notificacoes = notificacoes.where(pd.notnull(notificacoes), None)

notificacoes = notificacoes.values
notificacoes = [ (set(filter(lambda x: x != None, notificacao))) for notificacao in notificacoes ]
print(f"{len(notificacoes)} notificações")

with open('notificacoes_hash.txt','w') as out:
    for n in notificacoes:
        for h in n:
            out.write(f"{h}{' ' if h != list(n)[-1] else ''}")
        out.write('\n')

pacientes = [notificacoes[0]]
for i in trange(len(notificacoes)):
    achou = False
    for j in range(len(pacientes)):
        if notificacoes[i].intersection(pacientes[j]):
            pacientes[j] = pacientes[j].union(notificacoes[i])
            achou = True
            break
    if not achou:
        pacientes.append(notificacoes[i])

print(f"{len(pacientes)} pacientes")

with open('paciente_hash.txt','w') as out:
    for n in pacientes:
        for h in n:
            out.write(f"{h}{' ' if h != list(n)[-1] else ''}")
        out.write('\n')