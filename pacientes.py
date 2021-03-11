import numpy as np
import pandas as pd
from tqdm import trange

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

notificacoes = notificacoes[['cpf','hash_mae','hash_nasc','hash_resid','hash_atend']].values
notificacoes = [ set(notificacao) for notificacao in notificacoes ]
print(f"{len(notificacoes)} notificações")


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

