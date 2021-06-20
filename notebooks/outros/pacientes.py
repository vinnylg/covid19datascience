from os import makedirs
from os.path import isdir
import numpy as np
import pandas as pd
from tqdm import tqdm
import gc
from bulletin.sistemas.notifica import Notifica
from bulletin.utils.qgram import KGram
from bulletin.utils.utils import Timer

def agrupar_pacientes(pthread, notificacoes, begin, end):
    pacientes = [notificacoes[begin]]
    for i in range(begin,end):
        achou = False
        for j in range(len(pacientes)):
            if notificacoes[i].intersection(pacientes[j]):
                pacientes[j] = pacientes[j].union(notificacoes[i])
                achou = True
                break
        if not achou:
            pacientes.append(notificacoes[i])

    return (pthread,pacientes)


baixar_notifica = True if input("Enter para continuar, S para baixar notifica") == 'S' else False
ler_notifica = True if input("Enter para continuar, S para ler notifica") == 'S' else False

if __name__ == "__main__":
    timer_total = Timer()
    timer_total.start()
    timer = Timer()

    if not isdir('data/qgram'):
            makedirs('data/qgram')

    timer.start()
    notifica = Notifica()
    if ler_notifica or baixar_notifica:
        if baixar_notifica:
            notifica.download_todas_notificacoes()
        if ler_notifica:
            notifica.read_todas_notificacoes()
            notifica.save()
    else:
        notifica.load()
    timer.stop()
    print(f"{notifica.shape()}")
    print("Load notifica: ",timer.ftime())

    timer.start()
    notificacoes = notifica.get_casos()
    # notificacoes = notificacoes.loc[(notificacoes['ibge_residencia']==412810) & (notificacoes['classificacao_final']==2)]

    ids = notificacoes[['id']].values
    hash_notificacoes = notificacoes[['hash_cpf','hash_mae','hash_nasc','hash_resid','hash_atend']]
    hash_notificacoes = hash_notificacoes.where(pd.notnull(notificacoes), None)

    hash_notificacoes = hash_notificacoes.values
    set_hash_notificacoes = [ (set(filter(lambda x: x != None, notificacao))) for notificacao in hash_notificacoes ]

    timer.stop()
    print("Prepare data: ",timer.ftime())

    timer.start()
    alfabet = set(list('BCDFGJKLMNPQRSTVWXZ0123456789'.lower()))

    q = 2
    qgram = KGram(alfabet,q)
    chunk_size = 100000

    for i,set_spliced in enumerate([ set_hash_notificacoes[i:i+chunk_size] for i in range(0,len(set_hash_notificacoes)+1,chunk_size)]):
        print(f"{i}:{i*len(set_spliced)+len(set_spliced)}/{len(set_hash_notificacoes)}")
        freq_list = []
        gc.collect()
        for set_hash in tqdm(set_spliced):
            hash = "".join(list(set_hash))
            hash = qgram.space(hash)
            freq_list.append(qgram.freqkgrams(hash)[1])

        freq_list = np.array(freq_list)
        ids_spliced = list(ids)[i:i+chunk_size]
        with open(f'data/qgram/{i}.npz','wb') as fileout:
            np.savez_compressed(fileout,data=freq_list, source=ids_spliced)

    timer.stop()
    timer_total.stop()
    print("Calculate frequences: ",timer.ftime())
    print("Tudo: ",timer_total.ftime())