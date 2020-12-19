from time import time
from datetime import date
import pandas as pd
import numpy as np

class Timer:
    def __init__(self):
        self.__start = time()
        self.__running = True

    def stop(self):
        if self.__running:
            self.__running = False
            self.__end = time()
            self.__time_elapsed = self.__end - self.__start
            return self.__time_elapsed
        else:
            print('Timer already stoped')

    def time(self):
        if not self.__running:
            return self.__time_elapsed
        else:
            return time() - self.__start

    def reset(self):
        self.__running = True
        self.__start = time()
        self.__end = 0

def isvaliddate(date,begin=date(2020,3,12),end=date.today()):
	if date != pd.NaT:
		if date >= begin and date <= end:
			return True
	return False

def get_better_notifica(df):
    scores = np.zeros(len(df))

    for i, serie in enumerate(df.iterrows()):
        _, row = serie

        if row['idade'] != -99:
            scores[i]+=50

        if row['nome_mae']:
            scores[i]+=50

        if row['cpf']:
            scores[i]+=50

        if not row['nome_mae'] and not row['cpf'] and row['data_nascimento'] == pd.NaT:
            scores-=500

        if row['cod_classificacao_final'] == 2:
            scores[i]+=100

        if row['cod_criterio_classificacao'] == 1:
            scores[i]+=10

        if row['cod_evolucao'] == 2:
            if row['data_cura_obito'] != pd.NaT:
                scores[i]+=1000
            else:
                scores[i]-=100

        if row['cod_metodo'] == 1:
            scores[i]+=10

        if row['cod_status_notificacao'] in [1, 2]:
            scores[i]+=10

        if row['excluir_ficha'] == 2:
            scores[i]+=10

        if row['cod_origem'] == 1:
            scores[i]+=10

        if row['data_1o_sintomas'] != pd.NaT:
            scores[i]+=50

        if row['data_coleta'] != pd.NaT:
            scores[i]+=40

        if row['data_recebimento'] != pd.NaT:
            scores[i]+=30

        if row['data_liberacao'] != pd.NaT:
            scores[i]+=20

    i = np.argmax(scores)
    return df.iloc[i].name