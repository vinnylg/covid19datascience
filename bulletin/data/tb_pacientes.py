from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
from sys import exit
import pandas as pd

from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_hash

class TbPacientes:
    def __init__(self, pathfile=join('input','tb_pacientes.csv'), force=False, hard=False):
        self.pathfile = pathfile
        self.__source = None
        self.checksum_file = join(dirname(__root__),'resources','database','tb_pacientes_checksum')
        self.database = join(dirname(__root__),'resources','database','tb_pacientes.pkl')
        self.errorspath = join('output','errors','tb_pacientes')

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))

        if isfile(self.pathfile):
            saved_checksum = None

            if isfile(self.checksum_file):
                with open(self.checksum_file, "r") as checksum:
                    saved_checksum = checksum.read()

            with open(self.pathfile, "rb") as filein:
                bytes = filein.read()
                self.checksum = sha256(bytes).hexdigest()

            if saved_checksum != self.checksum:
                if force:
                    self.update()
            else:
                if hard:
                    self.update()

            if isfile(self.database):
                self.__source = pd.read_pickle(self.database)
            else:
                self.update()

        else:
            if not isdir(dirname(self.pathfile)):
                makedirs(dirname(self.pathfile))

            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def __len__(self):
        return len(self.__source)

    def update(self):

        tb_pacientes = pd.read_csv(self.pathfile, sep=';',
            dtype={
                "Identificacao": 'int32',
                "Idade": 'int32',
                "idade_mais1": 'int32',
                "idade_menos1": 'int32'
            },
            converters={
                "IBGE_RES_PR": normalize_text,
                "IBGE_ATEND_PR": normalize_text,
                "Nome": normalize_text,
                "Sexo": normalize_text,
                "Mun_Resid": normalize_text,
                "Mun_atend": normalize_text,
                "Laboratorio": normalize_text,
                "Dt_viagem_retorno": normalize_text,
                "Local_viagem": normalize_text,
                "Exposicao": normalize_text,
                "Obito": lambda x: normalize_text(x) if x else 'NAO',
                "Telefone": normalize_text,
                "Fonte": normalize_text,
                "idade_original": normalize_text,
                "COD_LABORATORIO": normalize_number
            },
            parse_dates=["Dt_diag", "dt_notificacao", "dt_inicio_sintomas", "15_dia_Isolamento", "Data_de_internamento", "Dt_alta", "Dt_obito", "DT_ATUALIZACAO", "Dt_internamento","dt_com_obito","dt_com_recuperado",],
            date_parser=lambda x: pd.to_datetime(x,errors='coerce',format='%d/%m/%Y')
        )

        tb_pacientes['hash'] = tb_pacientes.apply(lambda row: sha256(str.encode(normalize_hash(row['Nome'])+str(row['Idade'])+normalize_hash(row['Mun_Resid']))).hexdigest(), axis=1)
        tb_pacientes['hash_less'] = tb_pacientes.apply(lambda row: sha256(str.encode(normalize_hash(row['Nome'])+str(row['Idade']-1)+normalize_hash(row['Mun_Resid']))).hexdigest(), axis=1)
        tb_pacientes['hash_more'] = tb_pacientes.apply(lambda row: sha256(str.encode(normalize_hash(row['Nome'])+str(row['Idade']+1)+normalize_hash(row['Mun_Resid']))).hexdigest(), axis=1)

        tb_pacientes.to_pickle(self.database)

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        self.__source = tb_pacientes

    def get(self):
        return self.__source.copy()
