from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
from sys import exit
import pandas as pd

from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_number, normalize_hash, normalize_ibge, normalize_municipios

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

    def shape(self):
        casos = self.__source
        obitos = casos.loc[casos['obito']=="SIM"]
        return (len(casos),len(obitos))

    def update(self):

        tb_pacientes = pd.read_csv(self.pathfile, sep=';',
            low_memory=False,
            dtype={
                "Identificacao": 'str'
            },
            converters={
                "IBGE_RES_PR": normalize_text,
                "IBGE_ATEND_PR": normalize_text,
                "Nome": normalize_text,
                "Sexo": normalize_text,
                'Idade': lambda x: normalize_number(x,fill=0),
                "Mun_Resid": normalize_text,
                "Mun_atend": normalize_text,
                "Obito": lambda x: normalize_text(x) if x else 'NAO'
            },
            parse_dates=["Dt_diag", "dt_notificacao", "dt_inicio_sintomas", "15_dia_Isolamento", "Data_de_internamento", "Dt_alta", "Dt_obito", "DT_ATUALIZACAO", "Dt_internamento","dt_com_obito","dt_com_recuperado",],
            date_parser=lambda x: pd.to_datetime(x,errors='coerce',format='%d/%m/%Y')
        )

        tb_pacientes.columns = [ normalize_text(x).lower() for x in tb_pacientes.columns ]

        tb_pacientes['ibge_res_pr'] = tb_pacientes['ibge_res_pr'].apply(lambda x: str(x).zfill(7) if x != '9999999' else None)

        tb_pacientes['hash'] = tb_pacientes.apply(lambda row: normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        tb_pacientes['hash_less'] = tb_pacientes.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
        tb_pacientes['hash_more'] = tb_pacientes.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

        tb_pacientes['hash_atend'] = tb_pacientes.apply(lambda row: normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['mun_atend']), axis=1)
        tb_pacientes['hash_less_atend'] = tb_pacientes.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['mun_atend']), axis=1)
        tb_pacientes['hash_more_atend'] = tb_pacientes.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['mun_atend']), axis=1)


        tb_pacientes.to_pickle(self.database)

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        self.__source = tb_pacientes

    def get(self):
        return self.__source.copy()

    def fix_mun_resid_casos(self, mun='mun_resid', ibge='ibge_res_pr'):
        casos = self.get()

        casos[mun] = casos[mun].apply(lambda x: normalize_municipios(x)[0])
        casos['uf_resid'] = casos[mun].apply(lambda x: normalize_municipios(x)[1])

        casos['ibge'] = casos[ibge].apply(normalize_ibge)

        casos_sem_ibge = casos.loc[casos['ibge'].isnull()].copy()
        casos_sem_ibge = casos_sem_ibge.drop(columns=['ibge'])
        casos_sem_ibge['mun_hash'] = casos_sem_ibge[mun].apply(normalize_hash)


        municipios = static.municipios_sesa_ibge.copy()

        casos_com_ibge = casos.loc[casos['ibge'].notnull()].copy()
        casos_com_ibge = pd.merge(left=casos_com_ibge, right=municipios, how='left', on='ibge')

        municipios = municipios.loc[municipios['uf']!='PR']
        municipios['mun_hash'] = municipios['municipio_sesa'].apply(lambda x: normalize_hash(normalize_text(x)))
        municipios_hash = municipios.drop_duplicates('mun_hash')

        casos_sem_ibge = pd.merge(left=casos_sem_ibge, right=municipios_hash, how='left', on='mun_hash')
        casos_sem_ibge = casos_sem_ibge.drop(columns=['mun_hash'])

        casos_com_ibge = casos_com_ibge.append(casos_sem_ibge.loc[casos_sem_ibge['ibge'].notnull()], ignore_index=True).sort_values('identificacao')

        casos_sem_ibge = casos_sem_ibge.loc[casos_sem_ibge['ibge'].isnull()].copy()
        casos_sem_ibge = casos_sem_ibge.drop(columns=["cod_uf", "uf", "estado", "ibge", "municipio_sesa", "municipio_ibge"])
        casos_sem_ibge['mun_hash'] = casos_sem_ibge[mun].apply(normalize_hash)

        municipios['mun_hash'] = municipios['municipio_ibge'].apply(lambda x: normalize_hash(normalize_text(x)))
        municipios_hash = municipios.drop_duplicates('mun_hash')

        casos_sem_ibge = pd.merge(left=casos_sem_ibge, right=municipios_hash, how='left', on='mun_hash')
        casos = casos_com_ibge.append(casos_sem_ibge, ignore_index=True).sort_values('identificacao')

        casos['ibge'] = casos['ibge'].fillna('99')
        casos['ibge'] = casos['ibge'].apply(lambda x: str(x).zfill(2) if x != '99' else None)

        casos.loc[(casos['uf_resid'] == 'PR') & (casos['uf'].notnull()) & (casos['uf'] != 'PR'), 'uf_resid'] = casos.loc[(casos['uf_resid'] == 'PR') & (casos['uf'].notnull()) & (casos['uf'] != 'PR'), 'uf']
        casos.loc[(casos['uf_resid'] == 'PR') & (casos['uf'].isnull()), 'uf_resid'] = 'ERRO'
        # casos.loc[casos['uf']!='PR', mun] = casos.loc[casos['uf']!='PR', mun] + '/' + casos.loc[casos['uf']!='PR', 'uf_resid']

        return casos

    def get_est(self, mun):
        est = 'ERRO'
        municipios = static.municipios.loc[static.municipios['uf']!='PR']
        municipios['municipio_sesa'] = municipios['municipio_sesa'].apply(lambda x: normalize_hash(normalize_text(x)))
        municipios['municipio_ibge'] = municipios['municipio_ibge'].apply(lambda x: normalize_hash(normalize_text(x)))

        municipio = municipios.loc[municipios['municipio_sesa']==normalize_hash(mun)]
        if len(municipio) == 0:
            municipio = municipios.loc[municipios['municipio_ibge']==normalize_hash(mun)]

        if len(municipio) != 0:
            est = municipio.iloc[0]['uf']

        return est
