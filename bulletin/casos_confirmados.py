# import gc
from os.path import dirname, join, isfile, isdir
# from datetime import datetime, timedelta, date
# from unidecode import unidecode
# from hashlib import sha256
from os import makedirs
import pandas as pd
# import codecs
from bulletin import __file__ as __root__
from bulletin.notifica import Notifica
from bulletin.utils.normalize import normalize_number, normalize_labels, normalize_hash, date_hash, normalize_ibge, normalize_text
from bulletin.utils.timer import Timer
import logging
from pathlib import Path

# ----------------------------------------------------------------------------------------------------------------------
class CasosConfirmados:

    def __init__(self):
        self.df = None
        self.database = join(dirname(__root__), 'database', 'casos_confirmados.pkl')

    def __len__(self):
        return len(self.df)

    def __str__(self):
        return 'CasosConfirmados'

    @Timer('reading Casos Confirmados')
    def read(self, arquivo: str = join(Path(dirname(__root__)).parent,'input','Casos confirmados PR.xlsx'), append: str = False):
        if not isfile(arquivo):
            raise Exception(f"Arquivo {arquivo} não encontrado")

        logging.info(f"Lendo arquivo {arquivo}")

        #dividir por quadrimestre
        tmp_df = pd.read_excel(arquivo, 'Casos confirmados')
        
        tmp_df.columns = [normalize_labels(x) for x in tmp_df.columns]
        tmp_df = tmp_df.loc[tmp_df['excluir'] != 'SIM']
        
        del tmp_df['rs_res_pr']
        tmp_df = tmp_df.rename(columns={'ibge_res_pr':'ibge7'})
        
        tmp_df['ibge7'] = tmp_df['ibge7'].apply(lambda x: normalize_number(x, fill='999999'))
        tmp_df['rs'] = tmp_df['rs'].apply(lambda x: normalize_number(x, fill='99'))
        
        tmp_df['hash'] = (tmp_df['nome'].apply(normalize_hash) +
                          tmp_df['idade'].astype(str) +
                          tmp_df['mun_resid'].apply(normalize_hash))

        tmp_df['hash_less'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x - 1)) +
                                tmp_df['mun_resid'].apply(normalize_hash))

        tmp_df['hash_more'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x + 1)) +
                                tmp_df['mun_resid'].apply(normalize_hash))

        tmp_df['hash_atend'] = (tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].astype(str) +
                                tmp_df['mun_atend'].apply(normalize_hash))

        tmp_df['hash_less_atend'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x - 1)) +
                                tmp_df['mun_atend'].apply(normalize_hash))

        tmp_df['hash_more_atend'] = ( tmp_df['nome'].apply(normalize_hash) +
                                tmp_df['idade'].apply(lambda x: str(x + 1)) +
                                tmp_df['mun_atend'].apply(normalize_hash))


        tmp_df['hash_diag'] = (tmp_df['nome'].apply(normalize_hash) + tmp_df['dt_diag'].apply(date_hash))

        tmp_df.loc[tmp_df['data_obito'].notna(), 'hash_obito'] = tmp_df.loc[tmp_df['data_obito'].notna()].apply(
                lambda row: normalize_hash(row['nome']) + date_hash(row['data_obito']), axis=1
        )

        if isinstance(self.df, pd.DataFrame) and append:
            self.df = self.df.append(tmp_df)
        else:
            self.df = tmp_df

    @Timer('saving Casos Confirmados')
    def save(self):
        self.df.to_pickle(self.database)

    @Timer('loading Casos Confirmados')
    def load(self):
        self.df = pd.read_pickle(self.database)

    def export(self, output_file):
        pass

    def get_obitos(self):
        return self.df.loc[self.df['obito'] == 'SIM'].copy()

    def get_casos(self):
        return self.df.copy()

    def novos_casos(self, casos_notifica):
        casos = self.df
        return casos_notifica.loc[~(
                (casos_notifica['hash_resid'].isin(casos['hash'])) |
                (casos_notifica['hash_resid'].isin(casos['hash_less'])) |
                (casos_notifica['hash_resid'].isin(casos['hash_more'])) |
                (casos_notifica['hash_diag'].isin(casos['hash_diag']))
        )]

    def novos_obitos(self, obitos_notifica):
        obitos = self.df.loc[self.df['obito'] == 'SIM']
        return obitos_notifica.loc[~(
                (obitos_notifica['hash_resid'].isin(obitos['hash'])) |
                (obitos_notifica['hash_resid'].isin(obitos['hash_less'])) |
                (obitos_notifica['hash_resid'].isin(obitos['hash_more'])) |
                (obitos_notifica['hash_diag'].isin(obitos['hash_diag'])) |
                (obitos_notifica['hash_obito'].isin(obitos['hash_obito']))
        )]


# def update_casos(self, new_casos):
#     new_casos.to_pickle(self.database['casos'])
#     self.__source['casos'] = new_casos
#
# def update_obitos(self, new_obitos):
#     new_obitos.to_pickle(self.database['obitos'])
#
#     self.__source['obitos'] = new_obitos
#
# def get_casos(self):
#     return self.__source['casos'].copy()
#
# def get_obitos(self):
#     return self.__source['obitos'].copy()
#
# def fix_mun_resid_casos(self, mun='mun_resid'):
#     casos = self.get_casos()
#
#     casos[mun] = casos[mun].apply(lambda x: normalize_municipios(x)[0])
#     casos['uf_resid'] = casos[mun].apply(lambda x: normalize_municipios(x)[1])
#
#     casos['ibge'] = casos['ibge7'].apply(normalize_ibge)
#
#     casos_sem_ibge = casos.loc[casos['ibge'].isnull()].copy()
#     casos_sem_ibge = casos_sem_ibge.drop(columns=['ibge'])
#     casos_sem_ibge['mun_hash'] = casos_sem_ibge[mun].apply(normalize_hash)
#
#     municipios = static.municipios.copy()
#
#     casos_com_ibge = casos.loc[casos['ibge'].notnull()].copy()
#     casos_com_ibge = pd.merge(left=casos_com_ibge, right=municipios, how='left', on='ibge')
#
#     municipios = municipios.loc[municipios['uf'] != 'PR']
#     municipios['mun_hash'] = municipios['municipio_sesa'].apply(lambda x: normalize_hash(normalize_text(x)))
#     municipios_hash = municipios.drop_duplicates('mun_hash')
#
#     casos_sem_ibge = pd.merge(left=casos_sem_ibge, right=municipios_hash, how='left', on='mun_hash')
#     casos_sem_ibge = casos_sem_ibge.drop(columns=['mun_hash'])
#
#     casos_com_ibge = casos_com_ibge.append(casos_sem_ibge.loc[casos_sem_ibge['ibge'].notnull()],
#                                            ignore_index=True).sort_values('ordem')
#
#     casos_sem_ibge = casos_sem_ibge.loc[casos_sem_ibge['ibge'].isnull()].copy()
#     casos_sem_ibge = casos_sem_ibge.drop(
#         columns=["cod_uf", "uf", "estado", "ibge", "municipio_sesa", "municipio_ibge"])
#     casos_sem_ibge['mun_hash'] = casos_sem_ibge[mun].apply(normalize_hash)
#
#     municipios['mun_hash'] = municipios['municipio_ibge'].apply(lambda x: normalize_hash(normalize_text(x)))
#     municipios_hash = municipios.drop_duplicates('mun_hash')
#
#     casos_sem_ibge = pd.merge(left=casos_sem_ibge, right=municipios_hash, how='left', on='mun_hash')
#     casos = casos_com_ibge.append(casos_sem_ibge, ignore_index=True).sort_values('ordem')
#
#     casos['ibge'] = casos['ibge'].fillna('99')
#     casos['ibge'] = casos['ibge'].apply(lambda x: str(x).zfill(2) if x != '99' else None)
#
#     casos.loc[(casos['uf_resid'] == 'PR') & (casos['uf'].notnull()) & (casos['uf'] != 'PR'), 'uf_resid'] = \
#         casos.loc[(casos['uf_resid'] == 'PR') & (casos['uf'].notnull()) & (casos['uf'] != 'PR'), 'uf']
#     casos.loc[(casos['uf_resid'] == 'PR') & (casos['uf'].isnull()), 'uf_resid'] = 'ERRO'
#     # casos.loc[casos['uf']!='PR', mun] = casos.loc[casos['uf']!='PR', mun] + '/' + casos.loc[casos['uf']!='PR', 'uf_resid']
#
#     return casos
#
# def get_est(self, mun):
#     est = 'ERRO'
#     municipios = static.municipios.loc[static.municipios['uf'] != 'PR']
#     municipios['municipio_sesa'] = municipios['municipio_sesa'].apply(lambda x: normalize_hash(normalize_text(x)))
#     municipios['municipio_ibge'] = municipios['municipio_ibge'].apply(lambda x: normalize_hash(normalize_text(x)))
#
#     municipio = municipios.loc[municipios['municipio_sesa'] == normalize_hash(mun)]
#     if len(municipio) == 0:
#         municipio = municipios.loc[municipios['municipio_ibge'] == normalize_hash(mun)]
#
#     if len(municipio) != 0:
#         est = municipio.iloc[0]['uf']
#
#     return est
#
# def fix_mun_resid_obitos(self):
#     obitos = self.get_obitos()
#
#     obitos['municipio'] = obitos['municipio'].apply(lambda x: normalize_municipios(x)[0])
#     # obitos['uf'] = obitos['municipio'].apply(lambda x: normalize_municipios(x)[1])
#
#     obitos.loc[obitos['rs'].isnull(), 'uf'] = obitos.loc[obitos['rs'].isnull(), 'municipio'].apply(self.get_est)
#
#     obitos.loc[obitos['rs'].isnull(), 'municipio'] = obitos.loc[obitos['rs'].isnull(), 'municipio'] + '/' + \
#                                                      obitos.loc[obitos['rs'].isnull(), 'uf']
#
#     return obitos
