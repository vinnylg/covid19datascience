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

        tmp_df = pd.read_excel(
                arquivo, 'Casos confirmados', converters={
                    'IBGE_RES_PR': lambda x: normalize_ibge(normalize_number(x, fill='999999')),
                    'RS_RES_PR': lambda x: normalize_number(x, fill='99'),
                    'RS': lambda x: normalize_number(x, fill='99'),
                    'Nome': normalize_text,
                    'Mun Resid': normalize_text,
                    'Mun Atend': normalize_text
                }
        )
        tmp_df.columns = [normalize_labels(x) for x in tmp_df.columns]
        
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

# def novos_casos(self, casos_raw):
#     # casos_raw.to_excel(join("output","casos_raw.xlsx"))
#     casos_confirmados = self.__source['casos']
#     casos_raw = casos_raw.sort_values(by='paciente')
#
#     print(f"casos novos: {casos_raw.shape[0]}")
#
#     print(f"casos novos duplicados: {casos_raw.loc[casos_raw.duplicated(subset='hash')].shape[0]}")
#     casos_raw.loc[casos_raw.duplicated(subset='hash')].to_excel(join(self.errorspath, 'casos_raw_duplicates.xlsx'))
#     casos_raw = casos_raw.drop_duplicates(subset='hash')
#
#     dropar = casos_raw.loc[casos_raw['data_liberacao'].isnull()]
#     dropar.to_excel(join(self.errorspath, 'casos_raw_sem_liberacao.xlsx'))
#     print(f"casos novos sem diagnóstico: {dropar.shape[0]}")
#     # casos_raw = casos_raw.drop(index=dropar.index)
#
#     dropar = casos_raw.loc[casos_raw['nome_exame'].isnull()]
#     dropar.to_excel(join(self.errorspath, 'casos_raw_sem_nome_exame.xlsx'))
#     print(f"casos novos sem laboratório: {dropar.shape[0]}")
#     # casos_raw = casos_raw.drop(index=dropar.index)
#
#     dropar = casos_raw.loc[casos_raw['sexo'].isnull()]
#     print(f"casos novos sem sexo: {dropar.shape[0]}")
#     dropar.to_excel(join(self.errorspath, 'casos_raw_sem_sexo.xlsx'))
#     casos_raw = casos_raw.drop(index=dropar.index)
#
#     dropar = casos_raw.loc[casos_raw['data_liberacao'] > datetime.today()]
#     print(f"casos novos com data_liberacao no futuro: {dropar.shape[0]}")
#     dropar.to_excel(join(self.errorspath, 'casos_data_liberacao_futuro.xlsx'))
#     casos_raw = casos_raw.drop(index=dropar.index)
#
#     dropar = casos_raw.loc[casos_raw['data_liberacao'] < datetime.strptime('12/03/2020', '%d/%m/%Y')]
#     print(f"casos novos com data_liberacao antes de 2020: {dropar.shape[0]}")
#     dropar.to_excel(join(self.errorspath, 'casos_data_liberacao_passado.xlsx'))
#     casos_raw = casos_raw.drop(index=dropar.index)
#
#     print(f"casos novos validos: {casos_raw.shape[0]}")
#
#     index_casos_duplicados = casos_raw.loc[casos_raw['hash'].isin(casos_confirmados['hash'])].index.to_list()
#     print(f"casos já em casos com a mesma idade: {len(index_casos_duplicados)}")
#     index_casos_duplicados_idade_less = casos_raw.loc[
#         casos_raw['hash_less'].isin(casos_confirmados['hash'])].index.to_list()
#     print(f"casos já em casos com a idade - 1: {len(index_casos_duplicados_idade_less)}")
#     index_casos_duplicados_idade_more = casos_raw.loc[
#         casos_raw['hash_more'].isin(casos_confirmados['hash'])].index.to_list()
#     print(f"casos já em casos com a idade + 1: {len(index_casos_duplicados_idade_more)}")
#
#     print(
#         f"dentre os quais {len(set(index_casos_duplicados_idade_more).intersection(index_casos_duplicados)) + len(set(index_casos_duplicados_idade_less).intersection(index_casos_duplicados))} são casos em comum, o que leva a crer que estão duplicados na planilha já com idade a mais ou idade a menos")
#     index_duplicados = list(
#         set(index_casos_duplicados + index_casos_duplicados_idade_less + index_casos_duplicados_idade_more))
#     print(f"sendo assim, {len(index_duplicados)} casos que já se encontram na planilha serão removidos")
#
#     print(
#         f"\nentão, de {len(casos_raw)} casos baixados hoje  {len(casos_raw) - len(index_duplicados)} serão adicionados\n")
#     casos_raw = casos_raw.drop(index=index_duplicados)
#
#     casos_raw.loc[(casos_raw['rs'].isnull()) & (casos_raw['mun_resid'].notnull()), 'mun_resid'] = casos_raw.loc[(
#                                                                                                                     casos_raw[
#                                                                                                                         'rs'].isnull()) & (
#                                                                                                                     casos_raw[
#                                                                                                                         'mun_resid'].notnull()), 'mun_resid'] + '/' + \
#                                                                                                   casos_raw.loc[(
#                                                                                                                     casos_raw[
#                                                                                                                         'rs'].isnull()) & (
#                                                                                                                     casos_raw[
#                                                                                                                         'mun_resid'].notnull()), 'uf_resid']
#     # casos_raw = casos_raw.loc[casos_raw['rs'].notnull()]
#     casos_raw['data_com'] = date.today()
#
#     casos_raw['ibge_residencia'] = casos_raw.apply(
#         lambda row: row['ibge_residencia'] if row['uf_residencia'] == 41 else '999999', axis=1)
#
#     novos_casos = casos_raw[
#         ['id', 'ibge_residencia', 'rs', 'ibge_unidade_notifica', 'paciente', 'sexo', 'idade', 'mun_resid',
#          'mun_atend', 'nome_exame', 'data_liberacao', 'data_com', 'data_1o_sintomas', 'hash']]
#
#     # novos_casos.loc[~novos_casos['evolucao'].isin([1,2]),'evolucao'] = None
#     # novos_casos.loc[novos_casos['evolucao']==1,'evolucao'] = 'CURA'
#     # novos_casos.loc[novos_casos['evolucao']==2,'evolucao'] = 'OBITO'
#
#     novos_casos.to_excel(join('output', 'novos_casos.xlsx'), index=False)
#
#     return novos_casos
#
# def novos_obitos(self, novos_casos, obitos_raw):
#     # obitos_raw.to_excel(join("output","obitos_raw.xlsx"))
#     casos_confirmados = self.__source['casos']
#     obitos_confirmados = self.__source['obitos']
#     obitos_raw = obitos_raw.sort_values(by='paciente')
#
#     print(f"obitos novos notifica {obitos_raw.shape[0]}", end=" ")
#
#     if isfile(join('input', 'obitos_curitiba.xlsx')):
#         obitos_curitiba = pd.read_excel(join('input', 'obitos_curitiba.xlsx'),
#                                         converters={
#                                             'paciente': normalize_text,
#                                             'idade': lambda x: normalize_number(x, fill=0),
#                                             'mun_resid': normalize_text,
#                                             'rs': lambda x: str(normalize_number(x, fill=99)).zfill(
#                                                 2) if x != 99 else None
#                                         },
#                                         parse_dates=['data_cura_obito'])
#
#         obitos_curitiba['hash'] = obitos_curitiba.apply(
#             lambda row: normalize_hash(row['paciente']) + str(row['idade']) + normalize_hash(row['mun_resid']),
#             axis=1)
#         obitos_curitiba['hash_less'] = obitos_curitiba.apply(
#             lambda row: normalize_hash(row['paciente']) + str(row['idade'] - 1) + normalize_hash(row['mun_resid']),
#             axis=1)
#         obitos_curitiba['hash_more'] = obitos_curitiba.apply(
#             lambda row: normalize_hash(row['paciente']) + str(row['idade'] + 1) + normalize_hash(row['mun_resid']),
#             axis=1)
#
#         print(f" + {obitos_curitiba.shape[0]} curitiba", end="")
#
#         obitos_raw = obitos_raw.append(obitos_curitiba, ignore_index=True)
#     print("\n")
#
#     dropar = obitos_raw.loc[obitos_raw['data_cura_obito'].isna()]
#     print(f"obitos novos sem data: {dropar.shape[0]}")
#     dropar.to_excel(join(self.errorspath, 'obitos_raw_sem_data.xlsx'))
#     obitos_raw = obitos_raw.drop(index=dropar.index)
#
#     dropar = obitos_raw.loc[obitos_raw['data_cura_obito'] > datetime.today()]
#     print(f"obitos novos com data no futuro: {dropar.shape[0]}")
#     dropar.to_excel(join(self.errorspath, 'obitos_raw_futuro.xlsx'))
#     obitos_raw = obitos_raw.drop(index=dropar.index)
#
#     dropar = obitos_raw.loc[obitos_raw['data_cura_obito'] < datetime.strptime('12/03/2020', '%d/%m/%Y')]
#     print(f"obitos novos com data no passado: {dropar.shape[0]}")
#     dropar.to_excel(join(self.errorspath, 'obitos_raw_passado.xlsx'))
#     obitos_raw = obitos_raw.drop(index=dropar.index)
#
#     obitos_raw_duplicates = obitos_raw.loc[obitos_raw.duplicated(subset='hash')]
#     print(f"obitos novos duplicados: {obitos_raw_duplicates.shape[0]}")
#     obitos_raw_duplicates.to_excel(join(self.errorspath, 'obitos_raw_duplicates.xlsx'))
#     obitos_raw = obitos_raw.drop_duplicates(subset='hash')
#
#     index_obitos_duplicados = obitos_raw.loc[obitos_raw['hash'].isin(obitos_confirmados['hash'])].index.to_list()
#     print(f"obitos já em obitos com a mesma idade: {len(index_obitos_duplicados)}")
#     index_obitos_duplicados_idade_less = obitos_raw.loc[
#         obitos_raw['hash_less'].isin(obitos_confirmados['hash'])].index.to_list()
#     print(f"obitos já em obitos com a idade - 1: {len(index_obitos_duplicados_idade_less)}")
#     index_obitos_duplicados_idade_more = obitos_raw.loc[
#         obitos_raw['hash_more'].isin(obitos_confirmados['hash'])].index.to_list()
#     print(f"obitos já em obitos com a idade + 1: {len(index_obitos_duplicados_idade_more)}")
#
#     obitos_duplicados_idade_diferente = list(
#         set(index_obitos_duplicados_idade_more).intersection(index_obitos_duplicados).union(
#             set(index_obitos_duplicados_idade_less).intersection(index_obitos_duplicados)))
#     if len(obitos_duplicados_idade_diferente) > 0:
#         print(f"dentre os quais {len(obitos_duplicados_idade_diferente)} são obitos duplicados com idade diferente")
#
#     print(
#         f"obitos que estão nos casos com a mesma idade {obitos_raw.loc[obitos_raw['hash'].isin(casos_confirmados['hash'])].shape[0]}")
#
#     index_idade_less = obitos_raw.loc[obitos_raw['hash_less'].isin(casos_confirmados['hash'])].index
#     print(f"obitos que estão nos casos porém com um ano a menos {index_idade_less.shape[0]}")
#     if len(index_idade_less) > 0:
#         obitos_raw.loc[index_idade_less, 'idade'] -= 1
#         obitos_raw.loc[index_idade_less, 'hash'] = obitos_raw.loc[index_idade_less].apply(
#             lambda row: normalize_hash(row['paciente']) + str(row['idade']) + normalize_hash(row['mun_resid']),
#             axis=1)
#
#     index_idade_more = obitos_raw.loc[obitos_raw['hash_more'].isin(casos_confirmados['hash'])].index
#     print(f"obitos que estão nos casos porém com um ano a mais {index_idade_more.shape[0]}")
#     if len(index_idade_more) > 0:
#         obitos_raw.loc[index_idade_more, 'idade'] += 1
#         obitos_raw.loc[index_idade_more, 'hash'] = obitos_raw.loc[index_idade_more].apply(
#             lambda row: normalize_hash(row['paciente']) + str(row['idade']) + normalize_hash(row['mun_resid']),
#             axis=1)
#
#     all_casos = casos_confirmados[['hash']].append(novos_casos[['hash']])
#     obitos_nao_casos = obitos_raw.loc[~obitos_raw['hash'].isin(all_casos['hash'])]
#     obitos_nao_casos.to_excel(join(self.errorspath, 'obitos_nao_casos_confirmados.xlsx'))
#     print(f"obitos que não estão nos casos {obitos_nao_casos.shape[0]}")
#
#     index_duplicados = list(
#         set(index_obitos_duplicados + index_obitos_duplicados_idade_less + index_obitos_duplicados_idade_more + obitos_nao_casos.index.to_list()))
#     print(
#         f"sendo assim, {len(index_duplicados) + len(obitos_raw_duplicates)} obitos que já se encontram na planilha serão removidos")
#     print(
#         f"\nentão, de {len(obitos_raw) - len(obitos_curitiba) + len(obitos_raw_duplicates)} obitos baixados hoje + {len(obitos_curitiba)} inseridos de Curitiba, ",
#         end='')
#     obitos_raw = obitos_raw.drop(index=index_duplicados)
#
#     obitos_raw.loc[(obitos_raw['rs'].isnull()) & (obitos_raw['mun_resid'].notnull()), 'mun_resid'] = obitos_raw.loc[
#                                                                                                          (
#                                                                                                              obitos_raw[
#                                                                                                                  'rs'].isnull()) & (
#                                                                                                              obitos_raw[
#                                                                                                                  'mun_resid'].notnull()), 'mun_resid'] + '/' + \
#                                                                                                      obitos_raw.loc[
#                                                                                                          (
#                                                                                                              obitos_raw[
#                                                                                                                  'rs'].isnull()) & (
#                                                                                                              obitos_raw[
#                                                                                                                  'mun_resid'].notnull()), 'uf_resid']
#     # obitos_raw = obitos_raw.loc[obitos_raw['rs'].notnull()]
#
#     print(
#         f"{len(obitos_raw) - len(obitos_raw.loc[obitos_raw['hash'].isin(obitos_curitiba['hash'])])} do notifica e {len(obitos_raw.loc[obitos_raw['hash'].isin(obitos_curitiba['hash'])])} de Curitiba serão adicionados\n")
#
#     obitos_raw['data_com_evolucao'] = date.today()
#     novos_obitos = obitos_raw[
#         ['id', 'ibge_residencia', 'rs', 'paciente', 'sexo', 'idade', 'mun_resid', 'data_cura_obito',
#          'data_com_evolucao', 'hash']]
#
#     novos_obitos.to_excel(join('output', 'novos_obitos.xlsx'), index=False)
#
#     return novos_obitos
#
# def relatorio(self, novos_casos, novos_obitos):
#     novos_obitos['mun_resid_swap'] = novos_obitos['mun_resid'].str.title()
#
#     casos_confirmados = self.get_casos()
#
#     casos_confirmadosPR = casos_confirmados.loc[casos_confirmados['rs'] != '99']
#
#     obitos_confirmados = self.get_obitos()
#
#     obitos_confirmadosPR = obitos_confirmados.loc[obitos_confirmados['rs'] != '99']
#
#     print(f"Total de casos: {len(casos_confirmados)} + {len(novos_casos)}")
#     print(f"Total de obitos: {len(obitos_confirmados)} + {len(novos_obitos)}\n\n")
#
#     novos_casosPR = novos_casos.loc[novos_casos['rs'] != '99'].copy()
#     print(f"Total de casos PR: {len(casos_confirmadosPR)} + {len(novos_casosPR)}")
#
#     novos_obitosPR = novos_obitos.loc[novos_obitos['rs'] != '99'].copy()
#     print(f"Total de obitos PR: {len(obitos_confirmadosPR)} + {len(novos_obitosPR)}")
#
#     novos_casosFora = novos_casos.loc[novos_casos['rs'] == '99'].copy()
#     print(f"Total de casos Fora: {len(casos_confirmados) - len(casos_confirmadosPR)} + {len(novos_casosFora)}")
#
#     novos_obitosFora = novos_obitos.loc[novos_obitos['rs'] == '99'].copy()
#     print(f"Total de obitos Fora: {len(obitos_confirmados) - len(obitos_confirmadosPR)} + {len(novos_obitosFora)}")
#
#     novos_obitosPR_group = novos_obitosPR.groupby(by='mun_resid_swap')
#
#     today = datetime.today()
#     ontem = today - timedelta(1)
#     data_retroativos = ontem - timedelta(1)
#
#     print(f"data retroativo: {data_retroativos}")
#
#     retroativos = novos_casosPR.loc[(novos_casosPR['data_liberacao'] <= data_retroativos)].sort_values(
#         by='data_liberacao')
#     obitos_retroativos = novos_obitosPR.loc[(novos_obitosPR['data_cura_obito'] <= data_retroativos)].sort_values(
#         by='data_cura_obito')
#
#     with codecs.open(join('output', 'relatorios',
#                           f"relatorio_{(today.strftime('%d/%m/%Y_%Hh').replace('/', '_').replace(' ', ''))}.txt"),
#                      "w", "utf-8-sig") as relatorio:
#         relatorio.write(f"{today.strftime('%d/%m/%Y')}\n")
#         relatorio.write(f"{len(novos_casosPR):,} novos casos residentes ".replace(',', '.'))
#
#         if len(novos_casosFora) > 0:
#             relatorio.write(
#                 f"e {len(novos_casosFora):,} não residente{'s' if len(novos_casosFora) > 1 else ''} ".replace(',',
#                                                                                                               '.'))
#         relatorio.write(f"divulgados no PR.\n")
#         relatorio.write('\n')
#         relatorio.write(
#             f"{len(obitos_confirmadosPR) + len(novos_obitosPR):,} óbitos residentes do PR.\n".replace(',', '.'))
#         relatorio.write(f"{len(obitos_confirmados) + len(novos_obitos):,} total geral.\n\n".replace(',', '.'))
#
#         for _, row in novos_obitos.iterrows():
#             relatorio.write(
#                 f"{row['sexo']}\t{row['idade']}\t{row['mun_resid_swap'] if row['rs'] else row['mun_resid_swap']}\t{row['rs'] if row['rs'] else '#N/D'}\t{row['data_cura_obito'].day}/{static.meses[row['data_cura_obito'].month - 1]}/{row['data_cura_obito'].year}\n")
#         relatorio.write('\n')
#
#         if len(retroativos) > 0:
#
#             relatorio.write(f"Total divulgados:\n")
#             relatorio.write(f"{len(novos_casosPR):,} novos casos residentes ".replace(',', '.'))
#
#             if len(novos_casosFora) > 0:
#                 relatorio.write(
#                     f"e {len(novos_casosFora):,} não residente{'s' if len(novos_casosFora) > 1 else ''} ".replace(
#                         ',', '.'))
#             relatorio.write(f"divulgados no PR.\n")
#
#             relatorio.write(
#                 f"{len(retroativos)} casos retroativos confirmados no período de {retroativos.iloc[0]['data_liberacao'].strftime('%d/%m/%Y')} à {retroativos.iloc[-1]['data_liberacao'].strftime('%d/%m/%Y')}.\n")
#             relatorio.write(
#                 f"{len(novos_casosPR.loc[(novos_casosPR['data_liberacao'] > data_retroativos)])} novos casos confirmados na data de hoje.\n\n")
#
#             relatorio.write(
#                 f"{len(obitos_retroativos)} obitos retroativos confirmados no período de {obitos_retroativos.iloc[0]['data_cura_obito'].strftime('%d/%m/%Y')} à {obitos_retroativos.iloc[-1]['data_cura_obito'].strftime('%d/%m/%Y')}.\n")
#             relatorio.write(
#                 f"{len(novos_obitosPR.loc[(novos_casosPR['data_cura_obito'] > data_retroativos)])} novos casos confirmados na data de hoje.\n\n")
#
#             novos_casosPR['month'] = novos_casosPR.apply(lambda x: x['data_liberacao'].month, axis=1)
#             novos_casosPR['year'] = novos_casosPR.apply(lambda x: x['data_liberacao'].year, axis=1)
#             relatorio.write('Novos casos por meses:\n')
#             for group, value in novos_casosPR.groupby(by=['year', 'month']):
#                 relatorio.write(f"{static.meses[int(group[1]) - 1]}\{group[0]}: {len(value)}\n")
#             relatorio.write('\n')
#
#             relatorio.write('Novos obitos por meses:\n')
#             novos_obitosPR['month'] = novos_obitosPR.apply(lambda x: x['data_cura_obito'].month, axis=1)
#             novos_obitosPR['year'] = novos_obitosPR.apply(lambda x: x['data_cura_obito'].year, axis=1)
#             for group, value in novos_obitosPR.groupby(by=['year', 'month']):
#                 relatorio.write(f"{static.meses[int(group[1]) - 1]}\{group[0]}: {len(value)}\n")
#             relatorio.write('\n')
#
#             relatorio.write('Novos obitos por dia:\n')
#             for group, value in novos_obitosPR.groupby(by='data_cura_obito'):
#                 relatorio.write(f"{group.strftime('%d/%m/%Y')}: {len(value)}\n")
#
#             # -----RELATÓRIO DA COMUNICAÇÃO--------------
#             obitos_list = []
#             munic = []
#             for municipio, obitos in novos_obitosPR_group:
#                 obito = len(obitos)
#                 obitos_list.append(obito)
#                 munic.append(municipio)
#
#             dicionario = (dict(zip(list(munic), list(obitos_list))))
#             # print(dicionario)
#             dicionario = sorted(dicionario.items(), key=lambda x: x[1], reverse=True)
#             # print(dicionario)
#
#             relatorio.write(
#                 f"\n MANDAR ESSE ÚLTIMO PARÁGRAFO PARA A COMUNICAÇÃO NO PRIVADO\nOs pacientes que foram a óbito residiam em: ")
#             for municip, obit in dict(dicionario).items():
#                 if obit != 1:
#                     relatorio.write(f"{municip} ({obit})")
#                     relatorio.write(f", ")
#             relatorio.write(f".\n")
#             relatorio.write(
#                 f"A Sesa registra ainda a morte de uma pessoa que residia em cada um dos seguintes municípios:  ")
#             for municip, obit in dict(dicionario).items():
#                 if obit == 1:
#                     relatorio.write(f"{municip}")
#                     relatorio.write(f", ")
#
# # ----------------------------------------------------------------------------------------------------------------------
# def __get_mun(self, mun):
#     if '/' in mun:
#         mun, _ = mun.split('/')
#     return mun
#
# def update(self):
#     casos = pd.read_excel(self.pathfile,
#                           'Casos confirmados',
#                           converters={
#                               'IBGE_RES_PR': lambda x: normalize_number(x, fill=9999999),
#                               'Nome': normalize_text,
#                               'Sexo': normalize_text,
#                               'Idade': lambda x: normalize_number(x, fill=0),
#                               'Mun Resid': normalize_text,
#                               'Mun atend': normalize_text,
#                               'RS': lambda x: normalize_number(x, fill=99),
#                               'Laboratório': normalize_text
#                           },
#                           parse_dates=False
#                           )
#
#     casos.columns = [normalize_labels(x) for x in casos.columns]
#     casos = casos.rename(columns={'ibge_res_pr': 'ibge7'})
#
#     print(f"Casos excluidos: {len(casos.loc[casos['excluir'] == 'SIM'])}")
#     casos = casos.loc[casos['excluir'] != 'SIM']
#
#     casos['hash'] = casos.apply(
#         lambda row: normalize_hash(row['paciente']) + str(row['idade']) + normalize_hash(row['mun_resid']), axis=1)
#     casos['hash_less'] = casos.apply(
#         lambda row: normalize_hash(row['paciente']) + str(row['idade'] - 1) + normalize_hash(row['mun_resid']),
#         axis=1)
#     casos['hash_more'] = casos.apply(
#         lambda row: normalize_hash(row['paciente']) + str(row['idade'] + 1) + normalize_hash(row['mun_resid']),
#         axis=1)
#
#     obitos = pd.read_excel(self.pathfile, 'Obitos', dtype={'ibge_resid': str, 'ibge_atend': str, 'rs': str})
#
#     print(f"Obitos excluidos: {len(obitos.loc[obitos['excluir'] == 'SIM'])}")
#     obitos = obitos.loc[obitos['excluir'] != 'SIM']
#
#     obitos['hash'] = obitos.apply(
#         lambda row: normalize_hash(row['paciente']) + str(row['idade']) + normalize_hash(row['mun_resid']), axis=1)
#     obitos['hash_less'] = obitos.apply(
#         lambda row: normalize_hash(row['paciente']) + str(row['idade'] - 1) + normalize_hash(row['mun_resid']),
#         axis=1)
#     obitos['hash_more'] = obitos.apply(
#         lambda row: normalize_hash(row['paciente']) + str(row['idade'] + 1) + normalize_hash(row['mun_resid']),
#         axis=1)
#
#     casos.to_pickle(self.database['casos'])
#     obitos.to_pickle(self.database['obitos'])
#
#     with open(self.checksum_file, "w") as checksum:
#         checksum.write(self.checksum)
#
#     self.__source = {'casos': casos, 'obitos': obitos}
#
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
