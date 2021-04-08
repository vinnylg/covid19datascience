#-----------------------------------------------------------------------------------------------------------------------------#
# Esse arquivo faz parte de um pacote de scripts criados para realizar o tratamento de dados do Notifica Covid-19 Paraná.
# Todos os direitos reservados ao autor
#-----------------------------------------------------------------------------------------------------------------------------#

from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
import pandas as pd
import codecs
from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_labels, normalize_number, normalize_municipios, normalize_ibge, normalize_hash, data_hash

#----------------------------------------------------------------------------------------------------------------------
class CasosConfirmados:

    #----------------------------------------------------------------------------------------------------------------------
    def __init__(self, pathfile=''):
        self.__source = None
        self.pathfile = pathfile
        self.database = join(dirname(__root__),'resources','database','casos_confirmados.pkl')
        self.errorspath = join('output','errors','casos_confirmados',datetime.today().strftime('%B_%Y'))

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))

    def __len__(self):
        return len(self.__source)

    #----------------------------------------------------------------------------------------------------------------------
    def shape(self):
        casos = self.__source['casos']
        obitos = casos.loc[casos['obito']=="SIM"]
        return (len(casos),len(obitos))

    def novos_casos(self, casos_raw):
        casos_confirmados =  self.__source['casos']
        casos_raw = casos_raw.sort_values(by='paciente')

        print(f"casos novos: {casos_raw.shape[0]}")

        index_casos_duplicados = casos_raw.loc[casos_raw['hash'].isin(casos_confirmados['hash'])].index.to_list()
        print(f"casos já em casos com a mesma idade: {len(index_casos_duplicados)}")
        index_casos_duplicados_idade_less = casos_raw.loc[casos_raw['hash_less'].isin(casos_confirmados['hash'])].index.to_list()
        print(f"casos já em casos com a idade - 1: {len(index_casos_duplicados_idade_less)}")
        index_casos_duplicados_idade_more = casos_raw.loc[casos_raw['hash_more'].isin(casos_confirmados['hash'])].index.to_list()
        print(f"casos já em casos com a idade + 1: {len(index_casos_duplicados_idade_more)}")

        print(f"dentre os quais {len(set(index_casos_duplicados_idade_more).intersection(index_casos_duplicados)) + len(set(index_casos_duplicados_idade_less).intersection(index_casos_duplicados))} são casos em comum, o que leva a crer que estão duplicados na planilha já com idade a mais ou idade a menos")
        index_duplicados = list(set(index_casos_duplicados + index_casos_duplicados_idade_less + index_casos_duplicados_idade_more))
        print(f"sendo assim, {len(index_duplicados)} casos que já se encontram na planilha serão removidos")

        print(f"\nentão, de {len(casos_raw)} casos baixados hoje  {len(casos_raw)-len(index_duplicados)} serão adicionados\n")
        casos_raw = casos_raw.drop(index=index_duplicados)

        casos_raw.loc[(casos_raw['rs'].isnull()) & (casos_raw['mun_resid'].notnull()), 'mun_resid'] = casos_raw.loc[(casos_raw['rs'].isnull()) & (casos_raw['mun_resid'].notnull()), 'mun_resid'] + '/' + casos_raw.loc[(casos_raw['rs'].isnull()) & (casos_raw['mun_resid'].notnull()), 'uf_resid']

        casos_raw['data_com'] = [ "" ] * len(casos_raw)
        novos_casos = casos_raw[['id','paciente','sexo','idade','mun_resid', 'mun_atend', 'rs', 'nome_exame','data_liberacao','data_com','data_1o_sintomas','hash']]
        novos_casos.to_excel(join('output','novos_casos.xlsx'), index=False)

        return novos_casos

    def novos_obitos(self, novos_casos, obitos_raw):
        casos_confirmados =  self.__source['casos']
        obitos_confirmados = self.__source['obitos']
        obitos_raw = obitos_raw.sort_values(by='paciente')

        print(f"obitos novos notifica {obitos_raw.shape[0]}", end=" ")

        if isfile(join('input','obitos_curitiba.xlsx')):
            obitos_curitiba = pd.read_excel(join('input','obitos_curitiba.xlsx'),
                            converters = {
                               'paciente': normalize_text,
                               'idade': lambda x: normalize_number(x,fill=0),
                               'mun_resid': normalize_text,
                               'rs': lambda x: str(normalize_number(x,fill=99)).zfill(2) if x != 99 else None
                            },
                            parse_dates=['data_cura_obito'])

            obitos_curitiba['hash'] = obitos_curitiba.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
            obitos_curitiba['hash_less'] = obitos_curitiba.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
            obitos_curitiba['hash_more'] = obitos_curitiba.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

            print(f" + {obitos_curitiba.shape[0]} curitiba", end="")

            obitos_raw = obitos_raw.append(obitos_curitiba, ignore_index=True)
        print("\n")

        obitos_raw_duplicates = obitos_raw.loc[obitos_raw.duplicated(subset='hash')]
        print(f"obitos novos duplicados: {obitos_raw_duplicates.shape[0]}")
        obitos_raw_duplicates.to_excel(join(self.errorspath,'obitos_raw_duplicates.xlsx'))
        obitos_raw = obitos_raw.drop_duplicates(subset='hash')

        index_obitos_duplicados = obitos_raw.loc[obitos_raw['hash'].isin(obitos_confirmados['hash'])].index.to_list()
        print(f"obitos já em obitos com a mesma idade: {len(index_obitos_duplicados)}")
        index_obitos_duplicados_idade_less = obitos_raw.loc[obitos_raw['hash_less'].isin(obitos_confirmados['hash'])].index.to_list()
        print(f"obitos já em obitos com a idade - 1: {len(index_obitos_duplicados_idade_less)}")
        index_obitos_duplicados_idade_more = obitos_raw.loc[obitos_raw['hash_more'].isin(obitos_confirmados['hash'])].index.to_list()
        print(f"obitos já em obitos com a idade + 1: {len(index_obitos_duplicados_idade_more)}")

        obitos_duplicados_idade_diferente = list(set(index_obitos_duplicados_idade_more).intersection(index_obitos_duplicados).union(set(index_obitos_duplicados_idade_less).intersection(index_obitos_duplicados)))
        if len(obitos_duplicados_idade_diferente) > 0:
            print(f"dentre os quais {len(obitos_duplicados_idade_diferente)} são obitos duplicados com idade diferente")

        print(f"obitos que estão nos casos com a mesma idade {obitos_raw.loc[obitos_raw['hash'].isin(casos_confirmados['hash'])].shape[0]}")

        index_idade_less = obitos_raw.loc[obitos_raw['hash_less'].isin(casos_confirmados['hash'])].index
        print(f"obitos que estão nos casos porém com um ano a menos {index_idade_less.shape[0]}")
        if len(index_idade_less) > 0:
            obitos_raw.loc[index_idade_less,'idade'] -= 1
            obitos_raw.loc[index_idade_less,'hash'] = obitos_raw.loc[index_idade_less].apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)

        index_idade_more = obitos_raw.loc[obitos_raw['hash_more'].isin(casos_confirmados['hash'])].index
        print(f"obitos que estão nos casos porém com um ano a mais {index_idade_more.shape[0]}")
        if len(index_idade_more) > 0:
            obitos_raw.loc[index_idade_more,'idade'] += 1
            obitos_raw.loc[index_idade_more,'hash'] = obitos_raw.loc[index_idade_more].apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)

        all_casos = casos_confirmados[['hash']].append(novos_casos[['hash']])
        obitos_nao_casos = obitos_raw.loc[~obitos_raw['hash'].isin(all_casos['hash'])]
        obitos_nao_casos.to_excel(join(self.errorspath,'obitos_nao_casos_confirmados.xlsx'))
        print(f"obitos que não estão nos casos {obitos_nao_casos.shape[0]}")

        index_duplicados = list(set(index_obitos_duplicados + index_obitos_duplicados_idade_less + index_obitos_duplicados_idade_more + obitos_nao_casos.index.to_list()))
        print(f"sendo assim, {len(index_duplicados) + len(obitos_raw_duplicates)} obitos que já se encontram na planilha serão removidos")
        print(f"\nentão, de {len(obitos_raw) - len(obitos_curitiba) + len(obitos_raw_duplicates)} obitos baixados hoje + {len(obitos_curitiba)} inseridos de Curitiba, ",end='')
        obitos_raw = obitos_raw.drop(index=index_duplicados)

        obitos_raw.loc[(obitos_raw['rs'].isnull()) & (obitos_raw['mun_resid'].notnull()), 'mun_resid'] = obitos_raw.loc[(obitos_raw['rs'].isnull()) & (obitos_raw['mun_resid'].notnull()), 'mun_resid'] + '/' + obitos_raw.loc[(obitos_raw['rs'].isnull()) & (obitos_raw['mun_resid'].notnull()), 'uf_resid']

        print(f"{len(obitos_raw) - len(obitos_raw.loc[obitos_raw['hash'].isin(obitos_curitiba['hash'])])} do notifica e {len(obitos_raw.loc[obitos_raw['hash'].isin(obitos_curitiba['hash'])])} de Curitiba serão adicionados\n")
        novos_obitos = obitos_raw[['id','paciente','sexo','idade','mun_resid', 'rs', 'data_cura_obito','hash']]
        novos_obitos.to_excel(join('output','novos_obitos.xlsx'), index=False)

        return novos_obitos


    def relatorio(self, novos_casos, novos_obitos):
        casos_confirmados =  self.get_casos()

        casos_confirmadosPR = casos_confirmados.loc[casos_confirmados['rs'].notnull()]

        obitos_confirmados =  self.get_obitos()

        obitos_confirmadosPR = obitos_confirmados.loc[obitos_confirmados['rs'].notnull()]

        print(f"Total de casos: {len(casos_confirmados)} + {len(novos_casos)}")
        print(f"Total de obitos: {len(obitos_confirmados)} + {len(novos_obitos)}\n\n")


        novos_casosPR = novos_casos.loc[novos_casos['rs'].notnull()].copy()
        print(f"Total de casos PR: {len(casos_confirmadosPR)} + {len(novos_casosPR)}")

        novos_obitosPR = novos_obitos.loc[novos_obitos['rs'].notnull()].copy()
        print(f"Total de obitos PR: {len(obitos_confirmadosPR)} + {len(novos_obitosPR)}")

        novos_casosFora = novos_casos.loc[novos_casos['rs'].isnull()].copy()
        print(f"Total de casos Fora: {len(casos_confirmados) - len(casos_confirmadosPR)} + {len(novos_casosFora)}")

        novos_obitosFora = novos_obitos.loc[novos_obitos['rs'].isnull()].copy()
        print(f"Total de obitos Fora: {len(obitos_confirmados) - len(obitos_confirmadosPR)} + {len(novos_obitosFora)}")

        novos_obitosPR_group = novos_obitosPR.groupby(by='mun_resid')

        today = datetime.today()
        ontem = today - timedelta(1)
        anteontem = ontem - timedelta(1)


        retroativos = novos_casosPR.loc[(novos_casosPR['data_liberacao'] <= anteontem)].sort_values(by='data_liberacao')

        with codecs.open(join('output','relatorios',f"relatorio_{(today.strftime('%d/%m/%Y_%Hh').replace('/','_').replace(' ',''))}.txt"),"w","utf-8-sig") as relatorio:
            relatorio.write(f"{today.strftime('%d/%m/%Y - %Hh%M')}\n")
            relatorio.write(f"{len(novos_casosPR):,} novos casos residentes divulgados ".replace(',','.'))

            if len(novos_casosFora) > 0:
                relatorio.write(f"e {len(novos_casosFora):,} não residente{'s' if len(novos_casosFora) > 1 else ''} ".replace(',','.'))
            relatorio.write(f"no PR.")

            if len(retroativos) > 0:
                relatorio.write(f" Sendo:\n")
                relatorio.write(f"Em {today.strftime('%d/%m/%Y')}: {len(novos_casosPR.loc[(novos_casosPR['data_liberacao'] > anteontem)])} novos casos confirmados.\n")
                relatorio.write(f"{len(retroativos)} casos retroativos confirmados do período de {retroativos.iloc[0]['data_liberacao'].strftime('%d/%m/%Y')} à {retroativos.iloc[-1]['data_liberacao'].strftime('%d/%m/%Y')}.\n")
            else:
                relatorio.write(f" \n")

    #----------------------------------------------------------------------------------------------------------------------
    def read(self,pathfile=join('input','casos_confirmados.xlsx'),append=False):
        self.pathfile = pathfile
        casos_confirmados = pd.read_excel(pathfile)

        casos_confirmados['hash_resid'] = casos_confirmados.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        casos_confirmados['hash_atend'] = casos_confirmados.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_atend']), axis=1)
        casos_confirmados['hash_diag'] = casos_confirmados.apply(lambda row: normalize_hash(row['paciente'])+row['data_diagnostico'].strftime('%d%m%Y'), axis=1)

        if isinstance(self.__source, pd.DataFrame) and append:
            self.__source = self.__source.append(casos_confirmados, ignore_index=True)
        else:
            self.__source = casos_confirmados

    #----------------------------------------------------------------------------------------------------------------------
    def load(self):
        try:
            self.__source = pd.read_pickle(self.database)
        except:
            raise Exception(f"Arquivo {self.database} não encontrado")

    #----------------------------------------------------------------------------------------------------------------------
    def save(self, df=None):
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            new_df = df
            self.__source = new_df
        elif isinstance(self.__source, pd.DataFrame) and len(self.__source) > 0:
            new_df = self.__source
        else:
            raise Exception('Não é possível salvar um DataFrame inexistente, realize a leitura antes ou passe como para esse método')

        new_df.to_pickle(self.database)

    #----------------------------------------------------------------------------------------------------------------------
    def get_casos(self):
        return self.__source.copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_obitos(self):
        return self.__source.loc[self.__source['obito']].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_recuperados(self):
        return self.__source.loc[self.__source['recuperado']].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def get_ativos(self):
        return self.__source.loc[self.__source['ativo']].copy()

    #----------------------------------------------------------------------------------------------------------------------
    def __get_uf(self,mun):
        if '/' in mun:
            _, uf = mun.split('/')
        else:
            uf = 'PR'
        return uf

    #----------------------------------------------------------------------------------------------------------------------
    def __get_mun(self, mun):
        if '/' in mun:
            mun, _ = mun.split('/')
        return mun

    def update(self):
        casos = pd.read_excel(self.pathfile,
                            'Casos confirmados',
                            dtype = {
                               'Ordem': str,
                               'Identificacao': str
                            },
                            converters = {
                                'IBGE_RES_PR': lambda x: normalize_number(x,fill=9999999),
                                'Nome': normalize_text,
                                'Sexo': normalize_text,
                                'Idade': lambda x: normalize_number(x,fill=0),
                                'Mun Resid': normalize_text,
                                'Mun atend': normalize_text,
                                'RS': lambda x: normalize_number(x,fill=99),
                                'Laboratório': normalize_text
                            },
                            parse_dates=False
                        )

        casos.columns = [ normalize_labels(x) for x in casos.columns ]
        casos = casos.rename(columns={'ibge_res_pr': 'ibge7'})

        casos['rs'] = casos['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)
        casos['ibge7'] = casos['ibge7'].apply(lambda x: str(x).zfill(7) if x != 9999999 else None)

        casos_excluidos = casos.loc[casos['excluir'] == 'SIM']
        print(f"Casos confirmados excluidos: {len(casos_excluidos)}")
        casos = casos.loc[casos['excluir'] != 'SIM']

        # casos['uf_resid'] = casos['mun_resid'].apply(self.get_uf)
        # casos['mun_resid'] = casos['mun_resid'].apply(self.get_mun)

        casos['hash'] = casos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        casos['hash_less'] = casos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
        casos['hash_more'] = casos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

        casos['hash_atend'] = casos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['mun_atend']), axis=1)
        casos['hash_less_atend'] = casos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['mun_atend']), axis=1)
        casos['hash_more_atend'] = casos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['mun_atend']), axis=1)

        casos['hash_dt_diag'] = casos.apply(lambda row: normalize_hash(row['nome'])+row['dt_diag'].strftime('%d%m%Y'), axis=1)

        obitos = pd.read_excel(self.pathfile,
                            'Obitos',
                            usecols = 'A,B,D:J',
                            dtype = {
                               'Ordem': str
                            },
                            converters = {
                                'IBGE_RES_PR': lambda x: normalize_number(x,fill=9999999),
                                'Nome': normalize_text,
                                'Sexo': normalize_text,
                                'Idade': lambda x: normalize_number(x,fill=0),
                                'Município': normalize_text,
                                'RS': lambda x: normalize_number(x,fill=99)
                            },
                            parse_dates=False
                        )

        obitos.columns = [ normalize_labels(x) for x in obitos.columns ]
        obitos = obitos.rename(columns={'ibge_res_pr': 'ibge7'})

        obitos['rs'] = obitos['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)
        obitos['ibge7'] = obitos['ibge7'].apply(lambda x: str(x).zfill(7) if x != 9999999 else None)

        obitos_excluidos = obitos.loc[obitos['excluir'] == 'SIM']
        print(f"Obitos confirmados excluidos: {len(obitos_excluidos)}")
        obitos = obitos.loc[obitos['excluir'] != 'SIM']

        writer = pd.ExcelWriter(join('output',"exclusoes.xlsx"),
                        engine='xlsxwriter',
                        datetime_format='dd/mm/yyyy',
                        date_format='dd/mm/yyyy')

        casos_excluidos.to_excel(writer,sheet_name='casos_excluidos',index=None)
        obitos_excluidos.to_excel(writer,sheet_name='obitos_excluidos',index=None)

        writer.save()
        writer.close()

        # obitos['uf'] = obitos['municipio'].apply(self.get_uf)
        # obitos['municipio'] = obitos['municipio'].apply(self.get_mun)

        obitos['hash'] = obitos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['municipio']), axis=1)
        obitos['hash_less'] = obitos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['municipio']), axis=1)
        obitos['hash_more'] = obitos.apply(lambda row: normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['municipio']), axis=1)
        obitos['hash_dt_obito'] = obitos.apply(lambda row: normalize_hash(row['nome'])+row['data_do_obito'].strftime('%d%m%Y'), axis=1)

        casos.to_pickle(self.database['casos'])
        obitos.to_pickle(self.database['obitos'])

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        self.__source = { 'casos': casos, 'obitos': obitos }

    def update_casos(self, new_casos):
        new_casos.to_pickle(self.database['casos'])
        self.__source['casos'] = new_casos

    def update_obitos(self, new_obitos):
        new_obitos.to_pickle(self.database['obitos'])

        self.__source['obitos'] = new_obitos

    def get_casos(self):
        return self.__source['casos'].copy()

    def get_obitos(self):
        return self.__source['obitos'].copy()

    def fix_mun_resid_casos(self, mun='mun_resid'):
        casos = self.get_casos()

        casos[mun] = casos[mun].apply(lambda x: normalize_municipios(x)[0])
        casos['uf_resid'] = casos[mun].apply(lambda x: normalize_municipios(x)[1])

        casos['ibge'] = casos['ibge7'].apply(normalize_ibge)

        casos_sem_ibge = casos.loc[casos['ibge'].isnull()].copy()
        casos_sem_ibge = casos_sem_ibge.drop(columns=['ibge'])
        casos_sem_ibge['mun_hash'] = casos_sem_ibge[mun].apply(normalize_hash)

        municipios = static.municipios.copy()

        casos_com_ibge = casos.loc[casos['ibge'].notnull()].copy()
        casos_com_ibge = pd.merge(left=casos_com_ibge, right=municipios, how='left', on='ibge')

        municipios = municipios.loc[municipios['uf']!='PR']
        municipios['mun_hash'] = municipios['municipio_sesa'].apply(lambda x: normalize_hash(normalize_text(x)))
        municipios_hash = municipios.drop_duplicates('mun_hash')

        casos_sem_ibge = pd.merge(left=casos_sem_ibge, right=municipios_hash, how='left', on='mun_hash')
        casos_sem_ibge = casos_sem_ibge.drop(columns=['mun_hash'])

        casos_com_ibge = casos_com_ibge.append(casos_sem_ibge.loc[casos_sem_ibge['ibge'].notnull()], ignore_index=True).sort_values('ordem')

        casos_sem_ibge = casos_sem_ibge.loc[casos_sem_ibge['ibge'].isnull()].copy()
        casos_sem_ibge = casos_sem_ibge.drop(columns=["cod_uf", "uf", "estado", "ibge", "municipio_sesa", "municipio_ibge"])
        casos_sem_ibge['mun_hash'] = casos_sem_ibge[mun].apply(normalize_hash)

        municipios['mun_hash'] = municipios['municipio_ibge'].apply(lambda x: normalize_hash(normalize_text(x)))
        municipios_hash = municipios.drop_duplicates('mun_hash')

        casos_sem_ibge = pd.merge(left=casos_sem_ibge, right=municipios_hash, how='left', on='mun_hash')
        casos = casos_com_ibge.append(casos_sem_ibge, ignore_index=True).sort_values('ordem')

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

    def fix_mun_resid_obitos(self):
        obitos = self.get_obitos()

        obitos['municipio'] = obitos['municipio'].apply(lambda x: normalize_municipios(x)[0])
        # obitos['uf'] = obitos['municipio'].apply(lambda x: normalize_municipios(x)[1])

        obitos.loc[obitos['rs'].isnull(), 'uf'] = obitos.loc[obitos['rs'].isnull(), 'municipio'].apply(self.get_est)

        obitos.loc[obitos['rs'].isnull(), 'municipio'] = obitos.loc[obitos['rs'].isnull(), 'municipio'] + '/' + obitos.loc[obitos['rs'].isnull(), 'uf']

        return obitos
