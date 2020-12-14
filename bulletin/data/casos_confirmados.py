from os.path import dirname, join, isfile, isdir
from datetime import datetime, timedelta
from unidecode import unidecode
from hashlib import sha256
from os import makedirs
from sys import exit
import pandas as pd
import codecs


from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_labels, normalize_number, normalize_municipios, normalize_igbe, trim_overspace, normalize_hash

class CasosConfirmados:
    def __init__(self, pathfile=join('input','Casos Confirmados PR.xlsx'),force=False, hard=False):

        self.pathfile = pathfile
        self.__source = None
        self.checksum_file = join(dirname(__root__),'resources','database','casos_confirmados_checksum')
        self.database = {
            'casos': join(dirname(__root__),'resources','database','casos_confirmados','casos.pkl'),
            'obitos': join(dirname(__root__),'resources','database','casos_confirmados','obitos.pkl')
        }
        self.errorspath = join('output','errors','casos_confirmados')

        if not isdir(self.errorspath):
            makedirs(self.errorspath)

        if not isdir(dirname(self.database['casos'])):
            makedirs(dirname(self.database['casos']))

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

            if isfile(self.database['casos']) and isfile(self.database['obitos']):
                casos = pd.read_pickle(self.database['casos'])
                obitos = pd.read_pickle(self.database['obitos'])
                self.__source = { 'casos': casos, 'obitos': obitos }
            else:
                self.update()

        else:
            if not isdir(dirname(self.pathfile)):
                makedirs(dirname(self.pathfile))

            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def shape(self):
        return (len(self.__source['casos']),len(self.__source['obitos']))

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

            obitos_curitiba['hash'] = obitos_curitiba.apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)
            obitos_curitiba['hash_less'] = obitos_curitiba.apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade']-1)+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)
            obitos_curitiba['hash_more'] = obitos_curitiba.apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade']+1)+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)

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
            obitos_raw.loc[index_idade_less,'hash'] = obitos_raw.loc[index_idade_less].apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)

        index_idade_more = obitos_raw.loc[obitos_raw['hash_more'].isin(casos_confirmados['hash'])].index
        print(f"obitos que estão nos casos porém com um ano a mais {index_idade_more.shape[0]}")
        if len(index_idade_more) > 0:
            obitos_raw.loc[index_idade_more,'idade'] += 1
            obitos_raw.loc[index_idade_more,'hash'] = obitos_raw.loc[index_idade_more].apply(lambda row: sha256(str.encode(normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)

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

            relatorio.write(f"{len(casos_confirmadosPR)+len(novos_casosPR):,} casos confirmados residentes do PR.\n".replace(',','.'))
            relatorio.write(f"{len(casos_confirmados)+len(novos_casos):,} total geral.\n\n".replace(',','.'))
            relatorio.write(f"{len(novos_obitosPR):,} Óbitos residentes do PR:\n".replace(',','.'))

            for municipio, obitos in novos_obitosPR_group:
                relatorio.write(f"{len(obitos):,} {municipio}\n".replace(',','.'))

            if len(novos_obitosFora) > 0:
                relatorio.write('\n')
                relatorio.write(f"{len(novos_obitosFora):,} Óbito{'s' if len(novos_obitosFora) > 1 else ''} não residente{'s' if len(novos_obitosFora) > 1 else ''} do PR.\n".replace(',','.'))

            relatorio.write('\n')
            relatorio.write(f"{len(obitos_confirmadosPR)+len(novos_obitosPR):,} óbitos residentes do PR.\n".replace(',','.'))
            relatorio.write(f"{len(obitos_confirmados)+len(novos_obitos):,} total geral.\n\n".replace(',','.'))

            for _, row in novos_obitos.iterrows():
                relatorio.write(f"{row['sexo']}\t{row['idade']}\t{row['mun_resid'] if row['rs'] else row['mun_resid']}\t{row['rs'] if row['rs'] else '#N/D'}\t{row['data_cura_obito'].day}/{static.meses[row['data_cura_obito'].month-1]}\n")
            relatorio.write('\n')


            if len(retroativos) > 0:
                novos_casosPR['month'] = novos_casosPR.apply(lambda x: x['data_liberacao'].month, axis=1)
                relatorio.write('Casos por meses:\n')
                for group, value in novos_casosPR.groupby(by='month'):
                    relatorio.write(f"{static.meses[int(group)-1]}: {len(value)}\n")
                relatorio.write('\n')

                relatorio.write('Obitos por meses:\n')
                novos_obitosPR['month'] = novos_obitosPR.apply(lambda x: x['data_cura_obito'].month, axis=1)
                for group, value in novos_obitosPR.groupby(by='month'):
                    relatorio.write(f"{static.meses[int(group)-1]}: {len(value)}\n")
                relatorio.write('\n')

                for group, value in novos_casosPR.groupby(by='data_liberacao'):
                    relatorio.write(f"{group.strftime('%d/%m/%Y')}\t{len(value)}\n")

        with codecs.open(join('output','relatorios',f"relatorio_{(today.strftime('%d/%m/%Y_%Hh').replace('/','_').replace(' ',''))}.txt"),"r","utf-8-sig") as relatorio:
            print("\nrelatorio:\n")
            print(relatorio.read())

    def get_uf(self,mun):
        if '/' in mun:
            _, uf = mun.split('/')
        else:
            uf = 'PR'

        return uf

    def get_mun(self, mun):
        if '/' in mun:
            mun, _ = mun.split('/')

        return mun

    def update(self):
        casos = pd.read_excel(self.pathfile,
                            'Casos confirmados',
                            usecols = 'A,B,D:P',
                            dtype = {
                               'Ordem': int,
                            },
                            converters = {
                                'IBGE_RES_PR': lambda x: normalize_number(x,fill=99),
                                'Nome': normalize_text,
                                'Sexo': normalize_text,
                                'Idade': lambda x: normalize_number(x,fill=0),
                                'Mun Resid': normalize_text,
                                'Mun atend': normalize_text,
                                'RS': lambda x: normalize_number(x,fill=99),
                                'Laboratório': normalize_text,
                                'IS': normalize_text
                            },
                            parse_dates=['Dt diag', 'Comunicação', 'data_obito']
                        )

        casos.columns = [ normalize_labels(x) for x in casos.columns ]
        casos = casos.rename(columns={'ibge_res_pr': 'ibge7'})

        casos['rs'] = casos['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)
        casos['ibge7'] = casos['ibge7'].apply(lambda x: str(x).zfill(7) if x != 99 else None)

        print(f"Casos confirmados excluidos: {len(casos.loc[casos['mun_resid'] == 'EXCLUIR'])}")
        casos = casos.loc[casos['mun_resid'] != 'EXCLUIR']

        # casos['uf_resid'] = casos['mun_resid'].apply(self.get_uf)
        # casos['mun_resid'] = casos['mun_resid'].apply(self.get_mun)

        casos['hash'] = casos.apply(lambda row: sha256(str.encode(normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)
        casos['hash_less'] = casos.apply(lambda row: sha256(str.encode(normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)
        casos['hash_more'] = casos.apply(lambda row: sha256(str.encode(normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['mun_resid']))).hexdigest(), axis=1)

        # casos = casos[['ordem','ibge7','ibge','nome','sexo','idade','mun_resid','municipio','uf','mun_atend','rs','laboratorio','dt_diag','comunicacao','is','hash','hash_less','hash_more']]

        obitos = pd.read_excel(self.pathfile,
                            'Obitos',
                            usecols = 'A,B,D:I',
                            dtype = {
                               'Ordem': int,
                            },
                            converters = {
                                'IBGE_RES_PR': lambda x: normalize_number(x,fill=99),
                                'Nome': normalize_text,
                                'Sexo': normalize_text,
                                'Idade': lambda x: normalize_number(x,fill=0),
                                'Município': normalize_text,
                                'RS': lambda x: normalize_number(x,fill=99)
                            },
                            parse_dates = ['Data do óbito']
                        )

        obitos.columns = [ normalize_labels(x) for x in obitos.columns ]
        obitos = obitos.rename(columns={'ibge_res_pr': 'ibge7'})

        obitos['rs'] = obitos['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)

        print(f"Obitos confirmados excluidos: {len(obitos.loc[obitos['municipio'] == 'EXCLUIR'])}")
        obitos = obitos.loc[obitos['municipio'] != 'EXCLUIR']

        # obitos['uf'] = obitos['municipio'].apply(self.get_uf)
        # obitos['municipio'] = obitos['municipio'].apply(self.get_mun)

        obitos['hash'] = obitos.apply(lambda row: sha256(str.encode(normalize_hash(row['nome'])+str(row['idade'])+normalize_hash(row['municipio']))).hexdigest(), axis=1)
        obitos['hash_less'] = obitos.apply(lambda row: sha256(str.encode(normalize_hash(row['nome'])+str(row['idade']-1)+normalize_hash(row['municipio']))).hexdigest(), axis=1)
        obitos['hash_more'] = obitos.apply(lambda row: sha256(str.encode(normalize_hash(row['nome'])+str(row['idade']+1)+normalize_hash(row['municipio']))).hexdigest(), axis=1)

        casos.to_pickle(self.database['casos'])
        obitos.to_pickle(self.database['obitos'])

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        self.__source = { 'casos': casos, 'obitos': obitos }

    def get_casos(self):
        return self.__source['casos'].copy()

    def get_obitos(self):
        return self.__source['obitos'].copy()

    def get_all(self):
        casos = self.get_casos()
        obitos = self.get_obitos()