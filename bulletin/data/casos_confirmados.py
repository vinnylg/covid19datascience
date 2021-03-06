from os.path import dirname, join, isfile
from datetime import datetime, date, timedelta
from unidecode import unidecode
from hashlib import sha256
import pandas as pd
import codecs

from sys import exit

from bulletin import __file__ as __root__
from bulletin.commom import static
from bulletin.commom.normalize import normalize_text, normalize_labels, normalize_number, normalize_municipios, normalize_igbe, trim_overspace, normalize_hash

class CasosConfirmados:
    def __init__(self, pathfile:str=join(dirname(__root__),'tmp','Casos confirmados PR.xlsx'),force=False, hard=False):
        self.pathfile = pathfile
        self.__source = None
        self.database = { 'casos': join(dirname(__root__),'tmp','casos.pkl'), 'obitos': join(dirname(__root__),'tmp','obitos.pkl')}
        self.checksum_file = join(dirname(__root__),'tmp','casos_confirmados_checksum')
        self.errorspath = join('output','errors')

        if isfile(self.pathfile):
            saved_checksum = None
            if isfile(self.checksum_file):
                with open(self.checksum_file, "r") as checksum:
                    saved_checksum = checksum.read()
                    print(f"saved checksum: {saved_checksum}")

            with open(self.pathfile, "rb") as filein:
                bytes = filein.read()
                self.checksum = sha256(bytes).hexdigest()
                print(f"current checksum: {self.checksum}")

            if saved_checksum != self.checksum:
                print(f"Parece que o arquivo {self.pathfile} sofreu alterações, considere usar o método update ou o passe force=True")
                if force:
                    print(f"Utilizando o método update")
                    self.update()
            else:
                print(f"Tudo certo, nenhuma alteração detectada")
                if hard:
                    print(f"Utilizando forcadamente com método update")
                    self.update()

            if isfile(self.database['casos']) and isfile(self.database['obitos']):
                casos = pd.read_pickle(self.database['casos'])
                obitos = pd.read_pickle(self.database['obitos'])
                self.__source = { 'casos': casos, 'obitos': obitos }
                print(f"{self.database} carregado")
            else:
                print(f"{self.database} não encontrado, utilizando o método update")
                self.update()

        else:
            exit(f"{self.pathfile} não encontrado, insira o arquivo para dar continuidade")

    def shape(self):
        return (len(self.__source['casos']),len(self.__source['obitos']))

    def novos_casos(self, casos_raw):
        print(casos_raw['uf_residencia'])
        casos_confirmados =  self.__source['casos']
        casos_raw = casos_raw.sort_values(by='paciente')

        print(f"casos novos: {casos_raw.shape[0]}")

        print(f"casos novos duplicados: {casos_raw.loc[casos_raw.duplicated(subset='hash')].shape[0]}")
        casos_raw.loc[casos_raw.duplicated(subset='hash')].to_excel(join(self.errorspath,'casos_raw_duplicates.xlsx'))
        casos_raw = casos_raw.drop_duplicates(subset='hash')

        dropar = casos_raw.loc[casos_raw['data_liberacao'].isnull()]
        dropar.to_excel(join(self.errorspath,'casos_raw_sem_liberacao.xlsx'))
        print(f"casos novos sem diagnóstico: {dropar.shape[0]}")
        # casos_raw = casos_raw.drop(index=dropar.index)

        dropar = casos_raw.loc[casos_raw['nome_exame'].isnull()]
        dropar.to_excel(join(self.errorspath,'casos_raw_sem_nome_exame.xlsx'))
        print(f"casos novos sem laboratório: {dropar.shape[0]}")
        # casos_raw = casos_raw.drop(index=dropar.index)

        dropar = casos_raw.loc[casos_raw['sexo'].isnull()]
        print(f"casos novos sem sexo: {dropar.shape[0]}")
        dropar.to_excel(join(self.errorspath,'casos_raw_sem_sexo.xlsx'))
        casos_raw = casos_raw.drop(index=dropar.index)

        dropar = casos_raw.loc[casos_raw['data_liberacao'] > datetime.today()]
        print(f"casos novos com data_liberacao no futuro: {dropar.shape[0]}")
        dropar.to_excel(join(self.errorspath,'casos_data_liberacao_futuro.xlsx'))
        casos_raw = casos_raw.drop(index=dropar.index)

        dropar = casos_raw.loc[casos_raw['data_liberacao'] < datetime.strptime('12/03/2020','%d/%m/%Y')]
        print(f"casos novos com data_liberacao antes de 2020: {dropar.shape[0]}")
        dropar.to_excel(join(self.errorspath,'casos_data_liberacao_passado.xlsx'))
        casos_raw = casos_raw.drop(index=dropar.index)

        print(f"casos novos validos: {casos_raw.shape[0]}")

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

        casos_raw.loc[(casos_raw['rs']=='99') & (casos_raw['mun_resid'].notnull()), 'mun_resid'] = casos_raw.loc[(casos_raw['rs']=='99') & (casos_raw['mun_resid'].notnull()), 'mun_resid'] + '/' + casos_raw.loc[(casos_raw['rs']=='99') & (casos_raw['mun_resid'].notnull()), 'uf_resid']
        casos_raw['data_com'] = date.today()

        casos_raw['ibge_residencia'] = casos_raw.apply( lambda row: row['ibge_residencia'] if row['uf_residencia'] == 41 else '999999', axis=1)

        novos_casos = casos_raw[['id','ibge_residencia','rs','ibge_unidade_notifica','paciente','sexo','idade','mun_resid', 'mun_atend', 'nome_exame','data_liberacao','data_com','data_1o_sintomas','hash']]

        # novos_casos.loc[~novos_casos['evolucao'].isin([1,2]),'evolucao'] = None
        # novos_casos.loc[novos_casos['evolucao']==1,'evolucao'] = 'CURA'
        # novos_casos.loc[novos_casos['evolucao']==2,'evolucao'] = 'OBITO'

        novos_casos.to_excel(join('output','novos_casos.xlsx'), index=False)

        return novos_casos

    def novos_obitos(self, novos_casos, obitos_raw):
        casos_confirmados =  self.__source['casos']
        obitos_confirmados = self.__source['obitos']
        obitos_raw = obitos_raw.sort_values(by='paciente')

        obitos_curitiba = pd.read_excel(join(dirname(__root__),'tmp','obitos_curitiba.xlsx'))

        obitos_curitiba['paciente'] = obitos_curitiba['paciente'].apply(lambda x: trim_overspace(normalize_text(x)))
        obitos_curitiba['mun_resid'] = obitos_curitiba['mun_resid'].apply(lambda x: trim_overspace(normalize_text(x)))
        obitos_curitiba['idade'] = obitos_curitiba['idade'].apply(lambda x: normalize_number(x,fill=0))

        obitos_curitiba['rs'] = obitos_curitiba['rs'].apply(lambda x: normalize_number(x,fill='99'))
        obitos_curitiba['rs'] = obitos_curitiba['rs'].apply(lambda x: str(x).zfill(2))

        obitos_curitiba['hash'] = obitos_curitiba.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        obitos_curitiba['hash_less'] = obitos_curitiba.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
        obitos_curitiba['hash_more'] = obitos_curitiba.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

        print(f"obitos novos notifica {obitos_raw.shape[0]} + {obitos_curitiba.shape[0]} curitiba\n")

        obitos_raw = obitos_raw.append(obitos_curitiba, ignore_index=True)

        dropar = obitos_raw.loc[obitos_raw['data_cura_obito'] > datetime.today()]
        print(f"obitos novos com data no futuro: {dropar.shape[0]}")
        dropar.to_excel(join(self.errorspath,'obitos_raw_futuro.xlsx'))
        obitos_raw = obitos_raw.drop(index=dropar.index)


        dropar = obitos_raw.loc[obitos_raw['data_cura_obito'] < datetime.strptime('12/03/2020','%d/%m/%Y')]
        print(f"obitos novos com data no passado: {dropar.shape[0]}")
        dropar.to_excel(join(self.errorspath,'obitos_raw_passado.xlsx'))
        obitos_raw = obitos_raw.drop(index=dropar.index)

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

        obitos_raw.loc[(obitos_raw['rs']=='99') & (obitos_raw['mun_resid'].notnull()), 'mun_resid'] = obitos_raw.loc[(obitos_raw['rs']=='99') & (obitos_raw['mun_resid'].notnull()), 'mun_resid'] + '/' + obitos_raw.loc[(obitos_raw['rs']=='99') & (obitos_raw['mun_resid'].notnull()), 'uf_resid']
        obitos_raw.loc[obitos_raw['ibge_residencia'].isnull(), 'ibge_residencia'] = obitos_raw.loc[obitos_raw['ibge_residencia'].isnull()].apply( lambda row: static.municipios.loc[(static.municipios['cod_uf']==41) & (static.municipios['municipio']==row['mun_resid']),'ibge'], axis=1)
        obitos_raw['ibge_residencia'] = obitos_raw.apply( lambda row: row['ibge_residencia'] if row['uf_residencia'] == 41 else '999999', axis=1)


        print(f"{len(obitos_raw) - len(obitos_raw.loc[obitos_raw['hash'].isin(obitos_curitiba['hash'])])} do notifica e {len(obitos_raw.loc[obitos_raw['hash'].isin(obitos_curitiba['hash'])])} de Curitiba serão adicionados\n")

        obitos_raw['data_com_evolucao'] = date.today()
        novos_obitos = obitos_raw[['id','ibge_residencia','rs','paciente','sexo','idade','mun_resid','data_cura_obito','data_com_evolucao','hash']]

        novos_obitos.to_excel(join('output','novos_obitos.xlsx'), index=False)

        return novos_obitos


    def relatorio(self, novos_casos, novos_obitos):
        casos_confirmados =  self.get_casos()

        casos_confirmadosPR = casos_confirmados.loc[casos_confirmados['rs']!='99']

        obitos_confirmados =  self.get_obitos()

        obitos_confirmadosPR = obitos_confirmados.loc[obitos_confirmados['rs']!='99']

        print(f"Total de casos: {len(casos_confirmados)} + {len(novos_casos)}")
        print(f"Total de obitos: {len(obitos_confirmados)} + {len(novos_obitos)}\n\n")

        novos_casosPR = novos_casos.loc[novos_casos['rs']!='99'].copy()
        print(f"Total de casos PR: {len(casos_confirmadosPR)} + {len(novos_casosPR)}")

        novos_obitosPR = novos_obitos.loc[novos_obitos['rs']!='99'].copy()
        print(f"Total de obitos PR: {len(obitos_confirmadosPR)} + {len(novos_obitosPR)}")

        novos_casosFora = novos_casos.loc[novos_casos['rs']=='99'].copy()
        print(f"Total de casos Fora: {len(casos_confirmados) - len(casos_confirmadosPR)} + {len(novos_casosFora)}")

        novos_obitosFora = novos_obitos.loc[novos_obitos['rs']=='99'].copy()
        print(f"Total de obitos Fora: {len(obitos_confirmados) - len(obitos_confirmadosPR)} + {len(novos_obitosFora)}")

        novos_obitosPR_group = novos_obitosPR.groupby(by='mun_resid')

        today = datetime.today()
        ontem = today - timedelta(1)
        data_retroativos = ontem - timedelta(1)

        retroativos = novos_casosPR.loc[(novos_casosPR['data_liberacao'] <= data_retroativos)].sort_values(by='data_liberacao')

        with codecs.open(join('output','relatorios',f"relatorio_{(today.strftime('%d/%m/%Y_%Hh').replace('/','_').replace(' ',''))}.txt"),"w","utf-8-sig") as relatorio:
            relatorio.write(f"{today.strftime('%d/%m/%Y - %Hh%M')}\n")
            relatorio.write(f"{len(novos_casosPR):,} novos casos residentes ".replace(',','.'))

            if len(novos_casosFora) > 0:
                relatorio.write(f"e {len(novos_casosFora):,} não residente{'s' if len(novos_casosFora) > 1 else ''} ".replace(',','.'))
            relatorio.write(f"divulgados no PR.\n")

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

                relatorio.write(f"Total divulgados:\n")
                relatorio.write(f"{len(novos_casosPR):,} novos casos residentes ".replace(',','.'))

                if len(novos_casosFora) > 0:
                    relatorio.write(f"e {len(novos_casosFora):,} não residente{'s' if len(novos_casosFora) > 1 else ''} ".replace(',','.'))
                relatorio.write(f"divulgados no PR.\n")

                relatorio.write(f"{len(retroativos)} casos retroativos confirmados no período de {retroativos.iloc[0]['data_liberacao'].strftime('%d/%m/%Y')} à {retroativos.iloc[-1]['data_liberacao'].strftime('%d/%m/%Y')}.\n")
                relatorio.write(f"{len(novos_casosPR.loc[(novos_casosPR['data_liberacao'] > data_retroativos)])} novos casos confirmados na data de hoje.\n\n")


                novos_casosPR['month'] = novos_casosPR.apply(lambda x: x['data_liberacao'].month, axis=1)
                novos_casosPR['year'] = novos_casosPR.apply(lambda x: x['data_liberacao'].year, axis=1)
                relatorio.write('Novos casos por meses:\n')
                for group, value in novos_casosPR.groupby(by=['year','month']):
                    print(group)
                    relatorio.write(f"{static.meses[int(group[1])-1]}/{group[0]}: {len(value)}\n")
                relatorio.write('\n')

                relatorio.write('Novos obitos por meses:\n')
                novos_obitosPR['month'] = novos_obitosPR.apply(lambda x: x['data_cura_obito'].month, axis=1)
                novos_obitosPR['year'] = novos_obitosPR.apply(lambda x: x['data_cura_obito'].year, axis=1)
                for group, value in novos_obitosPR.groupby(by=['year','month']):
                    relatorio.write(f"{static.meses[int(group[1])-1]}/{group[0]}: {len(value)}\n")
                relatorio.write('\n')

                relatorio.write('Novos obitos por dia:\n')
                for group, value in novos_obitosPR.groupby(by='data_cura_obito'):
                    relatorio.write(f"{group.strftime('%d/%m/%Y')}: {len(value)}\n")

        with codecs.open(join('output','relatorios',f"relatorio_{(today.strftime('%d/%m/%Y_%Hh').replace('/','_').replace(' ',''))}.txt"),"r","utf-8-sig") as relatorio:
            print("\nrelatorio:\n")
            print(relatorio.read())

    def update(self):
        print(f"Atualizando o arquivo {self.database} com o {self.pathfile}...")

        casos = pd.read_excel(self.pathfile,'Casos confirmados',dtype={'ibge_resid': str, 'ibge_atend': str, 'rs': str})

        print(f"Casos excluidos: {len(casos.loc[casos['excluir'] == 'SIM'])}")
        casos = casos.loc[casos['excluir'] != 'SIM']

        casos['hash'] = casos.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        casos['hash_less'] = casos.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
        casos['hash_more'] = casos.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

        obitos = pd.read_excel(self.pathfile, 'Obitos',dtype={'ibge_resid': str, 'ibge_atend': str, 'rs': str})

        print(f"Obitos excluidos: {len(obitos.loc[obitos['excluir'] == 'SIM'])}")
        obitos = obitos.loc[obitos['excluir'] != 'SIM']

        obitos['hash'] = obitos.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade'])+normalize_hash(row['mun_resid']), axis=1)
        obitos['hash_less'] = obitos.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']-1)+normalize_hash(row['mun_resid']), axis=1)
        obitos['hash_more'] = obitos.apply(lambda row: normalize_hash(row['paciente'])+str(row['idade']+1)+normalize_hash(row['mun_resid']), axis=1)

        casos.to_pickle(self.database['casos'])
        obitos.to_pickle(self.database['obitos'])

        with open(self.checksum_file, "w") as checksum:
            checksum.write(self.checksum)

        print(f"{self.database} salvo e {self.checksum_file} atualizado")

        self.__source = { 'casos': casos, 'obitos': obitos }

    def get_casos(self):
        try:
            return self.__source['casos'].copy()
        except:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")

    def get_obitos(self):
        try:
            return self.__source['obitos'].copy()
        except:
            exit("Fonte de dados não encontrada, primeiro utilize o método update")
    #
    # def get_recuperados(self):
    #     try:
    #         return self.__source.loc[self.__source['evolucao'] == 1].copy()
    #     except e:
    #         exit("Fonte de dados não encontrada, primeiro utilize o método update")
    #
    # def get_casos_ativos(self):
    #     try:
    #         return self.__source.loc[self.__source['evolucao'] == 3].copy()
    #     except e:
    #         exit("Fonte de dados não encontrada, primeiro utilize o método update")
    #
    # def get_obitos_nao_covid(self):
    #     try:
    #         return self.__source.loc[self.__source['evolucao'] == 4].copy()
    #     except e:
    #         exit("Fonte de dados não encontrada, primeiro utilize o método update")
