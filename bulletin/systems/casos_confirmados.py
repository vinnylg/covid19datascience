import re
import glob
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from os.path import join

import logging
logger = logging.getLogger(__name__)

from bulletin import data_comeco_pandemia, default_input, hoje, menor_data_nascimento, root, tables_path
from bulletin.utils import utils
from bulletin.utils.timer import Timer
from bulletin.utils.static import Municipios
from bulletin.utils.normalize import date_hash, normalize_hash, normalize_labels, normalize_number, normalize_text
from bulletin.systems import System


def group_dados_fontes_3(df, df_columns, datetype):
    df = df[df_columns+['identificacao']].groupby(df_columns).count().rename(columns={'identificacao':'qtde'}).reset_index()
    df['Tipo_Data'] = datetype
    df.columns = pd.Index(['IBGE_RES_PR', 'data', 'qtde', 'Tipo_Data'])
    df = df[['IBGE_RES_PR', 'Tipo_Data', 'data', 'qtde']]
    df['data'] = df['data'].dt.date
    df['IBGE_RES_PR'] = df['IBGE_RES_PR'].astype('str')

    return df


class CasosConfirmados(System):

    def __init__(self,database:str=f"cc_{hoje.strftime('%d_%m_%Y')}"):
        super().__init__('casos_confirmados',database)

    def __len__(self):
        return len(self.df)

    def __str__(self):
        return self.database
    
    def download_all(self):
        raise NotImplemented()

    def download_update():
        raise NotImplemented()


    def update(self, new_notifica, observed_cols=None, replace=True):

        if observed_cols is None:
            observed_cols = set(self.df.columns.tolist()).intersection(new_notifica.df.columns.tolist())
    
        observed_cols = list(observed_cols)
        observed_cols.pop(observed_cols.index('id'))
        
        criterios_novos_casos = ( 
            (~new_notifica.df['id'].isin(self.df['id'])) & 
            (new_notifica.df['excluir_ficha'] == 2) &
            (new_notifica.df['status_notificacao'].isin([1,2])) &
            (new_notifica.df['classificacao_final'] == 2)
        )

        novas_notificacoes = new_notifica.df.loc[criterios_novos_casos].set_index('id')
        #!TODO filtrar duplicados

        logger.info(f"novas_notificacoes {len(novas_notificacoes)}")

        possiveis_alterados = self.df.loc[self.df['id'].isin(new_notifica.df['id'])]
        possiveis_atualizacoes = new_notifica.df.loc[new_notifica.df['id'].isin(self.df['id'])]

        mergedf = pd.merge(possiveis_alterados[observed_cols+['tipo_encerramento','id']],possiveis_atualizacoes[observed_cols+['id']],on='id',how='left',suffixes=['_old','_new'])
        mergedf['updated'] = False
        mergedf['updated_cols'] = ''

        for col in observed_cols:
            mergedf.loc[(mergedf[f'{col}_new'].notna()) & (mergedf[f'{col}_old'])!=(mergedf[f'{col}_new']), 'updated'] = True
            mergedf.loc[(mergedf[f'{col}_new'].notna()) & (mergedf[f'{col}_old'])!=(mergedf[f'{col}_new']), 'updated_cols'] = mergedf.loc[(mergedf[f'{col}_old'])!=(mergedf[f'{col}_new']), 'updated_cols'].apply(lambda l: utils.strlist(l,col))

        mergedf = mergedf.loc[mergedf['updated']==True]
        atualizacoes = possiveis_atualizacoes.loc[possiveis_atualizacoes['id'].isin(mergedf['id'])]
        
        ## filtrar não atualizaveis e atualizaveis
        # não pegar registros com tipo_encerramento automatico que foram evoluidos para óbito ou cura até que o notifica fique igual
        evolucao_automatica_nao_corrigida_notifica = mergedf.loc[mergedf['tipo_encerramento'].isin(['E2','E3']) & mergedf['evolucao_old']!=mergedf['evolucao_new'],['id','evolucao_old']].copy()
        # remove evolucao da lista de colunas atualizadas nesses casos
        mergedf.loc[mergedf['id'].isin(evolucao_automatica_nao_corrigida_notifica['id']),'updated_cols'] = mergedf.loc[mergedf['id'].isin(evolucao_automatica_nao_corrigida_notifica['id']),'updated_cols'].str.replace(',evolucao','')
        # prepara dataframe para atualizar a evolucao incorreta no dataframe de atualizacoes
        evolucao_automatica_nao_corrigida_notifica = evolucao_automatica_nao_corrigida_notifica.rename(columns={'evolucao_old':'evolucao'})
        evolucao_automatica_nao_corrigida_notifica = evolucao_automatica_nao_corrigida_notifica.set_index('id')

        # corrige tipo_encerramento de notificacoes que foram corrigidas no notifica
        evolucao_automatica_corrigda_notifica = mergedf.loc[mergedf['tipo_encerramento'].isin(['E2','E3']) & (mergedf['evolucao_old']==mergedf['evolucao_new'])]
        atualizacoes = atualizacoes.loc[atualizacoes['id'].isin(evolucao_automatica_corrigda_notifica['id']),'tipo_encerramento'] = 'E1'

        # atualiza a evolucao incorreta no dataframe de atualizacoes
        atualizacoes = atualizacoes.set_index('id')
        atualizacoes.update(evolucao_automatica_nao_corrigida_notifica)

        logger.info(f"atualizacoes {len(atualizacoes)}")
        
        if replace:
            #seta index nos casos confirmados, adiciona os novos e atualiza casos atualizaveis
            self.df = self.df.set_index('id')
            self.df = self.df.append(novas_notificacoes)
            self.df.update(atualizacoes)
            #retorna index casos confirmados
            self.df.reset_index(inplace=True)

        mergedf.to_pickle(join(self.database_dir,f"updates_{hoje.strftime('%d_%m_%Y_%H%M')}.pkl"),'bz2')

        return novas_notificacoes, mergedf

    def comunicacao_casosObitos(self, df_casos):
        df_casos_PR = df_casos.loc[df_casos['uf_residencia'] == 'PR']

        used_columns = ['ibge7_resid', 'ibge_residencia', 'rs', 'macro', 'municipio', 'evolucao']
        df_casos_PR = df_casos_PR.drop(columns=[col for col in df_casos_PR.columns if not col in used_columns])
      
        casos_obitos = df_casos_PR.rename(columns={'ibge7_resid':'IBGE7', 'ibge_residencia':'IBGE6'})


        obitos_count = casos_obitos.loc[casos_obitos['evolucao'] == 2]
        recuperados_count = casos_obitos.loc[casos_obitos['evolucao'] == 1]
        outrasCausas_count = casos_obitos.loc[casos_obitos['evolucao'] == 4]
        ativos_count = casos_obitos.loc[casos_obitos['evolucao'] == 3]
        

        #contagens
        casos_count = casos_obitos[['IBGE7', 'evolucao']].groupby(['IBGE7']).count().reset_index().rename(columns={'evolucao':'CASOS'})
        obitos_count = obitos_count[['IBGE7', 'evolucao']].groupby(['IBGE7']).count().reset_index().rename(columns={'evolucao':'ÓBITOS POR COVID-19'})
        recuperados_count = recuperados_count[['IBGE7', 'evolucao']].groupby(['IBGE7']).count().reset_index().rename(columns={'evolucao':'RECUPERADOS'})
        outrasCausas_count = outrasCausas_count[['IBGE7', 'evolucao']].groupby(['IBGE7']).count().reset_index().rename(columns={'evolucao':'ÓBITOS POR OUTRAS CAUSAS'})
        ativos_count = ativos_count[['IBGE7', 'evolucao']].groupby(['IBGE7']).count().reset_index().rename(columns={'evolucao':'ATIVOS'})
        adicionar_gal = pd.read_csv(join(default_input, 'Adicionar_GAL.csv'), sep = ';', usecols=['IBGE', 'Em_investigacao']).rename(columns={'IBGE':'IBGE6', 'Em_investigacao':'EM INVESTIGAÇÃO'})


        #merges
        casos_obitos = casos_obitos.drop_duplicates(subset=['IBGE7'], keep='first')
        casos_obitos = pd.merge(left=casos_obitos, right=casos_count,  how='left', on=['IBGE7'])
        casos_obitos = pd.merge(left=casos_obitos, right=obitos_count,  how='left', on=['IBGE7'])
        casos_obitos = pd.merge(left=casos_obitos, right=outrasCausas_count,  how='left', on=['IBGE7'])
        casos_obitos = pd.merge(left=casos_obitos, right=recuperados_count,  how='left', on=['IBGE7'])
        casos_obitos = pd.merge(left=casos_obitos, right=ativos_count,  how='left', on=['IBGE7'])
        casos_obitos = pd.merge(left=casos_obitos, right=adicionar_gal,  how='left', on=['IBGE6'])


        #ajustes
        casos_obitos = casos_obitos.drop(columns=['IBGE6', 'evolucao'])
        casos_obitos = casos_obitos.rename(columns={'IBGE7':'IBGE', 'rs':'RS', 'macro':'MACRO', 'municipio':'MUNICÍPIO'})
        casos_obitos['MACRO'] = casos_obitos['MACRO'].str.upper()
        casos_obitos['ÓBITOS POR COVID-19'] = casos_obitos['ÓBITOS POR COVID-19'].fillna(0).astype('int')
        casos_obitos['ÓBITOS POR OUTRAS CAUSAS'] = casos_obitos['ÓBITOS POR OUTRAS CAUSAS'].fillna(0).astype('int')
        casos_obitos['EM INVESTIGAÇÃO'] = casos_obitos['EM INVESTIGAÇÃO'].fillna(0).astype('int')
        casos_obitos = casos_obitos.sort_values(['RS', 'MUNICÍPIO'], ascending=True)


        #salvar em CSV
        # casos_obitos.to_csv(join(default_output, 'comunicacao',  f"INFORME_EPIDEMIOLOGICO_{str(hoje.day).zfill(2)}_{str(hoje.month).zfill(2)}_{hoje.year}_OBITOS_CASOS_MUNICIPIO.csv"), index=False, sep=';',encoding='utf-8-sig')

        return casos_obitos

    def comunicacao_csvGeralNew(self):
        df_casos = self.df.copy()

        #DROP das colunas que não serão utilizadas
        notificacao_schema = pd.read_csv(join(root,'systems','information_schema.csv'))
        used_columns = notificacao_schema.loc[notificacao_schema['csv_geral']==1]
        self.df = self.df.drop(columns=[col for col in self.df.columns if not col in used_columns['column'].values])


        #REPLACE das colunas que serão utilizadas
        replace_columns = self.schema.loc[self.schema['csv_geral_replace']==1]
        for j in replace_columns['column']:
            self.replace(j, inplace=True)


        municipios = Municipios()
        municipios['rs'] = municipios['rs'].astype('str').apply(lambda x: x.zfill(2))
        municipios['mun_resid'] = municipios['municipio'].apply(normalize_text)     
        municipios.loc[municipios['uf']!='PR','mun_resid'] += '/' + municipios['uf']


        self.df = pd.merge(self.df.rename(columns={'ibge_residencia':'ibge'}),municipios[['ibge','ibge7','rs','municipio', 'macro']],on='ibge',how='left').rename(columns={'ibge':'ibge_residencia', 'ibge7':'ibge7_residencia', 'municipio':'municipio_residencia','rs':'rs_residencia', 'macro':'macro_residencia'})
        self.df = pd.merge(self.df.rename(columns={'ibge_unidade_notifica':'ibge'}),municipios[['ibge','ibge7','municipio']].rename(columns={'municipio':'mun_atend'}),on='ibge',how='left').rename(columns={'ibge':'ibge_unidade_notifica', 'ibge7':'ibge7_unidade_notifica', 'mun_atend':'municipio_atendimento'})
      
        #ajustes gerais
        self.df.loc[self.df['reinfeccao'] == False, 'reinfeccao'] = None
        self.df.loc[self.df['reinfeccao'] == True, 'reinfeccao'] = 'Provável caso de REINFECÇÃO'

        self.df.loc[self.df['evolucao'] == 'Não se aplica', 'evolucao'] = 'Ativo'
        

        csv_geral = self.df.sort_values(['data_cura_obito', 'data_comunicacao'], ascending=False)


        self.df = df_casos
        return csv_geral
        

    def comunicacao_csvGeralOld(self, df_casos):    
        # ------------------------------------------------------------------- CSV: GERAL
        used_columns = ['ibge7_resid', 'ibge7_atend', 'uf_residencia', 'sexo', 'idade', 'mun_resid', 'mun_atend', 'exame', 'data_diagnostico', 'data_comunicacao', 'data_1o_sintomas', 'evolucao', 'data_cura_obito', 'data_com_evolucao', 'origem_nome']
        csv_geral = df_casos.drop(columns=[col for col in df_casos.columns if not col in used_columns])

        #criação de novas colunas
        csv_geral.loc[csv_geral['evolucao'] == 1, ('ÓBITO COVID-19', 'STATUS')] = ('NÃO', 'Recuperado')
        csv_geral.loc[csv_geral['evolucao'] == 2, ('ÓBITO COVID-19', 'STATUS')] = ('SIM', 'Óbito por COVID-19')
        csv_geral.loc[csv_geral['evolucao'] == 3, ('ÓBITO COVID-19', 'STATUS')] = ('NÃO', 'ATIVO')
        csv_geral.loc[csv_geral['evolucao'] == 4, ('ÓBITO COVID-19', 'STATUS')] = ('NÃO', 'Óbito por Outras Causas')



        #separação dos dados
        cura = csv_geral.loc[csv_geral['evolucao'] == 1]
        obito = csv_geral.loc[csv_geral['evolucao'].isin([2,4])]
        nao_aplica = csv_geral.loc[csv_geral['evolucao'] == 3]


        cura = cura.rename(columns={'data_com_evolucao':'DATA_RECUPERADO_DIVULGACAO'})
        obito = obito.rename(columns={'data_cura_obito':'DATA_OBITO','data_com_evolucao':'DATA_OBITO_DIVULGACAO'})


        #concatenar os dados que foram criados
        csv_geral = pd.DataFrame()
        csv_geral = pd.concat([cura,obito,nao_aplica])


        #ajustes
        csv_geral = csv_geral[['ibge7_resid', 'ibge7_atend', 'uf_residencia','sexo', 'idade', 'mun_resid', 'mun_atend', 'exame', 'data_diagnostico', 'data_comunicacao', 'data_1o_sintomas', 'ÓBITO COVID-19', 'DATA_OBITO', 'DATA_OBITO_DIVULGACAO', 'STATUS', 'DATA_RECUPERADO_DIVULGACAO', 'origem_nome']]
        csv_geral = csv_geral.rename(columns={
                                'ibge7_resid':'IBGE_RES_PR', 
                                'ibge7_atend':'IBGE_ATEND_PR',
                                'uf_residencia':'UF_RESIDENCIA',
                                'sexo':'SEXO',
                                'idade':'IDADE_ORIGINAL',
                                'mun_resid':'MUN_RESIDENCIA', 
                                'mun_atend':'MUN_ATENDIMENTO',
                                'exame':'EXAME',
                                'data_diagnostico':'DATA_DIAGNOSTICO',
                                'data_comunicacao':'DATA_CONFIRMACAO_DIVULGACAO',
                                'data_1o_sintomas':'DATA_INICIO_SINTOMAS',
                                'origem_nome':'ORIGEM_NOTIFICACAO'})

        #ajuste de datas
        csv_geral['DATA_DIAGNOSTICO'] = csv_geral['DATA_DIAGNOSTICO'].dt.date
        csv_geral['DATA_CONFIRMACAO_DIVULGACAO'] = csv_geral['DATA_CONFIRMACAO_DIVULGACAO'].dt.date
        csv_geral['DATA_INICIO_SINTOMAS'] = csv_geral['DATA_INICIO_SINTOMAS'].dt.date
        csv_geral['DATA_OBITO'] = csv_geral['DATA_OBITO'].dt.date
        csv_geral['DATA_OBITO_DIVULGACAO'] = csv_geral['DATA_OBITO_DIVULGACAO'].dt.date
        csv_geral['DATA_RECUPERADO_DIVULGACAO'] = csv_geral['DATA_RECUPERADO_DIVULGACAO'].dt.date


        csv_geral = csv_geral.sort_values(['DATA_OBITO_DIVULGACAO', 'DATA_CONFIRMACAO_DIVULGACAO'], ascending=False)

        #SALVAR EM CSV
        # csv_geral.to_csv(join(default_output, 'comunicacao', f"INFORME_EPIDEMIOLOGICO_{str(hoje.day).zfill(2)}_{str(hoje.month).zfill(2)}_{hoje.year}_GERAL.csv"), index=False, sep=';', encoding='utf-8-sig')

        
        # ------------------------------------------------------------------- CONFERÊNCIA E RECORDES
        casos_pr = csv_geral.loc[csv_geral['UF_RESIDENCIA'] == 'PR']
        obitos_pr = casos_pr.loc[casos_pr['ÓBITO COVID-19'] == 'SIM']


        logger.info('------------------- CONFERÊNCIA -------------------')
        logger.info(("TOTAL DE CASOS NO PARANÁ = ", re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(len(casos_pr)))))
        logger.info(("TOTAL GERAL DE CASOS = ", re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(len(csv_geral)))))
        logger.info('\n')
        logger.info(("TOTAL DE ÓBITOS NO PARANÁ = ", re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(len(obitos_pr)))))
        logger.info(("TOTAL GERAL DE ÓBITOS = ", re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(csv_geral['ÓBITO COVID-19'].value_counts()['SIM']))))
        logger.info('\n\n')


        casos_divulgados = casos_pr[['SEXO', 'DATA_CONFIRMACAO_DIVULGACAO']].groupby('DATA_CONFIRMACAO_DIVULGACAO').count().reset_index().rename(columns={'SEXO':'QTDE'}).sort_values('QTDE')
        casos_divulgados = casos_divulgados.iloc[-1]

        casos_diagnosticados = casos_pr[['SEXO', 'DATA_DIAGNOSTICO']].groupby('DATA_DIAGNOSTICO').count().reset_index().rename(columns={'SEXO':'QTDE'}).sort_values('QTDE')
        casos_diagnosticados = casos_diagnosticados.iloc[-1]

        obitos_divulgados = obitos_pr[['SEXO', 'DATA_OBITO_DIVULGACAO']].groupby('DATA_OBITO_DIVULGACAO').count().reset_index().rename(columns={'SEXO':'QTDE'}).sort_values('QTDE')
        obitos_divulgados = obitos_divulgados.iloc[-1]

        obitos_data = obitos_pr[['SEXO', 'DATA_OBITO']].groupby('DATA_OBITO').count().reset_index().rename(columns={'SEXO':'QTDE'}).sort_values('QTDE')
        obitos_data = obitos_data.iloc[-1]

        casos_divulgados['DATA_CONFIRMACAO_DIVULGACAO'] = str(casos_divulgados['DATA_CONFIRMACAO_DIVULGACAO'])
        casos_diagnosticados['DATA_DIAGNOSTICO'] = str(casos_diagnosticados['DATA_DIAGNOSTICO'])
        obitos_divulgados['DATA_OBITO_DIVULGACAO'] = str(obitos_divulgados['DATA_OBITO_DIVULGACAO'])
        obitos_data['DATA_OBITO'] = str(obitos_data['DATA_OBITO'])

        # logger.info('------------ RECORDES DE CASOS E ÓBITOS (RESIDENTES NO PARANÁ) ------------')
        # logger.info('----------------- CASOS')
        # logger.info('--  pela data de DIVULGAÇÃO do caso')
        # logger.info('DATA: ', casos_divulgados['DATA_CONFIRMACAO_DIVULGACAO'][:10], '    QTDE: ', re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(casos_divulgados['QTDE'])))
        # logger.info('--  pela data de DIAGNÓSTICO do caso')
        # logger.info('DATA: ', casos_diagnosticados['DATA_DIAGNOSTICO'][:10], '    QTDE: ', re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(casos_diagnosticados['QTDE'])))
        # logger.info('\n')
        # logger.info('----------------- ÓBITOS')
        # logger.info('--  pela data de DIVULGAÇÃO do óbito')
        # logger.info('DATA: ', obitos_divulgados['DATA_OBITO_DIVULGACAO'][:10], '    QTDE: ', re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(obitos_divulgados['QTDE'])))
        # logger.info('--  pela DATA DO ÓBITO')
        # logger.info('DATA: ', obitos_data['DATA_OBITO'][:10], '    QTDE: ', re.sub(r'(?<!^)(?=(\d{3})+$)', r'.', str(obitos_data['QTDE'])))
        # logger.info('\n')
        # logger.info('FONTE: ', f"INFORME_EPIDEMIOLOGICO_{str(hoje.day).zfill(2)}_{str(hoje.month).zfill(2)}_{hoje.year}_Geral.csv")


        return csv_geral


    def parser_dados_fontes(self):
        municipios = Municipios()
        municipios['rs'] = municipios['rs'].astype('str').apply(lambda x: x.zfill(2))
        municipios['macro'] = municipios['macro'].str.upper()

        dados_fontes = pd.merge(self.df.rename(columns={'ibge_residencia':'ibge'}),municipios,on=['ibge'],how='left').rename(columns={'ibge':'ibge_residencia'})

        dados_fontes['municipio_pr'] = dados_fontes['municipio']
        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','municipio_pr'] = 'Fora do Parana'
        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','ibge7'] = 9999999

        dados_fontes['uf_pr'] = 'PR'
        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','uf_pr'] = 'Fora do Parana'
        
        dados_fontes['obito'] = ''
        dados_fontes['status'] = ''
        dados_fontes.loc[dados_fontes['evolucao']==2, 'obito'] = 'SIM'
        dados_fontes.loc[dados_fontes['evolucao']==1, 'status'] = 'Recuperado'
        dados_fontes.loc[dados_fontes['evolucao']==3, 'status'] = 'Ativo'
        dados_fontes.loc[dados_fontes['evolucao']==4, 'status'] = 'Óbito por outras causas'

        return dados_fontes

    def dados_fontes(self):
        dados_fontes = self.parser_dados_fontes()

        cols = ['macro','rs','ibge7','municipio_pr','obito','status']

        dados_fontes = dados_fontes[cols+['identificacao']].groupby(cols).count().rename(columns={'identificacao':'qtde'}).reset_index()
        dados_fontes.loc[dados_fontes['macro']=='FORA', 'macro'] = None
        dados_fontes.loc[dados_fontes['rs']=='99', 'rs'] = None
        
        municipios = Municipios()
        municipios['rs'] = municipios['rs'].astype('str').apply(lambda x: x.zfill(2))
        municipios['macro'] = municipios['macro'].str.upper()
        municipios = municipios[['ibge', 'ibge7', 'rs', 'macro', 'municipio']]
        
        gal = pd.read_csv(join(default_input,'Adicionar_GAL.csv'), sep = ';', encoding = 'utf-8-sig')
        gal.columns = [ normalize_labels(x) for x in gal.columns ]
        del gal['municipio']
        gal = pd.merge(gal,municipios,on='ibge',how='left')
        gal = gal[['ibge7', 'macro', 'municipio', 'rs', 'em_investigacao']]
        gal['status'] = 'Em investigacao'
        gal['obito'] = ''
        gal = gal[['macro', 'rs', 'ibge7', 'municipio', 'obito', 'status', 'em_investigacao']]
        gal.loc[gal['macro']=='FORA', 'ibge7'] = 9999999
        fora_pr = pd.DataFrame(gal.loc[(gal['ibge7'] == 9999999) & (gal['status'] == 'Em investigacao')].sum(0), columns=['total']).transpose()
        gal = gal.loc[gal['macro']!='FORA']
        fora_pr.loc[fora_pr.index.values==['total'], 'macro'] = None
        fora_pr.loc[fora_pr.index.values==['total'], 'rs'] = None
        fora_pr.loc[fora_pr.index.values==['total'], 'ibge7'] = 9999999
        fora_pr.loc[fora_pr.index.values==['total'], 'status'] = 'Em investigacao'
        fora_pr.loc[fora_pr.index.values==['total'], 'municipio'] = 'Fora do Parana'
        fora_pr = fora_pr[['macro', 'rs', 'ibge7', 'municipio', 'obito', 'status', 'em_investigacao']]
        gal = gal.append(fora_pr)
        gal.columns = pd.Index(['MACRO', 'RS', 'IBGE', 'Municipio', 'Obito', 'STATUS', 'Qtde'])
        dados_fontes.columns = pd.Index(['MACRO', 'RS', 'IBGE', 'Municipio', 'Obito', 'STATUS', 'Qtde'])

        return pd.concat([dados_fontes,gal])

    def dados_fontes2(self):
        faixa_etaria = [5,9,19,29,39,49,59,69,79]
        faixa_etaria_labels = ['00-05','06-09','10-19','20-29','30-39','40-49','50-59','60-69','70-79','80 e mais']

        df = self.parser_dados_fontes()

        dd2 = pd.DataFrame(columns=['ibge', 'arquivo', 'qtde'])

        media_idade_casos_PR = df['idade'].mean()
        dd2 = dd2.append(pd.DataFrame([['PR','Media_Casos_PR',media_idade_casos_PR]],columns=dd2.columns), ignore_index=True)

        media_idade_obitos_PR = df.loc[df['evolucao']==2,'idade'].mean()
        dd2 = dd2.append(pd.DataFrame([['PR','Media_OBT_PR',media_idade_obitos_PR]],columns=dd2.columns), ignore_index=True)

        dd2 = dd2.append(
            pd.DataFrame(
                [[
                    'PR',
                    'CASOS_24H',
                    len(
                        self.df.loc[
                            (self.df['uf_residencia']=='PR') &
                            (self.df['data_comunicacao'] == today ) &
                            (self.df['data_diagnostico'] > anteontem )
                        ]
                    )
                ]],
                columns=dd2.columns
            ), 
            ignore_index=True)
        
        dd2 = dd2.append(
            pd.DataFrame(
                [[
                    'PR',
                    'OBITOS_24H',
                    len(
                        self.df.loc[
                            (self.df['uf_residencia']=='PR') &
                            (self.df['evolucao']==2) &
                            (self.df['data_com_evolucao'] == today ) &
                            (self.df['data_cura_obito'] > anteontem )
                        ]
                    )
                ]],
                columns=dd2.columns
            ),
            ignore_index=True)


        dd2 = dd2.append(
            pd.DataFrame(
                [[
                    'GERAL',
                    'REINFECCAO_GERAL',
                    len(self.df.loc[(self.df['reinfeccao']==True) & (self.df['uf_residencia']!='PR')])
                ]],
                columns=dd2.columns
            ),
            ignore_index=True)


        dd2 = dd2.append(
            pd.DataFrame(
                [[
                    'PR',
                    'REINFECCAO_PR',
                    len(self.df.loc[(self.df['reinfeccao']==True) & (self.df['uf_residencia']=='PR')])
                ]],
                columns=dd2.columns
            ),
            ignore_index=True)

        #DADOS DE TESTES RAPIDOS E GAL
        dados_gal = pd.read_csv(join(default_input, 'dados_gal.csv'))
        dd2 = dd2.append(pd.DataFrame([['PR','NOTIFICADOS',dados_gal.iloc[0,0]]],columns=dd2.columns), ignore_index=True)
        dd2 = dd2.append(pd.DataFrame([['PR','REALIZADOS',dados_gal.iloc[1,0]]],columns=dd2.columns), ignore_index=True)
        dd2 = dd2.append(pd.DataFrame([['PR','CONFIRMADOS',dados_gal.iloc[2,0]]],columns=dd2.columns), ignore_index=True)
        dd2 = dd2.append(pd.DataFrame([['PR','NEGATIVOS',dados_gal.iloc[3,0]]],columns=dd2.columns), ignore_index=True)
        dd2 = dd2.append(pd.DataFrame([['PR','INVESTIGACAO',dados_gal.iloc[4,0]]],columns=dd2.columns), ignore_index=True)
        dd2 = dd2.append(pd.DataFrame([['PR','TESTES_RAPIDOS_DIARIO',pd.read_csv(join(default_input, 'queries', 'testes_rapidos_diario.csv')).iloc[0,0]]],columns=dd2.columns), ignore_index=True)

        for ibge, ibge_df in df.groupby('ibge7'):
            dd2 = dd2.append(pd.DataFrame([[ibge,'Casos',len(ibge_df.loc[ibge_df['data_comunicacao']==hoje])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'Obitos',len(ibge_df.loc[(ibge_df['evolucao']==2)&(ibge_df['data_com_evolucao']==hoje)])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'Recuperados',len(ibge_df.loc[(ibge_df['evolucao']==1)&(ibge_df['data_com_evolucao']==hoje)])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'F',len(ibge_df.loc[ibge_df['sexo']=='F'])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'M',len(ibge_df.loc[ibge_df['sexo']=='M'])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'OBT_F',len(ibge_df.loc[(ibge_df['evolucao']==2)&(ibge_df['sexo']=='F')])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'OBT_M',len(ibge_df.loc[(ibge_df['evolucao']==2)&(ibge_df['sexo']=='M')])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'Media_Casos',ibge_df['idade'].mean()]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'Media_OBT',ibge_df.loc[ibge_df['evolucao']==2,'idade'].mean()]],columns=dd2.columns), ignore_index=True)
            
            ibge_df['faixa_etaria'] = [ faixa_etaria_labels[idx] for idx in np.digitize(ibge_df['idade'],faixa_etaria,right=True)]
            
            ibge_fx = ibge_df[['identificacao','faixa_etaria','sexo']].groupby(['sexo','faixa_etaria']).count().reset_index().rename(columns={'identificacao':'qtde'})
            ibge_fx['arquivo'] = ibge_fx['sexo'] + ibge_fx['faixa_etaria']
            ibge_fx['ibge'] = ibge
            ibge_fx = ibge_fx[['ibge','arquivo','qtde']]
            
            dd2 = dd2.append(ibge_fx,ignore_index=True)
            
            ibge_fx = ibge_df.loc[ibge_df['evolucao']==2,['identificacao','faixa_etaria','sexo']].groupby(['sexo','faixa_etaria']).count().reset_index().rename(columns={'identificacao':'qtde'})
            ibge_fx['arquivo'] = 'OBT_' + ibge_fx['sexo'] + '_' + ibge_fx['faixa_etaria']
            ibge_fx['ibge'] = ibge
            ibge_fx = ibge_fx[['ibge','arquivo','qtde']]
            
            dd2 = dd2.append(ibge_fx,ignore_index=True)

        return dd2

    def dados_fontes3(self):
        dados_fontes = self.parser_dados_fontes()

        cols = ['ibge7','data_diagnostico']
        cols = ['ibge7','data_diagnostico']
        dados_fontes_diag_1 = group_dados_fontes_3(dados_fontes.copy(), cols, 'Diagnostico')
        cols = ['rs','data_diagnostico']
        dados_fontes_diag_2 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Diagnostico')
        cols = ['macro','data_diagnostico']
        dados_fontes_diag_3 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Diagnostico')
        cols = ['uf_pr','data_diagnostico']
        dados_fontes_diag_4 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Diagnostico')
        dados_fontes_diag = pd.concat([dados_fontes_diag_1, dados_fontes_diag_2, dados_fontes_diag_3, dados_fontes_diag_4])
        
        cols = ['ibge7','data_cura_obito']
        dados_fontes_obito_1 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['obito']=='SIM'].copy(), cols, 'Obito')
        cols = ['rs','data_cura_obito']
        dados_fontes_obito_2 = group_dados_fontes_3(dados_fontes.loc[((dados_fontes['obito']=='SIM') & (dados_fontes['macro']!='FORA'))].copy(), cols, 'Obito')
        cols = ['macro','data_cura_obito']
        dados_fontes_obito_3 = group_dados_fontes_3(dados_fontes.loc[((dados_fontes['obito']=='SIM') & (dados_fontes['macro']!='FORA'))].copy(), cols, 'Obito')
        cols = ['uf_pr','data_cura_obito']
        dados_fontes_obito_4 = group_dados_fontes_3(dados_fontes.loc[((dados_fontes['obito']=='SIM') & (dados_fontes['macro']!='FORA'))].copy(), cols, 'Obito')
        dados_fontes_obito = pd.concat([dados_fontes_obito_1, dados_fontes_obito_2, dados_fontes_obito_3, dados_fontes_obito_4])
        
        cols = ['ibge7','data_comunicacao']
        dados_fontes_notificacao_1 = group_dados_fontes_3(dados_fontes.copy(), cols, 'Notificacao')
        cols = ['rs','data_comunicacao']
        dados_fontes_notificacao_2 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Notificacao')
        cols = ['macro','data_comunicacao']
        dados_fontes_notificacao_3 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Notificacao')
        cols = ['uf_pr','data_comunicacao']
        dados_fontes_notificacao_4 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Notificacao')
        dados_fontes_notificacao = pd.concat([dados_fontes_notificacao_1, dados_fontes_notificacao_2, dados_fontes_notificacao_3, dados_fontes_notificacao_4])
        
        dados_fontes_is = None
        cols = ['ibge7','data_1o_sintomas']
        dados_fontes_is = dados_fontes
        dados_fontes_is = dados_fontes_is[cols+['identificacao']].groupby(cols).count().rename(columns={'identificacao':'qtde'}).reset_index()
        dados_fontes_is = dados_fontes_is.rename(columns={'ibge7':'IBGE_RES_PR', 'data_1o_sintomas':'data'})
        dados_fontes_is['Tipo_Data'] = 'Inicio_Sintomas'
        dados_fontes_is['data'] = dados_fontes_is['data'].dt.date
        dados_fontes_is = dados_fontes_is[['IBGE_RES_PR', 'Tipo_Data', 'data', 'qtde']]
        dados_fontes_is['IBGE_RES_PR'] = dados_fontes_is['IBGE_RES_PR'].astype('str')
        
        tb_datas = pd.read_excel(join(default_input, 'TB_Datas.xlsx'))
        tb_datas['Data'] = pd.to_datetime(tb_datas['Data'], format = '%d/%m/%Y')
        tb_datas = tb_datas.set_index('Identificação')
        tb_datas['IBGE_RES_PR'] = 'PR'
        tb_datas['Tipo_Data'] = 'ObitoC'
        tb_datas = tb_datas.rename(columns={'Obitos':'qtde', 'Data':'data'})
        dados_fontes_obitoc = tb_datas[['IBGE_RES_PR', 'Tipo_Data', 'data', 'qtde']]
        
        tb_datas['Tipo_Data'] = 'DiagnosticoC'
        tb_datas = tb_datas.rename(columns={'qtde':'Obitos', 'Casos':'qtde'})
        dados_fontes_diagc = tb_datas[['IBGE_RES_PR', 'Tipo_Data', 'data', 'qtde']]
        

        return pd.concat([dados_fontes_diag, dados_fontes_diagc, dados_fontes_obito, dados_fontes_obitoc, dados_fontes_notificacao, dados_fontes_is])