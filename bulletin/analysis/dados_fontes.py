import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from os.path import join

from bulletin import default_input
from bulletin.systems.casos_confirmados import CasosConfirmados
from bulletin.utils.static import Municipios
from bulletin.utils.normalize import normalize_labels

today = datetime.today()
ontem = today - timedelta(1)

def group_dados_fontes_3(df, df_columns, datetype):
    df = df[df_columns+['identificacao']].groupby(df_columns).count().rename(columns={'identificacao':'qtde'}).reset_index()
    df['Tipo_Data'] = datetype
    df.columns = pd.Index(['IBGE_RES_PR', 'data', 'qtde', 'Tipo_Data'])
    df = df[['IBGE_RES_PR', 'Tipo_Data', 'data', 'qtde']]
    #df['data'] = df['data'].dt.strftime('%m/%d/%Y')
    df['data'] = df['data'].dt.date
    df['IBGE_RES_PR'] = df['IBGE_RES_PR'].astype('str')
    
    return df


class DadosFontes:
    def __init__(self, database=f"cc_{today.strftime('%d_%m_%Y')}", compress=True):
        self.database = database
        
        cc = CasosConfirmados()
        cc.load(self.database,compress=compress)
        dados_fontes = cc.df
        
        municipios = Municipios()

        dados_fontes = pd.merge(dados_fontes.rename(columns={'ibge_residencia':'ibge'}),municipios,on=['ibge'],how='left').rename(columns={'ibge':'ibge_residencia'})

        dados_fontes['municipio_pr'] = dados_fontes['municipio']
        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','municipio_pr'] = 'Fora do Parana'

        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','ibge7'] = 9999999

        dados_fontes['uf_pr'] = 'PR'
        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','uf_pr'] = 'Fora do Parana'
        
        dados_fontes['obito'] = ''
        dados_fontes['status'] = ''
        dados_fontes.loc[dados_fontes['evolucao']==2, 'obito'] = 'SIM'
        dados_fontes.loc[dados_fontes['evolucao']==1, 'status'] = 'Recuperado'


    def dados_fontes_1(self):
        mn = Municipios()
        mn['rs'] = mn['rs'].astype('str').apply(lambda x: x.zfill(2))
        mn['macro'] = mn['macro'].str.upper()
        mn = mn[['ibge7', 'rs', 'macro']]

        dados_fontes = dados_fontes.copy()
        del dados_fontes['rs']
        del dados_fontes['macro']
        dados_fontes = dados_fontes.merge(mn, on='ibge7', how='left')
        cols = ['macro','rs','ibge7','municipio_pr','obito','status']
        dados_fontes = dados_fontes[cols+['identificacao']].groupby(cols).count().rename(columns={'identificacao':'qtde'}).reset_index()
        dados_fontes.loc[dados_fontes['macro']=='FORA', 'macro'] = None
        dados_fontes.loc[dados_fontes['rs']=='99', 'rs'] = None
        
        municipios = Municipios()
        municipios['rs'] = municipios['rs'].astype('str').apply(lambda x: x.zfill(2))
        municipios['macro'] = municipios['macro'].str.upper()
        municipios = municipios[['ibge', 'ibge7', 'rs', 'macro', 'municipio']]
        
        gal = pd.read_csv(join('c:/SESA/arquivos/output','Adicionar_GAL.csv'), sep = ';', encoding = 'utf-8-sig')
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

    
    def dados_fontes_2(self):

        faixa_etaria = [5,9,19,29,39,49,59,69,79]
        faixa_etaria_labels = ['00-05','06-09','10-19','20-29','30-39','40-49','50-59','60-69','70-79','80 e mais']

        today = np.datetime64(date.today())

        df = dados_fontes
        #df = pd.merge(df.rename(columns={'ibge_residencia':'ibge'}), Municipios(), on='ibge', how='left')
        df.loc[df['uf_residencia']!='PR','ibge7'] = 9999999

        dd2 = pd.DataFrame(columns=['ibge', 'arquivo', 'qtde'])

        media_idade_casos_PR = df['idade'].mean()
        dd2 = dd2.append(pd.DataFrame([['PR','Media_Casos_PR',media_idade_casos_PR]],columns=dd2.columns), ignore_index=True)

        media_idade_obitos_PR = df.loc[df['evolucao']==2,'idade'].mean()
        dd2 = dd2.append(pd.DataFrame([['PR','Media_OBT_PR',media_idade_obitos_PR]],columns=dd2.columns), ignore_index=True)

        for ibge, ibge_df in df.groupby('ibge7'):
            dd2 = dd2.append(pd.DataFrame([[ibge,'Casos',len(ibge_df.loc[ibge_df['data_comunicacao']==today])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'Obitos',len(ibge_df.loc[(ibge_df['evolucao']==2)&(ibge_df['data_com_evolucao']==today)])]],columns=dd2.columns), ignore_index=True)
            dd2 = dd2.append(pd.DataFrame([[ibge,'Recuperados',len(ibge_df.loc[(ibge_df['evolucao']==1)&(ibge_df['data_com_evolucao']==today)])]],columns=dd2.columns), ignore_index=True)
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


    def dados_fontes_3(self):
        mn = Municipios()
        mn['rs'] = mn['rs'].astype('str').apply(lambda x: x.zfill(2))
        mn['macro'] = mn['macro'].str.upper()
        mn = mn[['ibge', 'ibge7', 'rs', 'macro']]

        dados_fontes = dados_fontes.copy()
        del dados_fontes['ibge7']
        del dados_fontes['rs']
        del dados_fontes['macro']
        dados_fontes = dados_fontes.rename(columns={'ibge_residencia':'ibge'})
        dados_fontes = dados_fontes.merge(mn, on='ibge', how='left')
        dados_fontes.loc[dados_fontes['uf_residencia']!='PR','ibge7'] = 9999999
        cols = ['ibge7','data_diagnostico']
        dados_fontes_diag_1 = group_dados_fontes_3(dados_fontes.copy(), cols, 'Diagnostico')
        cols = ['rs','data_diagnostico']
        dados_fontes_diag_2 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Diagnostico')
        cols = ['macro','data_diagnostico']
        dados_fontes_diag_3 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Diagnostico')
        cols = ['uf_pr','data_diagnostico']
        dados_fontes_diag_4 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['macro']!='FORA'].copy(), cols, 'Diagnostico')
        dados_fontes_diag = pd.concat([dados_fontes_diag_1, dados_fontes_diag_2, dados_fontes_diag_3, dados_fontes_diag_4])
        
        cols = ['ibge7','data_evolucao']
        dados_fontes_obito_1 = group_dados_fontes_3(dados_fontes.loc[dados_fontes['obito']=='SIM'].copy(), cols, 'Obito')
        cols = ['rs','data_evolucao']
        dados_fontes_obito_2 = group_dados_fontes_3(dados_fontes.loc[((dados_fontes['obito']=='SIM') & (dados_fontes['macro']!='FORA'))].copy(), cols, 'Obito')
        cols = ['macro','data_evolucao']
        dados_fontes_obito_3 = group_dados_fontes_3(dados_fontes.loc[((dados_fontes['obito']=='SIM') & (dados_fontes['macro']!='FORA'))].copy(), cols, 'Obito')
        cols = ['uf_pr','data_evolucao']
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