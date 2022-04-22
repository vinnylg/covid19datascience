#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from pathlib import Path
from os import getcwd, remove, chdir
from os.path import join, basename

sys.path.append(str(Path(__file__).parent.parent))
print(str(Path(__file__).parent.parent))


# In[2]:


import pandas as pd
from random import randint
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

from tqdm.auto import tqdm
tqdm.pandas()


# In[3]:


from bulletin import root, default_input, default_output, agora, hoje, ontem, anteontem, dias_apos, dias_apos_label
from bulletin.services.metabase import Metabase
from bulletin.systems.casos_confirmados import CasosConfirmados
from bulletin.systems.notifica import Notifica
from bulletin.utils.static import Municipios
from bulletin.utils import utils, static
from bulletin.utils.xls_ import fit_cols
from bulletin.utils.normalize import trim_overspace
from bulletin.utils.normalize import normalize_text#normalize_hash, normalize_labels, , date_hash, normalize_number


# In[4]:


from datetime import datetime, date, timedelta

exclusao_pathfile = join(root, 'database', 'casos_confirmados')

today = pd.to_datetime(date.today())
ontem = today - timedelta(1)
anteontem = ontem - timedelta(1)
data_retroativos = ontem - timedelta(31)


# In[5]:


utils.create_backup(first_name = "backup_notifica_diario_" , level=3)
chdir(current_dir)


# In[6]:


from bulletin.utils.clean_up import clear_directories
clear_directories()


# In[7]:


municipios = Municipios()
municipios['mun_resid'] = municipios['municipio']
municipios.loc[municipios['uf']!='PR','mun_resid'] = municipios.loc[municipios['uf']!='PR','municipio'] + '/' + municipios['uf']


# ## 1. Atualização e Carregamento das Bases de Dados

# In[8]:


update = True
load_downloaded = False

# Load 
notifica = Notifica()
notifica.databases()
notifica.load('notifica', compress=False)
notifica.df = notifica.df.drop_duplicates('id', keep='last')
    
if update:
    mb = Metabase()
    days = 3
    intervalo = f"(data_notificacao >= NOW() - INTERVAL '{days} DAY') or (data_liberacao >= NOW() - INTERVAL '{days} DAY') or (updated_at >= NOW() - INTERVAL '{days} DAY') or (data_coleta >= NOW() - INTERVAL '{days} DAY') or (data_encerramento >= NOW() - INTERVAL '{days} DAY') or (data_cura_obito >= NOW() - INTERVAL '{days} DAY')"
    mb.generate_notifica_query('update_notifica', where=intervalo, replace=True)
    update_notifica_parts = mb.download_notificacao('update_notifica', load=load_downloaded)
    
    update_notifica = Notifica()
    update_notifica.read(update_notifica_parts)
    update_notifica.normalize()
    
    
    update_notifica.df = update_notifica.df.drop_duplicates('id', keep='last')
    analise_dtd = update_notifica.analise_data_diagnostico() ## only in casos confirmados, pd.NaT nos demais

    # novas_notificacoes, atualizacoes_fichas = notifica.update(update_notifica)#, observed_cols=['excluir_ficha','status_notificacao','classificacao_final','data_1o_sintomas','data_diagnostico','evolucao','data_cura_obito','data_encerramento'])
    notifica.update(update_notifica)

    notifica.fix_dtypes()
    notifica.save(replace=True, compress=False)

notifica.df.shape


# In[9]:


cc = CasosConfirmados()
cc.load(f"cc_{ontem.strftime('%d_%m_%Y')}", compress=True)
cc.df.shape


# In[10]:


casos_confirmados = cc.df.loc[cc.df['id'] > 0].copy()
casos_confirmados['classificacao_final'] = 2
casos_confirmados['excluir_ficha'] = 2 #EXCLUIR FICHA "NÃO"


# ## 2. Processo de identificação de fichas diferentes

# In[11]:


## CASOS E/OU ÓBITOS COM CLASSIFICAÇÃO FINAL DIFERENTE DE 2


# In[12]:


diff_cc_final = pd.DataFrame()
diff_cc_final = pd.merge(casos_confirmados[['id','paciente','classificacao_final']], notifica.df[['id','classificacao_final']].rename(columns={'id':'id'}), on='id', how='inner', suffixes=['_old','_new'])

diff_cc_final = diff_cc_final.loc[diff_cc_final['classificacao_final_old'] != diff_cc_final['classificacao_final_new']]


# In[13]:


temporario_diff_cf = cc.df.loc[cc.df['id'].isin(diff_cc_final['id'])]
print('CASOS EXCLUSÃO que mudaram de classificação final =', len(temporario_diff_cf))


# In[14]:


## CASOS E/OU ÓBITOS COM STATUS NOTIFICACAO DIFERENTE DE 1 ou 2


# In[15]:


diff_cc_status = pd.DataFrame()
diff_cc_status = pd.merge(casos_confirmados[['id','paciente','status_notificacao','evolucao']], notifica.df[['id','status_notificacao']].rename(columns={'id':'id'}), on='id', how='inner', suffixes=['_old','_new'])

diff_cc_status = diff_cc_status.loc[diff_cc_status['status_notificacao_old'] != diff_cc_status['status_notificacao_new']]
diff_cc_status_obitos = diff_cc_status.loc[~diff_cc_status['status_notificacao_new'].isin([1,2]) & (diff_cc_status['evolucao']==2)]


# In[16]:


# diff_cc_status_obitos.to_excel('obitos_que_inativaram.xlsx')


# In[17]:


tmp_diff_cc_st = cc.df.loc[cc.df['id'].isin(diff_cc_status_obitos['id'])]
print('CASOS EXCLUSÃO que mudaram de status para inativo ou duplicado (3 ou 4) =', len(diff_cc_status_obitos))


# In[18]:


## CASOS E/OU ÓBITOS EXCLUIR FICHA == SIM


# In[19]:


diff_excluir_ficha = pd.DataFrame()
diff_excluir_ficha = pd.merge(casos_confirmados[['id','paciente', 'excluir_ficha']], notifica.df[['id', 'excluir_ficha']].rename(columns={'id':'id'}), on='id', how='inner', suffixes=['_old','_new'])

diff_excluir_ficha = diff_excluir_ficha.loc[diff_excluir_ficha['excluir_ficha_old'] != diff_excluir_ficha['excluir_ficha_new']]
# diff_excluir_ficha.sort_values('paciente')


# In[20]:


temporario_diff_excf = cc.df.loc[cc.df['id'].isin(diff_excluir_ficha['id'])]
print('CASOS EXCLUSÃO que mudaram para excluir ficha "SIM" =', len(temporario_diff_excf))


# In[21]:


# temporario_diff_excf.sort_values('paciente')[['paciente', 'evolucao']]


# In[22]:


## CASOS QUE NÃO FORAM A ÓBITO, MAS MUDARAM PARA OBITO NO NOTIFICA


# In[23]:


diff_evolucao_obito = pd.DataFrame()
diff_evolucao_obito = pd.merge(casos_confirmados[['id','paciente', 'evolucao']], notifica.df[['id', 'evolucao']].rename(columns={'id':'id'}), on='id', how='inner', suffixes=['_old','_new'])

diff_evolucao_obito = diff_evolucao_obito.loc[(diff_evolucao_obito['evolucao_old'] != 2) & (diff_evolucao_obito['evolucao_new'] == 2)]


# In[24]:


temporario_diff_evolu = cc.df.loc[cc.df['id'].isin(diff_evolucao_obito['id'])]
print('CASOS não óbitos QUE EVOLUÍRAM PARA ÓBITO =', len(temporario_diff_evolu))


# In[25]:


diff_evolucao = pd.DataFrame()
diff_evolucao = pd.merge(casos_confirmados[['id','paciente', 'evolucao']], notifica.df[['id', 'evolucao']].rename(columns={'id':'id'}), on='id', how='inner', suffixes=['_old','_new'])


# ## 3. PREVISÃO DOS NOVOS ÓBITOS QUE SERÃO INSERIDOS HOJE

# In[26]:


notifica.df = notifica.df.loc[((notifica.df['classificacao_final']==2)&(notifica.df['excluir_ficha']==2)&(notifica.df['status_notificacao'].isin([1,2])))]
notifica.replace('sexo')
notifica.df = pd.merge(notifica.df.rename(columns={'ibge_residencia':'ibge'}),municipios[['ibge','macro','rs','mun_resid','uf','municipio','regional']],on='ibge',how='left').rename(columns={'ibge':'ibge_residencia'})
notifica.df = pd.merge(notifica.df.rename(columns={'ibge_unidade_notifica':'ibge'}),municipios[['ibge','mun_resid']].rename(columns={'mun_resid':'mun_atend'}),on='ibge',how='left').rename(columns={'ibge':'ibge_unidade_notifica'})
notifica.df = notifica.df.loc[((notifica.df['sexo']!='N')&(notifica.df['mun_resid'].notna())&(notifica.df['data_diagnostico'].notna())&(notifica.df['paciente'].str.len() > 5))]


# In[27]:


notifica_duplicados = notifica.check_duplicates(keep=False)
notifica.df['duplicated'] = notifica.df['duplicated'].fillna(False)
notifica.df = notifica.df.loc[~notifica.df['duplicated']]


# In[28]:


novos_casos = notifica.df.loc[~(
    (notifica.df['id'].isin(cc.df['id'])) 
)].copy()

obitos_notifica = notifica.df.loc[(notifica.df['evolucao']==2)]
obitos_casos = cc.df.loc[(cc.df['evolucao']==2)]

novos_obitos = obitos_notifica.loc[~(
    (obitos_notifica['id'].isin(obitos_casos['id']))
)].copy()

novos_obitos = novos_obitos.loc[novos_obitos['data_cura_obito'].notna()]
novos_obitos = novos_obitos.loc[novos_obitos['data_cura_obito'] >= pd.to_datetime('2021-01-01')]

novos_obitos = novos_obitos.loc[novos_obitos['id'].isin(cc.df['id'].tolist() + novos_casos['id'].tolist())]
novos_obitos = novos_obitos.drop_duplicates('id', keep='last')
novos_obitos.groupby('data_cura_obito')[['id']].count().plot()


# In[29]:


print("PREVISÃO DE NOVOS ÓBITOS = ")
novos_obitos.shape


# In[30]:


print("PREVISÃO DE NOVOS CASOS = ")
novos_casos.shape


# ## 4. SALVAR OS ARQUIVOS NECESSÁRIOS

# In[31]:


exclu = CasosConfirmados()
try:
    exclu.load('casos_excluir', compress=False)
    df_exclusao_new = cc.df.loc[cc.df['id'].isin(exclu.df['id'])]
    
#     df_exclusao_new = df_exclusao_new.sample(n=randint(3000,4000))
except:
    df_exclusao_new = pd.DataFrame()


# In[32]:


# df_exclusao_new = pd.concat([df_exclusao_new, temporario_diff_cf, temporario_diff_excf, tmp_diff_cc_st, temporario_diff_evolu])
df_exclusao_new = pd.concat([df_exclusao_new, temporario_diff_cf, temporario_diff_excf, temporario_diff_evolu])


df_exclusao_new = df_exclusao_new.drop_duplicates('id', keep='last')
df_exclusao_new = df_exclusao_new.loc[df_exclusao_new['id'].notnull()]


casos = df_exclusao_new.loc[df_exclusao_new['evolucao'] != 2]
obitos = df_exclusao_new.loc[df_exclusao_new['evolucao'] == 2]

print(f"PS.: df_exclusao_new len =  {len(df_exclusao_new)}")
print(f"PS.: Foram adicionados {len(casos)} novos casos pra exclusão. Desse quantitativo, {len(obitos)} são óbitos.")


# In[33]:


casos_exlu = casos.copy()

if ( (len(novos_obitos) > 2 ) & (len(obitos) > 2) ):
#     print(len(novos_obitos),'????????')
    try:
        obitos_exlu = obitos.sample(n=randint(int(len(novos_obitos) / 2) , len(novos_obitos) - 1))
#         obitos_exlu = obitos.sample(n=randint(1,int(len(novos_obitos) - 1)))
    except:
        obitos_exlu = obitos
    df_exclusao_new = pd.concat([casos_exlu, obitos_exlu])
    print(f"PS.: Foram adicionados {len(df_exclusao_new)} novos casos pra exclusão. Desse quantitativo, {len(obitos_exlu)} são óbitos.")
else:
    df_exclusao_new = pd.concat([casos_exlu])
    print(f"PS.: Foram adicionados {len(df_exclusao_new)} novos casos pra exclusão.") 

df_exclusao_new.to_pickle(join(exclusao_pathfile, f"exclusao_notificacoes_{hoje.strftime('%d_%m_%Y')}.pkl"))


# In[34]:


ob02 = temporario_diff_excf.loc[temporario_diff_excf['evolucao'] == 2]
ob02['situacao'] = "marcado para exclusão de ficha"
ob02 = ob02[['id', 'paciente', 'nome_mae', 'data_diagnostico', 'evolucao', 'data_cura_obito', 'classificacao_final', 'situacao']]
#marcado para exclusão de ficha


# In[35]:


ob03 = tmp_diff_cc_st.loc[tmp_diff_cc_st['evolucao'] == 2]
ob03['situacao'] = "mudaram para inativo ou duplicado"
ob03 = ob03[['id', 'paciente', 'nome_mae', 'data_diagnostico', 'evolucao', 'data_cura_obito', 'classificacao_final', 'situacao']]
#mudaram para inativo ou duplicado


# In[36]:


path = join('C:\\', 'SESA', 'Inconsistencias')
obitos_verificar = pd.concat([ob02, ob03])

obitos_verificar = obitos_verificar.drop_duplicates('id', keep='last')
obitos_verificar = obitos_verificar.sort_values('paciente')

print(len(obitos_verificar))
obitos_verificar.to_excel(join(path, 'qualificação_exclusão_obitos.xlsx'), index=False)

