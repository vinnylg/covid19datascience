import pandas as pd

from bulletin.sistemas.casos_confirmados import CasosConfirmados

from bulletin.utils.utils import auto_fit_columns

def inserir_totais(df):
    df.loc['total_geral'] = df.sum(axis=0)

    for a in df.index.levels[0]:
        try:
            df.loc[(a,f'total_{a}')] = df[a].sum(axis=0)
        except:
            print(':/')
            pass

    return df.sort_index(axis=0)

cc = CasosConfirmados()
cc.load()
casosc = cc.get_casos().rename(columns={'identificacao':'id'})

ativos = casosc.loc[casosc['ativo']==1]



# baixar_notifica = True if input("Enter para continuar, S para baixar notifica") == 'S' else False
# ler_notifica = True if input("Enter para continuar, S para ler notifica") == 'S' else False


# notifica = Notifica()
# if ler_notifica or baixar_notifica:
#     if baixar_notifica:
#         notifica.download_todas_notificacoes()
#     if ler_notifica:
#         notifica.read_todas_notificacoes()
#         notifica.save()
# else:
#     notifica.load()


# notificacoes = notifica.get_casos()
# print(notifica.shape())

# notificacoes.loc[notificacoes['status_notificacao'].isnull()] = -1
# notificacoes.loc[notificacoes['evolucao'].isnull()] = -1

def groupando(notificacoes, grupinho:list, writer):
    grupo = notificacoes.groupby(grupinho)[['id']].count().rename(columns={'id':'quantidade'}).reset_index()
    grupo.to_excel(writer,"_".join([ x[0] if not '_' in x else x.split('_')[0][0]+x.split('_')[1][0] for x in grupinho]), index=False)
    worksheet = writer.sheets["_".join([ x[0] if not '_' in x else x.split('_')[0][0]+x.split('_')[1][0] for x in grupinho])]
    auto_fit_columns(worksheet,grupo)

writer = pd.ExcelWriter("output/grupos.xlsx",
                    engine='xlsxwriter',
                    datetime_format='dd/mm/yyyy',
                    date_format='dd/mm/yyyy')


groupando(ativos,['rs'],writer)
# groupando(notificacoes,['status_notificacao'],writer)
# groupando(notificacoes,['evolucao'],writer)

# groupando(notificacoes,['rs','classificacao_final'],writer)
# groupando(notificacoes,['rs','status_notificacao'],writer)
# groupando(notificacoes,['rs','evolucao'],writer)

# groupando(notificacoes,['rs','classificacao_final','status_notificacao'],writer)
# groupando(notificacoes,['rs','classificacao_final','evolucao'],writer)

writer.save()
