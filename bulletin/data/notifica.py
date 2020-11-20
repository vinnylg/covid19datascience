import pandas as pd
from datetime import datetime, timedelta
from unidecode import unidecode
from bulletin.commom import utils

def read(pathfile:str=join(dirname(__root__),'tmp','notifica.csv')):
    
    notifica = pd.read_csv(pathfile,
                       converters = {
                           'paciente': normalize_text,
                           'idade': lambda x: normalize_number(x,fill=-1),
                           'ibge_residencia': lambda x: normalize_number(x,fill=-1),
                           'ibge_unidade_notifica': lambda x: normalize_number(x,fill=-1),                           
                           'exame': lambda x: normalize_number(x,fill=0),
                           'evolucao': lambda x: normalize_number(x,fill=3)
                       },
                       parse_dates = ['data_notificacao','updated_at','data_liberacao','data_1o_sintomas','data_cura_obito'],
                       date_parser = lambda x: pd.to_datetime(x, errors='coerce', format='%d/%m/%Y')
                    )
    
    municipios = utils.municipios[['ibge','municipio']].copy()
    municipios['municipio'] = municipios['municipio'].apply(normalize_text)

    regionais = utils.regionais[['ibge','nu_reg']].copy()
    regionais = regionais.rename(columns={'ibge':'ibge_residencia','nu_reg':'rs'})

    exames = utils.termos.loc[utils.termos['tipo']=='exame',['codigo','valor']].copy()
    exames = exames.rename(columns={'codigo':'exame','valor':'nome_exame'})

    municipios = municipios.rename(columns={'ibge':'ibge_residencia','municipio':'mun_resid'})
    notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_residencia')
    notifica = pd.merge(left=notifica, right=regionais, how='left', on='ibge_residencia')

    municipios = municipios.rename(columns={'ibge_residencia':'ibge_unidade_notifica','mun_resid':'mun_atend'})
    notifica = pd.merge(left=notifica, right=municipios, how='left', on='ibge_unidade_notifica')
    notifica = pd.merge(left=notifica, right=exames, how='left', on='exame')

    notifica['rs'] = notifica['rs'].apply(lambda x: normalize_number(x,fill='99'))
    notifica['rs'] = notifica['rs'].apply(lambda x: str(x).zfill(2) if x != 99 else None)
    
    notifica = notifica[[ 'id', 'updated_at', 'data_notificacao', 'paciente', 'sexo', 'idade',
                         'mun_resid', 'mun_atend', 'rs', 'nome_exame', 'data_liberacao', 
                         'data_1o_sintomas', 'evolucao', 'data_cura_obito' ]]
    
    casos = notifica[['id', 'data_notificacao', 'paciente', 'sexo', 'idade', 'mun_resid',
                      'mun_atend', 'rs', 'nome_exame', 'data_liberacao','data_1o_sintomas']].copy()
    
    casos_ativos = notifica.loc[ notifica['evolucao'] == 3, ['id', 'data_notificacao', 'paciente', 'sexo', 'idade',
                                                             'mun_resid', 'mun_atend', 'rs', 'nome_exame', 'data_liberacao',
                                                             'data_1o_sintomas']].copy()
    
    obitos = notifica.loc[ notifica['evolucao'] == 2, ['id', 'data_notificacao', 'paciente', 'sexo', 'idade', 
                                                       'mun_resid', 'mun_atend', 'rs', 'nome_exame', 'data_liberacao', 
                                                       'data_1o_sintomas','data_cura_obito']].copy()
    
    recuperados = notifica.loc[ notifica['evolucao'] == 1, ['id', 'data_notificacao', 'paciente', 'sexo', 'idade', 
                                                            'mun_resid','mun_atend', 'rs', 'nome_exame', 'data_liberacao', 
                                                            'data_1o_sintomas', 'data_cura_obito']].copy()

    print(f"Foi lido {len(notifica)} casos do notifica")
    print(f"Dentre os quais:")
    print(f"{len(casos_ativos)} são casos ativos ou não finalizados no sistema")
    print(f"{len(obitos)} são obitos confirmados")
    print(f"{len(recuperados)} são casos recuperados")
    
    return { 'casos':casos,
             'casos_ativos':casos_ativos,
             'obitos':obitos,
             'recuperados':recuperados }