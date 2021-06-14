import pandas as pd
from datetime import datetime
from os.path import join

from bulletin import default_input, default_output
from bulletin.systems.casos_confirmados import CasosConfirmados
from bulletin.utils.static import Municipios
from bulletin.utils.normalize import normalize_hash, normalize_labels, normalize_text, date_hash, normalize_number

today = datetime.today()

class DadosFontes:
    def __init__(self):
        cc = CasosConfirmados()
        cc.load(f"cc_{today.strftime('%d_%m_%Y')}")
        self._df = cc.df
        
        municipios = Municipios()

        self._df = pd.merge(self._df.rename(columns={'ibge_resid':'ibge'}),municipios,on=['ibge'],how='left').rename(columns={'ibge':'ibge_resid'})

        self._df['municipio_pr'] = self._df['municipio']
        self._df.loc[self._df['uf_resid']!='PR','municipio_pr'] = 'Fora'

        self._df.loc[self._df['uf_resid']!='PR','ibge7'] = 9999999

        self._df['uf_pr'] = 'PR'
        self._df.loc[self._df['uf_resid']!='PR','uf_pr'] = 'Fora'

        
        self._df['obito'] = ''
        self._df['status'] = ''
        self._df.loc[self._df['evolucao']==2, 'obito'] = 'SIM'
        self._df.loc[self._df['evolucao']==1, 'status'] = 'RECUPERADO'
        
    '''
    ----------------------------------------------------------------------------------------------------
    Macro_19_Dados_Fonte
    SELECT TB_Municipios_PR.MACRO, TB_Municipios_PR.RS, TB_Municipios_PR.IBGE, TB_Municipios_PR.Municipio, TB_PACIENTES.Obito, TB_PACIENTES.STATUS, Count(TB_PACIENTES.Identificacao) AS Qtde INTO Dados_Fonte
    FROM TB_Municipios_PR INNER JOIN TB_PACIENTES ON TB_Municipios_PR.IBGE = TB_PACIENTES.IBGE_RES_PR
    GROUP BY TB_Municipios_PR.MACRO, TB_Municipios_PR.RS, TB_Municipios_PR.IBGE, TB_Municipios_PR.Municipio, TB_PACIENTES.Obito, TB_PACIENTES.STATUS;

    ----------------------------------------------------------------------------------------------------
    Macro_31_Add_Dados_GAL_Dados_Fonte
    INSERT INTO Dados_Fonte ( MACRO, RS, IBGE, Municipio, Obito, STATUS, Qtde )
    SELECT TB_Municipios_PR.MACRO, TB_Municipios_PR.RS, TB_Municipios_PR.IBGE, TB_Municipios_PR.Municipio, "" AS Obito, "Em investigacao" AS STATUS, Sum(TB_DADOS_GAL.Em_Investigacao) AS Qtde
    FROM TB_Municipios_PR LEFT JOIN TB_DADOS_GAL ON TB_Municipios_PR.IBGE = TB_DADOS_GAL.IBGE7
    GROUP BY TB_Municipios_PR.MACRO, TB_Municipios_PR.RS, TB_Municipios_PR.IBGE, TB_Municipios_PR.Municipio, "", "Em investigacao";

'''
    def dados_fontes(self):
        cols = ['macro','rs','ibge7','municipio_pr','obito','status']
        dados_fontes = self._df[cols+['identificacao']].groupby(cols).count().rename(columns={'identificacao':'qtde'}).reset_index()
        municipios = Municipios()

        gal = pd.read_csv(join(default_input,'gal.csv'))
        gal.columns = [ normalize_labels(x) for x in gal.columns ]
        gal = pd.merge(gal,municipios,on='ibge7',how='left')
        gal['status'] = gal['em_investigacao']
        gal['obito'] = ''
        
 
        cols = ['macro','rs','ibge7','municipio','obito','status']
        gal = gal[cols+['ibge']].groupby(cols).count().rename(columns={'ibge':'qtde'}).reset_index()

        return pd.merge(dados_fontes,gal,on='ibge7',how='left')
        
'''
    ----------------------------------------------------------------------------------------------------
    Macro_01_Add_Media_Faixa_Etaria_Dados_Fonte2
    SELECT TB_PACIENTES.IBGE_RES_PR AS ibge, "Media_Casos" AS arquivo, Avg(TB_PACIENTES.Idade) AS qtde INTO Dados_Fonte2
    FROM TB_PACIENTES
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "Media_Casos";

    ----------------------------------------------------------------------------------------------------
    Macro_02_Add_Obitos_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_Municipios_PR.IBGE, "Obitos" AS arquivo, Count(Add_Obitos.IBGE_RES_PR) AS qtde
    FROM TB_Municipios_PR INNER JOIN Add_Obitos ON TB_Municipios_PR.IBGE = Add_Obitos.IBGE_RES_PR
    GROUP BY TB_Municipios_PR.IBGE, "Obitos";

    ----------------------------------------------------------------------------------------------------
    Macro_03_Add_Recuperados_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_Municipios_PR.IBGE, "Recuperados" AS arquivo, Count(TB_Municipios_PR.IBGE) AS ContarDeIBGE
    FROM TB_Municipios_PR INNER JOIN Add_Recuperados ON TB_Municipios_PR.IBGE = Add_Recuperados.IBGE
    GROUP BY TB_Municipios_PR.IBGE, "Recuperados";

    ----------------------------------------------------------------------------------------------------
    Macro_04_Add_Sexo_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, TB_PACIENTES.Sexo, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    GROUP BY TB_PACIENTES.IBGE_RES_PR, TB_PACIENTES.Sexo;

    ----------------------------------------------------------------------------------------------------
    Macro_05_Add_Obito_Sexo_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "OBT_"+[Sexo] AS Expr1, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Obito)="SIM"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "OBT_"+[Sexo];

    ----------------------------------------------------------------------------------------------------
    Macro_06_Add_F_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "F"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais"))))))))) AS Expr1, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Sexo)="F"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "F"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais")))))))));

    ----------------------------------------------------------------------------------------------------
    Macro_06_Add_F_Faixa_Etaria_Dados_Fonte21
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "F"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais"))))))))) AS Expr1, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Sexo)="F"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "F"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais")))))))));

    ----------------------------------------------------------------------------------------------------
    Macro_07_Add_M_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "M"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais"))))))))) AS Expr1, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Sexo)="M"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "M"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais")))))))));

    ----------------------------------------------------------------------------------------------------
    Macro_08_Add_Obito_F_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "OBT_F_"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais"))))))))) AS Expr1, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Obito)="SIM"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "OBT_F_"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais"))))))))), TB_PACIENTES.Sexo
    HAVING (((TB_PACIENTES.Sexo)="F"));

    ----------------------------------------------------------------------------------------------------
    Macro_09_Add_Obito_M_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "OBT_M_"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais"))))))))) AS Expr1, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Obito)="SIM") AND ((TB_PACIENTES.Sexo)="M"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "OBT_M_"+IIf(Int([Idade])<=5,"00-05",IIf(Int([Idade])<=9,"06-09",IIf(Int([Idade])<=19,"10-19",IIf(Int([Idade])<=29,"20-29",IIf(Int([Idade])<=39,"30-39",IIf(Int([Idade])<=49,"40-49",IIf(Int([Idade])<=59,"50-59",IIf(Int([Idade])<=69,"60-69",IIf(Int([Idade])<=79,"70-79","80 e mais")))))))));

    ----------------------------------------------------------------------------------------------------
    Macro_10_Add_Media_PR_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT "PR" AS Expr1, "Media_Casos_PR" AS Expr2, Avg(TB_PACIENTES.Idade) AS M�diaDeIdade
    FROM TB_PACIENTES
    GROUP BY "PR", "Media_Casos_PR";

    ----------------------------------------------------------------------------------------------------
    Macro_11_Add_Obito_Media_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "Media_OBT" AS Expr1, Avg(TB_PACIENTES.Idade) AS M�diaDeIdade
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Obito)="SIM"))
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "Media_OBT";

    ----------------------------------------------------------------------------------------------------
    Macro_12_Add_Casos_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT TB_Municipios_PR.IBGE, "Casos" AS arquivo, Count(Add_Casos.IBGE_RES_PR) AS qtde
    FROM TB_Municipios_PR INNER JOIN Add_Casos ON TB_Municipios_PR.IBGE = Add_Casos.IBGE_RES_PR
    GROUP BY TB_Municipios_PR.IBGE, "Casos";

    ----------------------------------------------------------------------------------------------------
    Macro_13_Add_Obito_Media_PR_Faixa_Etaria_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT "PR" AS Expr2, "Media_OBT_PR" AS Expr1, Avg(TB_PACIENTES.Idade) AS M�diaDeIdade
    FROM TB_PACIENTES
    WHERE (((TB_PACIENTES.Obito)="SIM"))
    GROUP BY "PR", "Media_OBT_PR";

    ----------------------------------------------------------------------------------------------------
    Macro_14_Add_Grupo_Laboratorio_Dados_Fonte2
    INSERT INTO Dados_Fonte2 ( IBGE, arquivo, qtde )
    SELECT "GRUPO_LAB" AS Expr2, TB_Laboratorios.COD_GRUPO, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Laboratorios ON TB_PACIENTES.COD_LABORATORIO = TB_Laboratorios.Codigo
    GROUP BY "GRUPO_LAB", TB_Laboratorios.COD_GRUPO;

    ----------------------------------------------------------------------------------------------------
    Macro_15_Add_Diagostico_Dados_Fonte3
    SELECT TB_PACIENTES.IBGE_RES_PR, "Diagnostico" AS Tipo_Data, TB_PACIENTES.Dt_diag AS data, Count(TB_PACIENTES.Identificacao) AS qtde INTO Dados_Fonte3
    FROM TB_PACIENTES
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "Diagnostico", TB_PACIENTES.Dt_diag
    HAVING (((TB_PACIENTES.Dt_diag) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_16_Add_Inicio_Sintomas_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "Inicio_Sintomas" AS Tipo_Data, TB_PACIENTES.dt_inicio_sintomas, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "Inicio_Sintomas", TB_PACIENTES.dt_inicio_sintomas
    HAVING (((TB_PACIENTES.dt_inicio_sintomas) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_17_Add_Notificacao_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "Notificacao" AS Tipo_Data, TB_PACIENTES.dt_notificacao, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "Notificacao", TB_PACIENTES.dt_notificacao
    HAVING (((TB_PACIENTES.dt_notificacao) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_18_Add_Obito_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_PACIENTES.IBGE_RES_PR, "Obito" AS Tipo_Data, TB_PACIENTES.Dt_obito, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES
    GROUP BY TB_PACIENTES.IBGE_RES_PR, "Obito", TB_PACIENTES.Dt_obito
    HAVING (((TB_PACIENTES.Dt_obito) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_20_Add_Notificacao_RS_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_Municipios_PR.RS, "Notificacao" AS Tipo_Data, TB_PACIENTES.dt_notificacao, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR = TB_Municipios_PR.IBGE
    GROUP BY TB_Municipios_PR.RS, "Notificacao", TB_PACIENTES.dt_notificacao
    HAVING (((TB_Municipios_PR.RS) Is Not Null) AND ((TB_PACIENTES.dt_notificacao) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_21_Add_Notificacao_MACRO_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_Municipios_PR.MACRO, "Notificacao" AS Tipo_Data, TB_PACIENTES.dt_notificacao, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR=TB_Municipios_PR.IBGE
    GROUP BY TB_Municipios_PR.MACRO, "Notificacao", TB_PACIENTES.dt_notificacao
    HAVING (((TB_Municipios_PR.MACRO) Is Not Null) AND ((TB_PACIENTES.dt_notificacao) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_22_Add_Notificacao_PR_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT "PR" AS Expr1, "Notificacao" AS Tipo_Data, TB_PACIENTES.dt_notificacao, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR=TB_Municipios_PR.IBGE
    WHERE (((TB_Municipios_PR.MACRO) Is Not Null))
    GROUP BY "PR", "Notificacao", TB_PACIENTES.dt_notificacao
    HAVING (((TB_PACIENTES.dt_notificacao) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_23_Add_Obito_Dados_RS_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_Municipios_PR.RS, "Obito" AS Tipo_Data, TB_PACIENTES.Dt_obito, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR = TB_Municipios_PR.IBGE
    GROUP BY TB_Municipios_PR.RS, "Obito", TB_PACIENTES.Dt_obito
    HAVING (((TB_Municipios_PR.RS) Is Not Null) AND ((TB_PACIENTES.Dt_obito) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_24_Add_Obito_Dados_MACRO_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_Municipios_PR.MACRO, "Obito" AS Tipo_Data, TB_PACIENTES.Dt_obito, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR=TB_Municipios_PR.IBGE
    GROUP BY TB_Municipios_PR.MACRO, "Obito", TB_PACIENTES.Dt_obito
    HAVING (((TB_Municipios_PR.MACRO) Is Not Null) AND ((TB_PACIENTES.Dt_obito) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_25_Add_Obito_Dados_PR_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT "PR" AS Expr1, "Obito" AS Tipo_Data, TB_PACIENTES.Dt_obito, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR = TB_Municipios_PR.IBGE
    WHERE (((TB_Municipios_PR.MACRO) Is Not Null))
    GROUP BY "PR", "Obito", TB_PACIENTES.Dt_obito
    HAVING (((TB_PACIENTES.Dt_obito) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_26_Add_Diagnostico_RS_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_Municipios_PR.RS, "Diagnostico" AS Tipo_Data, TB_PACIENTES.Dt_diag, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR=TB_Municipios_PR.IBGE
    GROUP BY TB_Municipios_PR.RS, "Diagnostico", TB_PACIENTES.Dt_diag
    HAVING (((TB_Municipios_PR.RS) Is Not Null) AND ((TB_PACIENTES.Dt_diag) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_27_Add_Diagnostico_MACRO_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT TB_Municipios_PR.MACRO, "Diagnostico" AS Tipo_Data, TB_PACIENTES.Dt_diag, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR=TB_Municipios_PR.IBGE
    GROUP BY TB_Municipios_PR.MACRO, "Diagnostico", TB_PACIENTES.Dt_diag
    HAVING (((TB_Municipios_PR.MACRO) Is Not Null) AND ((TB_PACIENTES.Dt_diag) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_28_Add_Diagnostico_PR_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT "PR" AS Expr1, "Diagnostico" AS Tipo_Data, TB_PACIENTES.Dt_diag, Count(TB_PACIENTES.Identificacao) AS ContarDeIdentificacao
    FROM TB_PACIENTES INNER JOIN TB_Municipios_PR ON TB_PACIENTES.IBGE_RES_PR=TB_Municipios_PR.IBGE
    WHERE (((TB_Municipios_PR.MACRO) Is Not Null))
    GROUP BY "PR", "Diagnostico", TB_PACIENTES.Dt_diag
    HAVING (((TB_PACIENTES.Dt_diag) Is Not Null));

    ----------------------------------------------------------------------------------------------------
    Macro_29_Add_DiagosticoC_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT "PR" AS Expr1, "DiagnosticoC" AS Tipo_Data, TB_Datas.Data, TB_Datas.Casos
    FROM TB_Datas;

    ----------------------------------------------------------------------------------------------------
    Macro_30_Add_ObitosC_Dados_Fonte3
    INSERT INTO Dados_Fonte3 ( IBGE_RES_PR, Tipo_Data, data, qtde )
    SELECT "PR" AS Expr1, "ObitoC" AS Tipo_Data, TB_Datas.Data, TB_Datas.Obitos
    FROM TB_Datas;

    '''