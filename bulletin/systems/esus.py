# -----------------------------------------------------------------------------------------------------------------------------#
# Classe de tratamento de dados do e-SUS Notifica
# Todos os direitos reservados ao autor
# -----------------------------------------------------------------------------------------------------------------------------#
import os
from os.path import dirname, join, isdir, isfile
from datetime import datetime, date
from os import makedirs
import pandas as pd
import glob

from bulletin import root, default_input, default_output
from bulletin.utils import static
from bulletin.utils.timer import Timer
from bulletin.utils.utils import isvaliddate
from bulletin.utils.normalize import date_hash, normalize_cpf, normalize_text, normalize_number, normalize_hash, normalize_campo_aberto, normalize_do, normalize_cns
from bulletin.utils.static import Municipios

# ----------------------------------------------------------------------------------------------------------------------
class eSUS:

    # ----------------------------------------------------------------------------------------------------------------------
    def __init__(self):
        self.df = None
        self.database = join(root,'database','esus',f"esus_{datetime.today().strftime('%d_%m_%Y')}.pkl")

        if not isdir(dirname(self.database)):
            makedirs(dirname(self.database))
            
        self.input_path = join(default_input,'esus')
        
        if not isdir(self.input_path):
            makedirs(self.input_path)
        
        self.list_input_files = lambda: glob.glob(join(self.input_path , "*.csv"))
        self.list_input_files()
        
    # ----------------------------------------------------------------------------------------------------------------------
    def __len__(self):
        if isinstance(self.df,pd.DataFrame) and len(self.df) > 0: 
            return len(self.df)
        else:
            return -1

    # ----------------------------------------------------------------------------------------------------------------------
    def shape(self):
        if isinstance(self.df,pd.DataFrame) and len(self.df) > 0:   
            return tuple([len(self.df.loc[self.df['evolucao'] == evolucao]) for evolucao in [1, 2, 3, 4]])
        else:
            return -1

    # ----------------------------------------------------------------------------------------------------------------------
    @Timer('reading eSUS')
    def read(self, pathfile, append=False):
        print(f"Reading {pathfile}")
        esus = pd.read_csv(
                pathfile,
                sep=';'
        ).rename(columns={
            'paciente_cns': 'cns',
            'paciente_nome': 'paciente',
            'paciente_cpf': 'cpf',
            'paciente_idade': 'idade'
            'paciente_nome_mae': 'nome_mae',
            'paciente_enumSexoBiologico': 'sexo',
            'paciente_dataNascimento': 'data_nascimento',
            'vacina_dataAplicacao': 'data_aplicacao',
            'vacina_dose': 'dose',
            'vacina_fabricante_nome': 'fabricante',
            'paciente_endereco_coIbgeMunicipio': 'ibge_residencia',
            'estabelecimento_municipio_codigo': 'ibge_atendimento',
            'vacina_categoria_nome': 'categoria',
            'vacina_grupoAtendimento_nome': 'grupo_atendimento'
        })

        notifica = self.__normalize(esus)
        
        if isinstance(self.df, pd.DataFrame) and append:
            print(f"Appending")
            self.df = self.df.append(esus,ignore_index=True)
        else:
            self.df = esus

        print(len(self.df))

    # ----------------------------------------------------------------------------------------------------------------------
    def load(self):
        try:
            self.df = pd.read_pickle(self.database)
        except ValueError:
            raise Exception(f"{ValueError}\nArquivo {self.database} não encontrado")

    # ----------------------------------------------------------------------------------------------------------------------
    def save(self, df=None):
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            new_df = df
            self.df = new_df
        elif isinstance(self.df, pd.DataFrame) and len(self.df) > 0:
            new_df = self.df
        else:
            raise Exception(
                    'Não é possível salvar um DataFrame inexistente, '
                    'realize a leitura antes ou passe como para esse método'
            )

        new_df.to_pickle(self.database)

    # ----------------------------------------------------------------------------------------------------------------------
    # Normaliza strings, datas e códigos. Anula valores incorretos.
    def __normalize(self, esus):
        esus['cpf'] = esus['cpf'].apply(normalize_cpf)
        esus['cns'] = esus['cns'].apply(normalize_cns)
        esus['nome_mae'] = esus['nome_mae'].apply(normalize_campo_aberto)        
        esus['nome_mae'] = esus['nome_mae'].apply(normalize_campo_aberto)
        esus['data_nascimento'] = esus['data_nascimento'].apply(lambda x: pd.to_datetime(x,errors='coerce'))
        esus['data_aplicacao'] = esus['data_aplicacao'].apply(lambda x:isvaliddate(pd.to_datetime(x[:10],format='%Y-%m-%d',errors='coerce')))
        
        return esus