import pandas as pd
from os.path import join

import logging
logger = logging.getLogger(__name__)

from bulletin.systems import System
from bulletin.services.connections import get_ssh_conn

class Gal(System):
    def __init__(self,database:str='gal'):
        super().__init__('gal',database)

    def load(self, database:str=None, compress:bool=False):
        super().load(database=database,compress=compress)

    def save(self,database:str=None,replace:bool=False, compress:bool=False):
        super().save(database=database,replace=replace,compress=compress)

    def normalize(self):
        super().normalize()

    def download_all(self):
        command = "cd gal; ./python3 exporta-all.py"
        with get_ssh_conn(self.config) as ssh:
            stdin, stdout, stderr = ssh.exec_command(command)
            lines = stdout.readlines()
            logger.info("".join(lines))
            remotePath = "/home/boletim/gal/consulta-gal-all.pkl"
            localPath = join(self.database_dir,'gal.pkl')

            ## Copy remote file to server        
            sftp = ssh.open_sftp()
            sftp.get(remotePath,localPath)
            sftp.close()

    def download_update(self):
        command = "cd gal; ./python3 exporta-complemento.py"
        with get_ssh_conn(self.config) as ssh:
            stdin, stdout, stderr = ssh.exec_command(command)
            lines = stdout.readlines()
            logger.info("".join(lines))
            remotePath = "/home/boletim/gal/consulta-gal-complemento.csv"
            localPath = join(self.database_dir,'consulta-gal-complemento.csv')

            ## Copy remote file to server        
            sftp = ssh.open_sftp()
            sftp.get(remotePath,localPath)
            sftp.close()

    def update(self):
        assert not self.df is None
        self.download_update()
        gal_complemento = pd.read_csv(join(self.database_dir,'consulta-gal-complemento.csv'),sep=';')
        news = super().process_update(gal_complemento,Gal)
        super().update(news)


        