import paramiko
import os
import contextlib
from configparser import ConfigParser
import psycopg2



@contextlib.contextmanager
def get_ssh_conn(params):
    """
    Context manager to automatically close ssh connection. 
    We retrieve credentials from Environment variables
    """

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**params)

    try:
        yield ssh
    except:
        raise Exception('Connection failed!')
    finally:
        ssh.close()

@contextlib.contextmanager
def get_db_conn(params):
    conn = psycopg2.connect(**params)

    try:
        yield conn
    except:
        raise Exception('Connection failed!')
    finally:
        conn.close()