from datetime import datetime, timedelta
from os import listdir, remove, stat
from os.path import isfile, join

from bulletin import root, parent, default_input, default_output


def clear_directories():
    hoje = datetime.today()
    ontem = hoje - timedelta(1)
    anteontem = ontem - timedelta(1)

    try:
        # PASTA: covid19datascience-dev, bulletin, services
        pathfile = join(root, 'services', 'cookie')
        t = stat(pathfile)[8]
        filetime = datetime.fromtimestamp(t)
        if (filetime.date() == ontem.date()):
            try:
                print('Removing file: ', pathfile)
                remove(pathfile)
            except:
                pass
    except:
        pass


    try:
        # PASTA: SCRIPTS, LOGS
        path = join(parent,'logs')
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        if (len(pathfile) != 0):
            for files in pathfile:
                t = stat(files)[8] 
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == anteontem.date()):
                    try:
                        print('Removing file: ', files)
                        remove(files)
                    except:
                        pass
    except:
        pass


    try:
        # PASTA: covid19datascience-dev, bulletin, database, backups
        path = join(root, 'database', 'backups')
        exclu = f"backup_notifica_diario_{anteontem.strftime('%d_%m_%Y')}.zip"
        pathfile = join(path, exclu)
        try:
            remove(pathfile)
            print('Removing file: ', pathfile)
        except:
            pass
    except:
        pass

    try:        
        # PASTA: covid19datascience-dev, bulletin, database, casos_confirmados
        path = join(root, 'database', 'casos_confirmados')
        cc = f"cc_{anteontem.strftime('%d_%m_%Y')}.pkl"
        exclu = f"exclusao_notificacoes_{ontem.strftime('%d_%m_%Y')}.pkl"
        try:
            remove(join(path, cc))
            print('Removing file: ', join(path, cc))
        except:
            pass

        try:
            remove(join(path, exclu))
            print('Removing file: ', join(path, exclu)) 
        except:
            pass
    except:
        pass


    try:
        # PASTA: covid19datascience-dev, bulletin, database, gal
        pathfile = join(root, 'database', 'gal', 'consulta-gal-complemento-2021.csv')
        try:
            t = stat(pathfile)[8]
            filetime = datetime.fromtimestamp(t)
            if (filetime.date() == ontem.date()):
                try:
                    print('Removing file: ', pathfile)
                    remove(pathfile)
                except:
                    pass
        except:
            pass
    except:
        pass

    try:
        # PASTA: covid19datascience-dev, input, ocupacao_leitos
        pathfile = join(default_input, 'ocupacao_leitos', 'ocupacao.xls')
        try:
            t = stat(pathfile)[8]
            filetime = datetime.fromtimestamp(t)
            if (filetime.date() == ontem.date()):
                try:
                    print('Removing file: ', pathfile)
                    remove(pathfile)
                except:
                    pass
        except:
            pass
    except:
        pass

    try:
    
        # PASTA: covid19datascience-dev, input, queries, tmp
        path = join(default_input, 'queries', 'tmp')
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        if (len(pathfile) != 0):
            for files in pathfile:
                t = stat(files)[8] 
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == ontem.date()):
                    try:
                        print('Removing file: ', files)
                        remove(files)
                    except:
                        pass
    except:
        pass
    

    try:
        # PASTA: covid19datascience-dev, output, comunicacao
        path = join(default_output, 'comunicacao')
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        if (len(pathfile) != 0):
            for files in pathfile:
                t = stat(files)[8] 
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == ontem.date()):
                    try:
                        print('Removing file: ', files)
                        remove(files)
                    except:
                        pass
    except:
        pass
    


    try:
        # PASTA: covid19datascience-dev, output, dash
        path = join(default_output, 'dash')
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        if (len(pathfile) != 0):
            for files in pathfile:
                t = stat(files)[8]
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == ontem.date()):
                    try:
                        print('Removing file: ', files)
                        remove(files)
                    except:
                        pass
    except:
        pass

    try:
        # PASTA: covid19datascience-dev, output, dgs
        path = join(default_output, 'dgs')
        exclu = f"casos_obitos_{anteontem.strftime('%d_%m_%Y')}.xlsx"
        try:
            remove(join(path, exclu))
            print('Removing file: ', join(path, exclu))
        except:
            pass
    except:
        pass


    try:
        # PASTA: covid19datascience-dev, output, enviar_celepar
        path = join(default_output, 'enviar_celepar')
        # files_list = ['CORONAVIRUS.analise.csv', 'CORONAVIRUS.casos_confirmados.csv', 'CORONAVIRUS.leitos.csv', 'CORONAVIRUS.obitos.csv', 'CORONAVIRUS.recuperados.csv', 'CORONAVIRUS.testes.csv']
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
        # pathfile = [join(path, f) for f in files_list]

        for files in pathfile:
            try:
                t = stat(files)[8]
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == ontem.date()):
                    print('Removing file: ', files)
                    remove(files)
            except:
                pass
    except:
        pass
    

    try:
        # PASTA: covid19datascience-dev, output, regionais
        path = join(default_output, 'regionais')
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        if (len(pathfile) != 0):
            for files in pathfile:
                t = stat(files)[8] 
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == ontem.date()):
                    try:
                        print('Removing file: ', files)
                        remove(files)
                    except:
                        pass
    except:
        pass

    try:
        # PASTA: covid19datascience-dev, output, relatorios
        path = join(default_output, 'relatorios')
        pathfile = [join(path, f) for f in listdir(path) if isfile(join(path, f))]

        if (len(pathfile) != 0):
            for files in pathfile:
                t = stat(files)[8]
                filetime = datetime.fromtimestamp(t)
                
                if (filetime.date() == ontem.date()):
                    try:
                        print('Removing file: ', files)
                        remove(files)
                    except:
                        pass
    except:
        pass
