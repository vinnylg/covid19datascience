from time import time

class Timer:
    def __init__(self):
        self.__start = time()
        self.__running = True

    def stop(self):
        if self.__running:
            self.__running = False
            self.__end = time()
            self.__time_elapsed = self.__end - self.__start
            return self.__time_elapsed
        else:
            print('Timer already stoped')

    def time(self):
        if not self.__running:
            return self.__time_elapsed
        else:
            return time() - self.__start

    def reset(self):
        self.__running = True
        self.__start = time()
        self.__end = 0