import functools
from time import time


def ftime(time_in):
    days, seconds = divmod(time_in, 60 * 60 * 24)
    hours, seconds = divmod(seconds, 60 * 60)
    minutes, seconds = divmod(seconds, 60)
#     seconds = round(seconds)

    return f"{int(hours):02}:{int(minutes):02}:{seconds:02.4f}"
#     return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


class Timer():
    def __init__(self,msg='',end='\n',init_msg=''):
        self.running = False
        self.start_time = 0
        self.msg = msg + ' '
        self.end = end
        self.fname = ''
        self.init_msg = init_msg
        
    def __call__(self, f):
        self.fname = f"{f.__module__}.{f.__name__} "
        # if self.init_msg != '': print(self.init_msg,end=' ')
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            try:
                with self:
                    return f(*args, **kwargs)
            except TypeError:
                raise Exception(f"Error: {TypeError}")

        return decorated

    def run(self):
        self.running = True
        self.start_time = time()

    def time_elapsed(self):
        time_elapsed = time() - self.start_time
        return ftime(time_elapsed)

    def __enter__(self):
        if self.init_msg != '': print(self.init_msg,end='\t')
        self.run()
        return self

    def __exit__(self, *args, **kwargs):
        self.running = False
        print(f"time elepsed: {self.time_elapsed()}",end=self.end)
