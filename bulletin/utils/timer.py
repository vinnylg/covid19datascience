import functools
import threading
from time import time, sleep


def ftime(time_in):
    days, seconds = divmod(time_in, 60 * 60 * 24)
    hours, seconds = divmod(seconds, 60 * 60)
    minutes, seconds = divmod(seconds, 60)
    seconds = round(seconds)

    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


class Timer():
    def __init__(self, desc):
        self.running = False
        self.start_time = 0
        self.desc = desc

    def __call__(self, f):
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
        self.run()
        return self

    def __exit__(self, *args, **kwargs):
        self.running = False
        print(f"{self.desc} time elepsed:{self.time_elapsed()}")

