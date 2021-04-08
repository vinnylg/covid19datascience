import os
import sys
import math
import numpy as np
import pandas as pd
import itertools as it
from pandas.core.frame import DataFrame
from unidecode import unidecode as unidecode
from Bio import SeqIO
from tqdm import tqdm

class KGram:
    def __len__(self):
        return len(self.__data)

    def __init__(self, alfabet: set, q: int):
        self.alfabet = sorted(alfabet)
        self.q = q
        self.size = len(alfabet) ** q
        self.grams = [["".join(p),0] for p in it.product(self.alfabet, repeat=q)]
        print('\n----------------------------------------------------\n')

        print(f"\n{q}-grams with {len(self.alfabet)} symbols in alfabet: {self.alfabet} - size:{len(self.alfabet)**q}")
        print('\n----------------------------------------------------\n')
        # self.print_matrix(self.grams, len(self.alfabet)**q)

    def set_filein(self,path):
        self.filein = path

    def print_matrix(self, matrix, size):
        rows = int(math.sqrt(size))
        columns = rows

        print(f"M|n**q| = {len(matrix)}\n")

        for i in range(rows):
            for j in range(columns):
                print(matrix[i*rows+j],end='\t')
            print('')

        print('\n----------------------------------------------------\n')

    def freqkgrams(self,seq: list):
        grams = dict([["".join(p),0] for p in it.product(self.alfabet, repeat=self.q)])
        for i in range(0,len(seq)-self.q):
            window = "".join(seq[i:i+self.q])
            if window in grams.keys():
                grams[window] += 1
            else:
                grams[window] = 1

        freq = [ x / (len(seq) - self.q) for x in grams.values() ]
        gram = grams.keys()

        return gram,freq

    def space(self, text):
        parts = filter(lambda x: len(x) > 0,text.split(" "))
        parts = unidecode("".join(parts))
        return list(filter(lambda x: x in self.alfabet, parts.upper()))
