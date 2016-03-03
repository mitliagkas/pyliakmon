import csv
import numpy as np
from stream import *

class CSVStream(VectorStream):
    def __init__(self, sparse=False, file='/var/datasets/higgs/HIGGS1000.csv', **kwargs):
        VectorStream.__init__(self, p=29, delta=1, sparse=sparse, **kwargs)
        self.t=0

        self.f=open(file,'r')
        self.reader=csv.reader(self.f, delimiter=',')

        self._lastRow=None
        self._userId=None

        if self.sparse:
            self._userVector={}
        else:
            self._userVector=np.zeros((self.p,1))

    def __del__(self):
        del self.reader
        self.f.close()

    def __iter__(self):
        return self

    def __next__(self):
        nu=self.__nextRow()
        self.t+=1
        return nu

    def next(self):
        return self.__next__()

    def __nextRow(self):
        return np.array(self.reader.next(), ndmin=2).astype('float').T

