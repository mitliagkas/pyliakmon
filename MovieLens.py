import csv
import numpy as np
from stream import *

class UserStream(VectorStream):
    def __init__(self, sparse=True, file='/var/datasets/ml-10M100K/ratingsTab.dat', **kwargs):
        VectorStream.__init__(self, p=65133, n=69878, delta=0.0021972, sparse=sparse, **kwargs)
        self.t=0

        self.f=open(file,'r')
        self.reader=csv.reader(self.f, delimiter='\t')

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
        nu=self.__nextUser()
        self.t+=1
        return nu

    def next(self):
        return self.__next__()

    def __nextUser(self):
        while True:
            ## Get new row or use previously retrieved
            if self._lastRow:
                ## Previously retrieved row means it's the first 
                ## row of a new user
                row=self._lastRow
                if self.sparse:
                    self._userVector={}
                else:
                    self._userVector=np.zeros((self.p,1))
                self._userId=row[0]
            else:
                try:
                    row=self.reader.next()
                except StopIteration:
                    row=None
                self._lastRow=row

            if row==None:
                if self._userId==None:
                    raise StopIteration
                else:
                    ##No more data. Just return collected user vector
                    self._userId=None
                    return self._userVector

            else: ## row!=None
                if self._userId and row[0]!=self._userId:
                    ## New user: Return.
                    ## At the next call, we'll proceed with the next user
                    self._userId=None
                    return self._userVector

                self._userId=row[0]

                ##Proceed
                ## Get the basic values from the row
                try:
                    if False:
                        self.__helper(row)
                    else:
                        self._userVector[int(row[1])-1]=float(row[2])
                    #self._userVector[int(row[1])-1]=float(row[2])
                except IndexError:
                    print row
                    raise
                
                ## Delete row, now that we've used it
                self._lastRow=None

    def __helper(self, row):
        self._userVector[int(row[1])-1]=float(row[2])

