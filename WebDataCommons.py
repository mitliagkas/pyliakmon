import csv
import numpy as np
import streaming

class PLDStream(streaming.SampleStream):
    def __init__(self, file='/var/datasets/wdc/pld-arc.gz'):
        self.f=open(file,'r')
        #streaming.SampleStream.__init__(self,p=65133, n=71567)
        streaming.SampleStream.__init__(self,p=65133, n=69878)

        self.reader=csv.reader(self.f, delimiter='\t')
        self._lastRow=None
        self._userId=None
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
                    self._userVector[int(row[1])-1]=float(row[2])
                except IndexError:
                    print row
                    raise
                
                ## Delete row, now that we've used it
                self._lastRow=None


