
# coding: utf-8

# # Parallel PCA - Split Sources 

# Parallel principal component analysis based on [Mitliagkas et al, 2013]. This version assumes that the dataset is already split into separate files on disk.

# System and library modules.

# In[1]:

import numpy as np
from numpy import linalg as la
import time
import subprocess

from multiprocessing import Process
from multiprocessing import Array
import sys

import datetime


# Streaming modules from the Pyliakmon library.

# In[14]:

from streaming import *
from CSVStream import CSVStream


# In[52]:

def runBOI(k, id, sharedQ, sharedR, allT, nWorkers, doneWithBlock, cond):

    stream = CSVStream(file='/var/datasets/higgs/HIGGS1MPart'+str(nWorkers)+str(id)+'.csv', n=1000000)
    
    sys.stderr = open("logs/Worker"+str(id)+".out", "a")
    #sys.stderr.write('\n')
    boi = ParallelBlockOrthogonal(
        id=id,
        sharedQ=sharedQ,
        sharedR=sharedR,
        allT=allT,
        doneWithBlock=doneWithBlock,
        cond=cond,
        k=k,
        stream=stream,
    )

    for x in boi:
        continue

        #########

    Q = boi.getEstimate()
    print Q.T[:, 0:3]
    R = np.frombuffer(boi.sharedR.get_obj()).reshape((k, k))
    print R

    #datestring=time.strftime("%Y-%m-%d", time.gmtime())
    #timestring=time.strftime("%H:%M:%S", time.gmtime())
    datestring = time.strftime("%Y-%m-%d %H:%M:%S")

    if id == 1:
        print "Saving results to disk"
        np.savetxt('results/' + str(datetime.date.today()) + '-' + stream.name + '-Q.txt', Q)
        np.savetxt('results/' + str(datetime.date.today()) + '-' + stream.name + '-R.txt', R)
        np.savetxt('results/' + str(datetime.date.today()) + '-' + stream.name + '-Comp.txt',
                   np.dot(Q, np.linalg.cholesky(R)))

        np.savetxt('Q.txt', Q)
        np.savetxt('R.txt', R)
        np.savetxt('Comp.txt', np.dot(Q, np.linalg.cholesky(R)))


# ### Parameters

# In[62]:

p=29
k=2
nWorkers=4


# ### Run Parallel Block Orthogonal Iteration

# In[63]:

sharedQ = Array('d', p * k)
sharedR = Array('d', k * k)
allT = Array('I', nWorkers, lock=False)
doneWithBlock = Array('I', nWorkers, lock=False)
cond = Condition()

processes=[]

t0 = time.time()

for id in xrange(1,nWorkers+1):
    arg={   'id':id,
            'k':k,
            'sharedQ': sharedQ,
            'sharedR': sharedR,
            'allT':allT,
            'nWorkers':nWorkers,
            'doneWithBlock':doneWithBlock,
            'cond':cond
        }

    processes += [Process(target=runBOI, kwargs=arg)]
    processes[-1].start()
    

# Join them
for id in xrange(1,nWorkers+1):
    processes[id-1].join()

t1 = time.time()
total = t1-t0
print "Total time: ", total


# In[ ]:



