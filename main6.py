import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import *
import cms
from multiprocessing import Process
from multiprocessing import Array
import sys
 
def runBOI(k,id,sharedQ,sharedR,allT,nWorkers,doneWithBlock,cond):
    #sys.stderr = open("logs/Worker"+str(id)+".out", "a")
    stream=cms.PatientStream(ds=id, maxds=nWorkers)
    stream.n=5000
    boi=ParallelBlockOrthogonal(
            id=id,
            sharedQ=sharedQ,
            sharedR=sharedR,
            allT=allT,
            doneWithBlock=doneWithBlock,
            cond=cond,
            k=k,
            order=3,
            stream=stream
            )

    for x in boi:
        continue


    Q=boi.getEstimate()
    print Q.T[:,0:3]
    R=np.frombuffer(boi.sharedR.get_obj()).reshape((k,k))
    print R

    np.savetxt('cmsQ.txt',Q)
    np.savetxt('cmsR.txt',R)
    np.savetxt('cmsComp.txt',np.dot(Q,np.linalg.cholesky(R)))
    return boi.getEstimate()
                    
if __name__ == "__main__":
    t0 = time.time()

    p=260
    k=3
    nWorkers=1

    sharedQ = Array('d', p*k)
    sharedR = Array('d', k*k)
    allT = Array('I', nWorkers,lock=False)
    doneWithBlock = Array('I', nWorkers,lock=False)
    cond = Condition()

    arg={'id':1,'k':k,'sharedQ':sharedQ,'sharedR':sharedR,'allT':allT,'nWorkers':nWorkers,'doneWithBlock':doneWithBlock,'cond':cond}
    runBOI(**arg)


    t1 = time.time()
    total = t1-t0
    print "Total time: ", total


    #print np.dot(results[0].T,results[1])

