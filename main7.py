import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import *
import cms
from multiprocessing import Process
from multiprocessing import Array
import sys
 
def runBOI(k,id,sharedQ,sharedR,allT,nWorkers,doneWithBlock,cond,sharedQ2,sharedR2,allT2,doneWithBlock2,cond2):
    #sys.stderr = open("logs/Worker"+str(id)+".out", "a")
# Get whitening matrix
    boi=ParallelTopicWhiten(
            id=id,
            sharedQ=sharedQ,
            sharedR=sharedR,
            allT=allT,
            doneWithBlock=doneWithBlock,
            cond=cond,
            k=k,
            stream=cms.PatientStream(ds=id, maxds=nWorkers)
            )

    for x in boi:
        continue

    Q=boi.getEstimate()
    print Q.T[:,0:3]
    R=np.frombuffer(boi.sharedR.get_obj()).reshape((k,k))
    print R

    W=np.copy(np.dot(Q,np.linalg.cholesky(np.diag(np.diag(R)))))


# Do tensor stuff

    stream=cms.PatientStream(ds=id, maxds=nWorkers)
    stream.p=k
    boi=ParallelTopic(
            W=W,
            id=id,
            sharedQ=sharedQ2,
            sharedR=sharedR2,
            allT=allT2,
            doneWithBlock=doneWithBlock2,
            cond=cond2,
            k=k,
            stream=stream
            )

    for x in boi:
        continue

    Q=boi.getEstimate()
    print Q.T[:,0:3]
    R=np.frombuffer(boi.sharedR.get_obj()).reshape((k,k))
    print R

    if id==1:
        print "Saving results to disk"
        np.savetxt('cmsQTopic.txt',Q)
        np.savetxt('cmsRTopic.txt',R)
        np.savetxt('cmsCompTopic.txt',np.dot(Q,np.linalg.cholesky(R)))

    return
                    
if __name__ == "__main__":
    t0 = time.time()

    p=260
    k=3
    nWorkers=2

    sharedQ = Array('d', p*k)
    sharedR = Array('d', k*k)
    allT = Array('I', nWorkers,lock=False)
    doneWithBlock = Array('I', nWorkers,lock=False)
    cond = Condition()

    sharedQ2 = Array('d', k*k)
    sharedR2 = Array('d', k*k)
    allT2 = Array('I', nWorkers,lock=False)
    doneWithBlock2 = Array('I', nWorkers,lock=False)
    cond2 = Condition()

    processes=[]

    for id in xrange(1,nWorkers+1):
        arg={'id':id,'k':k,'nWorkers':nWorkers,
                'sharedQ':sharedQ,'sharedR':sharedR,'allT':allT,'doneWithBlock':doneWithBlock,'cond':cond,
                'sharedQ2':sharedQ2,'sharedR2':sharedR2,'allT2':allT2,'doneWithBlock2':doneWithBlock2,'cond2':cond2 
                }
        processes += [Process(target=runBOI, kwargs=arg)]
        processes[-1].start()

    # Join them
    for id in xrange(1,nWorkers+1):
        processes[id-1].join()

    t1 = time.time()
    total = t1-t0
    print "Total time: ", total

