import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import *
import MovieLens
from multiprocessing import Process
from multiprocessing import Array
 
def runBOI(k,id,arr,allT,nWorkers,doneWithBlock,cond):
    boi=ParallelBlockOrthogonal(
            id=id,
            arr=arr,
            allT=allT,
            doneWithBlock=doneWithBlock,
            cond=cond,
            k=k,
            stream=MovieLens.UserStream(file='/var/datasets/ml-10M100K/ratingsTab'+str(nWorkers)+str(id)+'.dat')
            )

    for x in boi:
        continue

    print boi.getEstimate().T[:,0:3]

    print np.dot(boi.getEstimate().T,np.loadtxt('mlpc.txt'))

    return boi.getEstimate()
                    
if __name__ == "__main__":
    t0 = time.time()

    p=65133
    k=1
    nWorkers=2

    if False:
        pl = Pool(nWorkers)
        #results=pl.map(runBOI, [{'id':x,'k':k,'acc':acc,'allT':allT} for x in xrange(1,nWorkers+1)])
        #results=pl.map(runBOI, [{'id':x,'k':k} for x in xrange(1,nWorkers+1)])
    else:
        arr = Array('d', p*k)
        allT = Array('I', nWorkers,lock=False)
        doneWithBlock = Array('I', nWorkers,lock=False)
        cond = Condition()

        processes=[]

        for id in xrange(1,nWorkers+1):
            arg={'id':id,'k':k,'arr':arr,'allT':allT,'nWorkers':nWorkers,'doneWithBlock':doneWithBlock,'cond':cond}
            processes += [Process(target=runBOI, kwargs=arg)]
            processes[-1].start()

        # Join them
        for id in xrange(1,nWorkers+1):
            processes[id-1].join()

    t1 = time.time()
    total = t1-t0
    print "Total time: ", total


    #print np.dot(results[0].T,results[1])

