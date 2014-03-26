import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import Oja
from streaming import BlockOrthogonal
import MovieLens
from multiprocessing import Process
from multiprocessing import Array
 
def runBOI(k,id,acc,allT):
    boi=BlockOrthogonal(
            k=k,
            stream=MovieLens.UserStream(sparse=False, file='/var/datasets/ml-10M100K/ratingsTab2'+str(id)+'.dat')
            )

    for x in boi:
        continue

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
        acc = Array('d', (p,k))
        allT = Array('I', (nWorkers,1))

        processes=[]

        for id in xrange(1,nWorkers+1):
            arg={'id':id,'k':k,'acc':acc,'allT':allT}
            processes += [Process(target=runBOI, kwargs=arg)]
            processes[-1].start()

        # Join them
        for id in xrange(1,nWorkers+1):
            processes[id-1].join()

    t1 = time.time()
    total = t1-t0
    print "Total time: ", total

    #print np.dot(results[0].T,results[1])

