import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import *
#import MovieLensNew as MovieLens
import MovieLens
from multiprocessing import Process
from multiprocessing import Array
import sys
 
def runBOI(stream, k,id,arr,allT,nWorkers,doneWithBlock,cond, parentFiles):

    # Sapirla: Workaround for this issue:
    #    http://bugs.python.org/issue12488
    for pc in parentFiles:
        os.close(pc)

    sys.stderr = open("logs/Worker"+str(id)+".out", "a")
    #sys.stderr.write('\n')

    boi=ParallelBlockOrthogonal(
            id=id,
            arr=arr,
            allT=allT,
            doneWithBlock=doneWithBlock,
            cond=cond,
            k=k,
            order=3,
            stream=stream
            )

    for x in boi:
        continue

    print boi.getEstimate().T[:,0:3]

    print np.dot(boi.getEstimate().T,np.loadtxt('mlpc.txt'))

    return boi.getEstimate()
                    
if __name__ == "__main__":
    t0 = time.time()

    p=65133
    k=2
    nWorkers=2

    arr = Array('d', p*k)
    allT = Array('I', nWorkers,lock=False)
    doneWithBlock = Array('I', nWorkers,lock=False)
    cond = Condition()

# Producer Stream
    producer=StreamIMUX(
            stream=MovieLens.UserStream(sparse=True, file='/var/datasets/ml-10M100K/ratingsTab.dat'),
            nPipes=nWorkers
            )

    childrenStreams=producer.getChildrenStreams()

    parentFiles=producer.getParentFiles()

    processes=[]

    for id in xrange(1,nWorkers+1):
        arg={   'id':id,
                'stream':childrenStreams[id-1],
                'k':k,
                'arr':arr,
                'allT':allT,
                'nWorkers':nWorkers,
                'doneWithBlock':doneWithBlock,
                'cond':cond,
                'parentFiles':parentFiles
            }
        processes += [Process(target=runBOI, kwargs=arg)]
        processes[-1].start()

    # Produce (serve samples to the created pipes)
    for x in producer:
        continue

    # Join them
    for id in xrange(1,nWorkers+1):
        processes[id-1].join()

    t1 = time.time()
    total = t1-t0
    print "Total time: ", total

    #print np.dot(results[0].T,results[1])

