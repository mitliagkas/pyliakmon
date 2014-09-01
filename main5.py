import numpy as np
from numpy import linalg as la
import time, datetime
import subprocess
from streaming import *
import cms
from multiprocessing import Process
from multiprocessing import Array
import sys


def runBOI(k, id, sharedQ, sharedR, allT, nWorkers, doneWithBlock, cond):
    sys.stderr = open("logs/Worker" + str(id) + ".out", "a")
    stream = cms.PatientStream(ds=id, maxds=nWorkers)

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

    return


if __name__ == "__main__":
    t0 = time.time()

    p = 260
    k = 3
    nWorkers = 4

    sharedQ = Array('d', p * k)
    sharedR = Array('d', k * k)
    allT = Array('I', nWorkers, lock=False)
    doneWithBlock = Array('I', nWorkers, lock=False)
    cond = Condition()

    processes = []

    for id in xrange(1, nWorkers + 1):
        arg = {'id': id, 'k': k, 'sharedQ': sharedQ, 'sharedR': sharedR, 'allT': allT, 'nWorkers': nWorkers,
               'doneWithBlock': doneWithBlock, 'cond': cond}
        processes += [Process(target=runBOI, kwargs=arg)]
        processes[-1].start()

    # Join them
    for id in xrange(1, nWorkers + 1):
        processes[id - 1].join()

    t1 = time.time()
    total = t1 - t0
    print "Total time: ", total

