import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import Oja
from streaming import BlockOrthogonal
import MovieLens

                    
if __name__ == "__main__":

    boi1=BlockOrthogonal(k=1,stream=MovieLens.UserStream(sparse=False, file='/var/datasets/ml-10M100K/ratingsTab.dat'))

    t0 = time.time()
    
    for x in boi1:
        continue

    t1 = time.time()
    total = t1-t0
    print "Total time: ", total

    print boi1.getEstimate().T
    np.savetxt('mlpc.txt',boi1.getEstimate())

