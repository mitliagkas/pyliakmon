import numpy as np
from numpy import linalg as la
import time
import subprocess 
from streaming import Oja
from streaming import BlockOrthogonal
import MovieLens

                    
if __name__ == "__main__":

    #oja=Oja(Ceta=1e-3,k=1,stream=MovieLens.UserStream())
    boi1=BlockOrthogonal(k=1,stream=MovieLens.UserStream(sparse=False, file='/var/datasets/ml-10M100K/ratingsTab21.dat'))
    boi2=BlockOrthogonal(k=1,stream=MovieLens.UserStream(sparse=False, file='/var/datasets/ml-10M100K/ratingsTab22.dat'))

    t0 = time.time()
    
    oneDone=False
    twoDone=False

    while not (oneDone and twoDone):
        try:
            boi1.next()
        except StopIteration:
            oneDone=True
        try:
            boi2.next()
        except StopIteration:
            twoDone=True

    t1 = time.time()
    total = t1-t0
    print "Total time: ", total

    print np.dot(boi1.getEstimate().T,boi2.getEstimate())

