import numpy as np
import math
from multiprocessing import Condition
from stream import *

class StreamingAlgo:
    def __init__(self, dim=None, n=0, stream=None):
    # stream here is assumed to be an instance of VectorStream
        if stream:
            dim=stream.p
            if stream.n:
                n=stream.n

        self.p=dim
        self.n=n
        self.t=0
        self.stream=stream

    def _getNextSample(self):
        nxt=self.stream.next()
        
        if self.stream.sparse:
            next=np.zeros((self.p,1))
            for x in nxt:
                next[x]=nxt[x]
            return next
        else:
            return nxt

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def next(self):
        return self.__next__(self)

    def _incrementTime(self):
        self.t+=1

    def _update(self, sample):
        self._incrementTime()

    def getEstimate(self):
        return None

class BlockOrthogonal(StreamingAlgo):
    def __init__(self, order=2, k=1, C=0.25, **kwargs):
        StreamingAlgo.__init__(self,**kwargs)
        self.C=C
        self.k=k
        self.order=order

        self.id=1
        self.done=False

        self.T=math.ceil(self.C*math.log(self.stream.p*self.stream.n*self.stream.delta/self.k))
        print "Using", int(self.T), "blocks"
        self.B=int(math.floor(self.n/self.T))
        self.blocks=range(self.B,self.B*(int(self.T)+1),self.B)
        self.blocks[-1]=self.n
        print self.blocks
        assert(len(self.blocks)==self.T)
        self.nextBlock=1
        self.Q=np.ones((self.p,self.k))
        self.Qnew=np.zeros((self.p,self.k))

    def __iter__(self):
        return self

    def __next__(self):
        if self.done:
            raise StopIteration

        try:
            self._update(self._getNextSample())
        except StopIteration:
            self.done=True
            
        if  self.done or self.getTime() >= self.blocks[self.nextBlock-1]:
            self._orthonormalize()
            self.nextBlock+=1
            if self.nextBlock>self.T:
                self.done=True
        return self.getEstimate()

    def next(self):
        return self.__next__()

    def getTime(self):
        return self.t

    def _orthonormalize(self):
        print "Finished block ", self.nextBlock, "after", self.getTime(), "samples"
        [self.Q,self.R]=np.linalg.qr(self.Qnew/self.B)
        # Flip signs to get nice non-negative diagonal in R.
        # Makes life easier.
        P=np.diag(np.sign(np.diag(self.R)))
        self.Q=np.dot(self.Q,P)
        self.R=np.dot(P,self.R)

        self.Qnew=np.zeros((self.p,self.k))

    def _update(self,sample):
        StreamingAlgo._update(self,None)
        #self.Qnew = self.Qnew + sample.dot(sample.T.dot(self.Q))
        self.Qnew = self.Qnew + sample.dot(
                    np.power(sample.T.dot(self.Q), [self.order-1]*self.k)
                )
        if self.getTime() % 2000 == 0:
            print self.Qnew.T[0,0:3]

    def getEstimate(self):
        return self.Q

class ParallelBlockOrthogonal(BlockOrthogonal):
    def __init__(self, id, sharedQ, sharedR, allT, doneWithBlock, cond, **kwargs):
        BlockOrthogonal.__init__(self,**kwargs)

        self.id=id
        self.arr=sharedQ
        self.sharedR=sharedR
        self.allT=allT
        self.allT[id-1]=0
        self.doneWithBlock=doneWithBlock
        self.cond=cond

    def getTime(self):
        return sum(self.allT)
        
    def _orthonormalize(self):
        print "Worker", self.id, "finished block ", self.nextBlock, "after", self.allT[self.id-1], "/", self.getTime(), "samples"
        # Blocking here to add contribution
        with self.arr.get_lock(): # synchronize access
            first=not any(self.doneWithBlock)
            if first:
                self.arr[:]=self.Qnew.reshape(self.p*self.k)[:]
            else:
                self.arr[:]=self.arr[:]+self.Qnew.reshape(self.p*self.k)[:]
            self.doneWithBlock[self.id-1]=True
            # The last process will do the QR
            if all(self.doneWithBlock):
                performQR=True
            else:
                performQR=False

        # Synchronize to perform QR/wait
        self.cond.acquire()
        if performQR:
            QnewTotal=np.frombuffer(
                                        self.arr.get_obj()
                                   ).reshape(self.p,self.k)
            # Rescale to get right scaling in R
            QnewTotal=QnewTotal/self.B
            # Orthonormalize
            [Q,R]=np.linalg.qr(QnewTotal)

            # Flip signs to get nice non-negative diagonal in R.
            # Makes life easier.
            P=np.diag(np.sign(np.diag(R)))
            Q=np.dot(Q,P)
            R=np.dot(P,R)

            self.arr[:]=Q.reshape(self.p*self.k)[:]
            self.sharedR[:]=R.reshape(self.k*self.k)[:]
            print Q.T[:,0:3]

            self.cond.notify_all()
        else:
            # Block here again to wait for result
            self.cond.wait()
        self.cond.release()

        self.Q=np.copy(self.arr).reshape(self.p,self.k)

        self.Qnew=np.zeros((self.p,self.k))
        self.doneWithBlock[self.id-1]=0
        return

    def _incrementTime(self):
        self.allT[self.id-1]+=1

class ParallelTopicWhiten(ParallelBlockOrthogonal):
    def __init__(self, **kwargs):
        ParallelBlockOrthogonal.__init__(self,**kwargs)

        self.lambdas=np.ones((self.k,1))

        #self.Q=np.ones((self.p,self.k))/math.sqrt(float(self.p))
        self.Q=np.random.randn(self.p,self.k)/math.sqrt(float(self.p))
        self.Q[:,1:]=np.random.randn(self.p,self.k-1)/math.sqrt(float(self.p))

        self.sharedR[:]=np.eye(self.k).reshape(self.k*self.k)[:]
        self.R=np.frombuffer( self.sharedR.get_obj()
                                   ).reshape(self.k,self.k)


    def _update(self,sample):
        StreamingAlgo._update(self,None)
# NNZ
        l=sum(sample!=0)
        if l<2:
            return

        for ik in range(self.k):
        # IP
            ip=sample.T.dot(self.Q[:,ik])
        # Q elementwise square
            Qslice=self.Q[:,ik].reshape((self.p,1))

        # Unscaled contribution
            contrib=ip*sample
            contrib-=np.multiply(sample, Qslice)
        # Scaling
            #scaling=math.factorial(l-3)/float(math.factorial(l))
            # Equivalent but faster
            scaling=1/float(l*(l-1))

            self.Qnew[:,ik] = self.Qnew[:,ik] + scaling*contrib.reshape((self.p,))

        if self.getTime() % 2000 == 0:
            print "New",  self.Qnew.T[0,0:3]


class ParallelTopic(ParallelBlockOrthogonal):
    def __init__(self, W, **kwargs):
        ParallelBlockOrthogonal.__init__(self,**kwargs)

        self.lambdas=np.ones((self.k,1))

        self.Q=np.ones((self.p,self.k))/float(self.p)

        self.sharedR[:]=np.eye(self.k).reshape(self.k*self.k)[:]
        self.R=np.frombuffer( self.sharedR.get_obj()
                                   ).reshape(self.k,self.k)
        self.W=W


    def _update(self,sample):
        StreamingAlgo._update(self,None)
# NNZ
        l=sum(sample!=0)
        if l<3:
            return

# Conversion:
# 1. Replace Q[:,ik] with W*Q[:,ik]
# 2. Left multiply result with W'

        for ik in range(self.k):
        # Q elementwise square
            Qslice=self.Q[:,ik].reshape((self.p,1))
            # Multiply with whitening
            Qslice=np.dot(self.W,Qslice)
            Qsq=np.square(Qslice)
        # IP
            ip=sample.T.dot(Qslice)

        # Unscaled contribution
            contrib=(ip**2)*sample
            contrib+=2*np.multiply(Qsq, sample)
            contrib-=2*ip*np.multiply(sample, Qslice)
            contrib-= sample.T.dot(Qsq)*sample

            # Multiply with whitening
            contrib=np.dot(self.W.T,contrib)

        # Scaling
            #scaling=math.factorial(l-3)/float(math.factorial(l))
            # Equivalent but faster
            scaling=1/float(l*(l-1)*(l-2))

        # Cancel previous components
            if ik>0: 
                contrib-=self.R[ik-1,ik-1]*self.Q[:,ik-1].reshape((self.p,1))*np.dot(
                                self.Q[:,ik-1].reshape((self.p,1)).T,
                                self.Q[:,ik].reshape((self.p,1))
                            )**2
            self.Qnew[:,ik] = self.Qnew[:,ik] + scaling*contrib.reshape((self.p,))

        if self.getTime() % 2000 == 0:
            print "New",  self.Qnew.T[0,0:3]

    def _orthonormalize(self):
        print "New worker", self.id, "finished block ", self.nextBlock, "after", self.allT[self.id-1], "/", self.getTime(), "samples"
        # Blocking here to add contribution
        with self.arr.get_lock(): # synchronize access
            first=not any(self.doneWithBlock)
            if first:
                self.arr[:]=self.Qnew.reshape(self.p*self.k)[:]
            else:
                self.arr[:]=self.arr[:]+self.Qnew.reshape(self.p*self.k)[:]
            self.doneWithBlock[self.id-1]=True
            # The last process will do the QR
            if all(self.doneWithBlock):
                performQR=True
            else:
                performQR=False

        # Synchronize to perform QR/wait
        self.cond.acquire()
        if performQR:
            QnewTotal=np.frombuffer(
                                        self.arr.get_obj()
                                   ).reshape(self.p,self.k)
            # Rescale to get right scaling in R
            QnewTotal=QnewTotal/self.B
            # Orthonormalize
            R=np.zeros((self.k, self.k))
            Q=np.copy(QnewTotal)
            
            for ik in range(self.k):
                R[ik,ik]=np.linalg.norm(Q[:,ik],ord=2)                
                Q[:,ik]=Q[:,ik]/R[ik,ik]

            # Flip signs to get nice non-negative diagonal in R.
            # Makes life easier.
            P=np.diag(np.sign(np.diag(R)))
            Q=np.dot(Q,P)
            R=np.dot(P,R)

            self.arr[:]=Q.reshape(self.p*self.k)[:]
            self.sharedR[:]=R.reshape(self.k*self.k)[:]
            print Q.T[:,0:3]

            self.cond.notify_all()
        else:
            # Block here again to wait for result
            self.cond.wait()
        self.cond.release()

        self.Q=np.copy(self.arr).reshape(self.p,self.k)

        self.Qnew=np.zeros((self.p,self.k))
        self.doneWithBlock[self.id-1]=0
        return


class Oja(StreamingAlgo):
    def __init__(self, k=1, Ceta=1, **kwargs):
        StreamingAlgo.__init__(self,**kwargs)
        self.Ceta=Ceta*math.sqrt(self.p)
        self.k=k

        self.Q=np.ones((self.p,self.k))

    def __iter__(self):
        return self

    def __next__(self):
        self._update(self._getNextSample())
        return self.getEstimate()

    def next(self):
        return self.__next__()

    def _update(self,sample):
        StreamingAlgo._update(self,None)
        self.Q = self.Q + (self.Ceta/float(self.t))*sample.dot(sample.T.dot(self.Q))
        [self.Q,R]=np.linalg.qr(self.Q)

    def getEstimate(self):
        return self.Q


