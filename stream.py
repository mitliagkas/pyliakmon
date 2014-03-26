import cPickle as pickle
import numpy as np
from fcntl import fcntl, F_GETFL, F_SETFL
import os
import select

class DataStream:
    def __init__(self, n=0):
        self.n=n

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def next(self):
        return self.__next__()

    def getStreamAtrributes(self):
        return {'n':self.n}

class VectorStream(DataStream):
    def __init__(self, p, sparse=False, delta=1.0, **kwargs):
        DataStream.__init__(self,**kwargs)
        self.p=p
        self.sparse=sparse
        self.delta=delta

        self.t=0

    def getStreamAtrributes(self):
        attr=DataStream.getStreamAtrributes(self)
        attr['p']=self.p
        attr['sparse']=self.sparse
        attr['delta']=self.delta
        return attr

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def next(self):
        self.__next__()


class PipeConsumerStream(DataStream):
    def __init__(self, pipe, **kwargs):
        DataStream.__init__(self,**kwargs)
        self.pipe=pipe
        self.t=0
        self.unpickler=pickle.Unpickler(self.pipe)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            next=self.unpickler.load()
        except EOFError as e:
            raise StopIteration

        self.t+=1
        return next

    def next(self):
        return self.__next__()

class StreamIMUX():
    def __init__(self, stream, nPipes=1):
        self.stream=stream
        self.nPipes=nPipes
        self.__setupPipes()
        self.nextChild=0
        return

    def __setupPipes(self):
        self.parentFiles=[]
        self.picklers=[]
        self.childrenStreams=[]
        self.childrenBuffers=[]
        for i in range(self.nPipes):
            rfd, wfd = os.pipe()

            # Make parent FD non-blocking
            flags = fcntl(wfd, F_GETFL)
            fcntl(wfd, F_SETFL, flags | os.O_NONBLOCK)

            parentFile=wfd
            self.parentFiles+=[parentFile]

            childFile=os.fdopen(rfd, 'rb')
            childStream=PipeConsumerStream(pipe=childFile)

            # Give the new stream all of the attributes of the original stream
            attr=self.stream.getStreamAtrributes()
            for key in attr:
                setattr(childStream, key, attr[key])

            self.childrenStreams+=[childStream]
            self.childrenBuffers+=[""]

    def getChildrenStreams(self):
        return self.childrenStreams

    def getParentFiles(self):
        return self.parentFiles
    
    def __iter__(self):
        return self

    def _producer(self):
        try:
            next=self.stream.next()
        except StopIteration:
            print "Producer done after", self.stream.t, "samples"
            for i in range(self.nPipes):
                os.close(self.parentFiles[i])
            raise StopIteration

        # Pickle new sample
        pickledItem=pickle.dumps(next,-1)

        # and load it into an empty local buffer
        while True:
            if len(self.childrenBuffers[self.nextChild])==0:
                self.childrenBuffers[self.nextChild]=pickledItem
                self.nextChild=(self.nextChild+1) % self.nPipes
                break
            self.nextChild=(self.nextChild+1) % self.nPipes
        
        # Push more data from all local buffers into pipes
        # Scan through all buffers to push stuff
        b=-1
        emptiedAnyBuffer=False
        while True:
            b=(b+1) % self.nPipes
            # At the beginning of every pass over the buffers,
            if b==0:
                # check whether at least on buffer has been completely emptied
                if emptiedAnyBuffer:
                    break
                # and wait for writable pipes
                rlist, wlist, xlist=select.select([],self.parentFiles,[])

            # Skipping non-writable buffers
            if self.parentFiles[b] not in wlist:
                continue

            ret=os.write(
                        self.parentFiles[b],
                        self.childrenBuffers[b]
                        )
            if ret<len(self.childrenBuffers[b]):
                # Retain what was not transmitted in the local buffer
                self.childrenBuffers[b]=self.childrenBuffers[b][ret:]
            else:
                # Return only after we've completely emptied a local buffer
                # This keeps all the buffers controlled
                # No local buffer will exceed the maximum sample size
                self.childrenBuffers[b]=""
                emptiedAnyBuffer=True

    def __next__(self):
        self._producer()

    def next(self):
        self.__next__()



