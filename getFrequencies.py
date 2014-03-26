import cms
import numpy as np

p=260

counts=np.zeros((p,1))

for i in xrange(1,41):
    stream=cms.PatientStream(ds=i)
    for x in stream:
        counts=counts+x
        
    np.savetxt('results/cmsFrequencies.txt',counts)

print counts.T


