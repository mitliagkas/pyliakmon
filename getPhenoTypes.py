import numpy as np
import json


with open('db/cpt.json', 'rb') as outfile:
    procHier = json.load(outfile)
    outfile.close()

with open('db/icd.json', 'rb') as outfile:
    icdHier = json.load(outfile)
    outfile.close()

with open('db/icd-level2.json', 'rb') as outfile:
    icdL2 = json.load(outfile)
    outfile.close()

with open('db/cpt-level2.json', 'rb') as outfile:
    procL2 = json.load(outfile)
    outfile.close()

icdMap=dict([(icdHier[x]['level2'],{'desc':icdL2[str(icdHier[x]['level2'])],'code':x}) for x in icdHier.keys()])
procMap=dict([(procHier[x]['level2'],{'desc':procL2[str(procHier[x]['level2'])],'code':x}) for x in procHier.keys()])
#procMap=dict([(procHier[x]['level2'],{'desc':procHier[x]['desc'],'code':x}) for x in procHier.keys()])

#pcs=np.loadtxt('cmsComp.txt') 
pcs=np.loadtxt('results/cmsCompOrder3.txt')
pcs=np.loadtxt('results/cmsQOrder2.txt')
pcs=np.loadtxt('results/cmsQOrder3.txt')
pcs=np.loadtxt('cmsQTopic.txt')

p,k=pcs.shape

l=8

print 
print 

for c in range(k):
    print
    print "[Component", c+1, "]"
    comp=pcs[:,c]
    #comp=pcs[:,c]
    #ind=abs(comp).argsort()[-l:]

    ind=comp.argsort()[-l:]
    ind=ind.tolist()
    ind.reverse()
    for id,magnitude in [(x,comp[x]) for x in ind]:
        if id < 132:
        # ICD
            print "  ICD9", icdMap[id]['desc'].ljust(70), magnitude
        else:
        # Procedure
            id-=132
            print "  Proc", procMap[id]['desc'].ljust(70), magnitude




