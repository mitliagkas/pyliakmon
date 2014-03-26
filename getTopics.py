import numpy as np
import json


with open('db/cpt.json', 'rb') as outfile:
    procHier = json.load(outfile)
    outfile.close()

with open('db/icd.json', 'rb') as outfile:
    icdHier = json.load(outfile)
    outfile.close()

icdMap=dict([(icdHier[x]['level2'],{'desc':icdHier[x]['desc'],'code':x}) for x in icdHier.keys()])
procMap=dict([(procHier[x]['level2'],{'desc':procHier[x]['desc'],'code':x}) for x in procHier.keys()])

pcs=np.loadtxt('results/cmsQOrder2.txt')

p,k=pcs.shape

# Get the

l=5

print 
print 

for c in range(k):
    print
    print "[Component", c+1, "]"
    comp=pcs[:,c]
    #comp=pcs[:,c]
    #ind=abs(comp).argsort()[-l:]
    if c>0:
        print "Positive Pole"

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

    if c>0:
        print "Negative Pole"
        ind=comp.argsort()[:l]
        ind=ind.tolist()
        for id,magnitude in [(x,comp[x]) for x in ind]:
            if id < 132:
            # ICD
                print "  ICD9", icdMap[id]['desc'].ljust(70), magnitude
            else:
            # Procedure
                id-=132
                print "  Proc", procMap[id]['desc'].ljust(70), magnitude


pcs=np.loadtxt('results/cmsCompOrder3.txt')
pcs=np.loadtxt('results/cmsQOrder2.txt')

p,k=pcs.shape

l=5

print 
print 

for c in range(k):
    print
    print "[Component", c+1, "]"
    comp=pcs[:,c]
    #comp=pcs[:,c]
    #ind=abs(comp).argsort()[-l:]
    if c>0:
        print "Positive Pole"

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

    if c>0:
        print "Negative Pole"
        ind=comp.argsort()[:l]
        ind=ind.tolist()
        for id,magnitude in [(x,comp[x]) for x in ind]:
            if id < 132:
            # ICD
                print "  ICD9", icdMap[id]['desc'].ljust(70), magnitude
            else:
            # Procedure
                id-=132
                print "  Proc", procMap[id]['desc'].ljust(70), magnitude






