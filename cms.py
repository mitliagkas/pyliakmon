import json
import re
import csv
import numpy as np
from streaming import *
from zipfile import ZipFile

class PatientStream(DataStream):
    def __init__(self, ds=1, maxds=1):
        self._nICDcodes=132
        self._nCPTcodes=128
        self.p=self._nICDcodes+self._nCPTcodes
        self.ds=ds
        self.delta=1
        self.sparse=False

        directory="/var/datasets/cms/"

        dataset=[
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip",	'n':49279},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip",	'n':49350},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_2A.zip",	'n':49154},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_2B.zip",	'n':49535},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_3A.zip",	'n':49344},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_3B.zip",	'n':49353},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_4A.zip",	'n':49345},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_4B.zip",	'n':49196},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_5A.zip",	'n':49311},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_5B.zip",	'n':49270},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_6A.zip",	'n':49378},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_6B.zip",	'n':49312},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_7A.zip",	'n':49368},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_7B.zip",	'n':49354},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_8A.zip",	'n':49143},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_8B.zip",	'n':49268},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_9A.zip",	'n':49146},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_9B.zip",	'n':49395},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_10A.zip",	'n':49491},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_10B.zip",	'n':49007},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.zip",	'n':49442},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_11B.zip",	'n':49223},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_12A.zip",	'n':49383},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_12B.zip",	'n':49409},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_13A.zip",	'n':49341},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_13B.zip",	'n':49294},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_14A.zip",	'n':49315},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_14B.zip",	'n':49327},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_15A.zip",	'n':49357},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_15B.zip",	'n':49257},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_16A.zip",	'n':49392},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_16B.zip",	'n':49343},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_17A.zip",	'n':49531},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_17B.zip",	'n':49203},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_18A.zip",	'n':49308},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_18B.zip",	'n':49341},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_19A.zip",	'n':49281},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_19B.zip",	'n':49431},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_20A.zip",	'n':49305},
                    {'file':directory+"DE1_0_2008_to_2010_Carrier_Claims_Sample_20B.zip",	'n':49414},
                ]


        file=dataset[ds-1]['file']
        self.n=sum([x['n'] for x in dataset[:maxds]])

        self.n=5000

        print "Reading", self.n, "samples from file", file

        self.t=0

        self._pidIdx=None
        self._diagIdx=None
        self._hcpcsIdx=None
        self._lastRow=None
        self._patientId=None

        self._patVector=np.zeros((self.p,1))

        zf=ZipFile(file)
        self.reader=csv.reader(zf.open(zf.namelist()[0]))
        row=self.reader.next()
        self._pidIdx  = [i for i, item in enumerate(row) if re.search('DESYNPUF_ID', item)][0]
        self._diagIdx = [i for i, item in enumerate(row) if re.search('ICD9_DGNS', item)]
        self._hcpcsIdx= [i for i, item in enumerate(row) if re.search('HCPCS_CD', item)]

        with open('db/cpt.json', 'rb') as outfile:
            self.procHier = json.load(outfile)
            outfile.close()

        with open('db/icd.json', 'rb') as outfile:
            self.icdHier = json.load(outfile)
            outfile.close()

    def __iter__(self):
        return self

    def __next__(self):
        np=self.__nextPatient()
        self.t+=1
        return np

    def next(self):
        return self.__next__()

    def __nextPatient(self):
        while True:
            ## Get new row or use previously retrieved
            if self._lastRow:
                ## Previously retrieved row means it's the first 
                ## row of a new patient
                row=self._lastRow
                #self._patVector=np.zeros((self._nICDcodes+self._nCPTcodes,1))
                self._patVector[:]=0
                self._patientId=row[self._pidIdx]
            else:
                try:
                    row=self.reader.next()
                except StopIteration:
                    row=None
                self._lastRow=row

            if row==None:
                if self._patientId==None:
                    raise StopIteration
                else:
                    ##No more data. Just return collected patient vector
                    self._patientId=None
                    return self._patVector

            else: ## row!=None
                if self._patientId and row[self._pidIdx]!=self._patientId:
                    ## New user: Return.
                    ## At the next call, we'll proceed with the next patient
                    self._patientId=None
                    return self._patVector

                self._patientId=row[self._pidIdx]

                ##Proceed
                ## Get the basic values from the row
                diagArray, diagCat = self.getDiagnosis(row)
                for code in diagCat:
                    self._patVector[code]+=1
                
                hcpcsArray, hcpcsCat = self.getProc(row)
                for code in hcpcsCat:
                    self._patVector[code+self._nICDcodes]+=1

                ## Delete row, now that we've used it
                self._lastRow=None


    def _formatICD(self,code):
        """
        Given an ICD-9 code that has no period, format for the period
        """
        if not code:
            return None
        elif code.isdigit():
            codeLen = len(code)
            if codeLen == 3:
                return code + ".00"
            elif codeLen == 4:
                return code[:3]+"."+ code[3:]+"0"
            elif codeLen == 5:
                return code[:3] + "." + code[3:]
        elif code[0] == 'V':
            return code[:3]+"."+code[3:]
        elif code[0] == 'E':
            return code[:4] + "."+code[4:]
        return code

    def getDiagnosis(self, row):
        """
        Get all the diagnosis values from a row and weed out the empty ones
        """
        diagArray = [row[i] for i in self._diagIdx]
        diagArray = filter(None, diagArray)
        diagArray = [self._formatICD(k) for k in diagArray]
        diagCatArray = [self._formatLevel2(self.icdHier, k) for k in diagArray]
        diagCatArray = filter(None, diagCatArray)
        return diagArray, diagCatArray

    def _formatLevel2(self, codeHier, code):
        """
        Given a code, get the level2 value
        """
        if code in codeHier:
            return codeHier[code]['level2']
        return None

    def getProc(self, row):
        """
        Get all the procedure codes from a row
        """
        #procArray = np.array(row)[procIdx]
        procArray = [row[i] for i in self._hcpcsIdx]
        procArray = filter(None, procArray)
        procCatArray = [self._formatLevel2(self.procHier, k) for k in procArray]
        procCatArray = filter(None, procCatArray)
        return procArray, procCatArray

