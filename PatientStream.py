import json
#import itertools
import re
import csv
import numpy as np
import streaming

class PatientStream(streaming.SampleStream):
    def __init__(self, file):
        self._pidIdx=None
        self._diagIdx=None
        self._hcpcsIdx=None
        self._lastRow=None
        self._patientId=None
        self._nICDcodes=132
        self._nCPTcodes=128
        super().__init__(self,p=self._nICDcodes+self._nCPTcodes)
        self._patVector=np.zeros((self._nICDcodes+self._nCPTcodes,1))

        self.reader=csv.reader(file)
        row=self.reader.next()
        self._pidIdx = [i for i, item in enumerate(row) if re.search('DESYNPUF_ID', item)][0]
        self._diagIdx = [ i for i, item in enumerate(row) if re.search('ICD9_DGNS', item)]
        self._hcpcsIdx = [ i for i, item in enumerate(row) if re.search('HCPCS_CD', item)]

        with open('db/cpt.json', 'rb') as outfile:
            self.procHier = json.load(outfile)
            outfile.close()

        with open('db/icd.json', 'rb') as outfile:
            self.icdHier = json.load(outfile)
            outfile.close()

    def __iter__(self):
        return self

    def __next__(self):
        np=nextPatient(self)
        self.t+=1
        return np

    def __nextPatient(self):
        while True:
            ## Get new row or use previously retrieved
            if self._lastRow:
                ## Previously retrieved row means it's the first 
                ## row of a new patient
                row=self._lastRow
                self._patVector=np.zeros((self._nICDcodes+self._nCPTcodes,1))
                #self._patVector[:]=0
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

