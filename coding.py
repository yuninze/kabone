import itertools
import random
import os
import pandas as pd

BL="c:/code/mkc.ac.kr/origin.csv"
BLCOLS=[
    "trainingSemester",
    "idx",
    "trainingClass",
    "trainingSerie",
    "trainingCompany",
    "trainingPeriod",
    "name",
    "registerProblem",
    "klass",
    "trainingTeacher",
    "trainingTeacherReal",
    "trainingUnit",
    "address",
    "contact",
    "trainingGroup",
    "trainingClassYear",
    "trainingClassCredit",
    "trainingClassCreditMoney",
    "trainingLeader",
    "trainingLeaderDepartment",
    "trainingLeaderPosition",
    "trainingLeaderDegree",
    "trainingLeaderRn",
    "trainingLeaderExperience",
    "enc"
]

class TrainingData:
    def __init__(self,data,
        objectColumns=[
            "idx",
            "name",
            "trainingClass",
            "trainingPeriod",
            "trainingCompany",
            "trainingUnit",
            "trainingTeacher",
            "address",
            "contact"
        ],
        charMapDataPath="c:/code/charMapData.csv"
    ):
        self.data=data
        
        self.cols=objectColumns.copy()
        if "enc" in self.cols:
            self.cols.remove("enc")

        self.charMapDataPath=charMapDataPath

        if self.data.enc.sum()==self.data.shape[0]:
            self.encoded=True
        elif self.data.enc.sum()==0:
            self.encoded=False
        else:
            raise ValueError(f"{self.data.enc.sum()=}")

        if os.path.exists(charMapDataPath):
            self.charMapData=pd.read_csv(self.charMapDataPath)
            self.getCharMap()
        else:
            self.charMapData=None
            self.charMap=None
            self.charMapInverse=None
            self.sieve=None
        return 

    def _createCharMap(self):
        if self.encoded==False:
            charIn=list(set(itertools.chain.from_iterable(
                ["".join(w) for w in [self.data[q].astype(str).unique() for q in self.cols]]
            )))
            sieve=random.sample(range(len(charIn)),k=len(charIn))
            charOut=[charIn[q] for q in sieve]

            if (len(charIn)==len(charOut)) + (len(charIn)==len(sieve)) < 2:
                raise Exception("Char number is unmatching")

            self.charMapData=pd.DataFrame(
                {"charIn":charIn,"charOut":charOut,"sieve":sieve})
        
        self.getCharMap()
        return 

    def getCharMap(self):
        self.charMap=dict(zip(self.charMapData.charIn,self.charMapData.charOut))
        self.charMapInverse=dict(zip(self.charMap.values(),self.charMap.keys()))
        self.sieve=self.charMapData.sieve.tolist()
        return 
    
    def getNewCharMap(self):
        self._createCharMap()
        return 
    
    def saveCharMapData(self):
        if self.encoded==False:
            try:
                self.charMapData.to_csv(self.charMapDataPath,encoding="utf-8",index=0)
                return 
            except Exception as e:
                raise Exception(f"{e}")
        raise Exception("You're trying to save a new mapper irrelevant to current data.")
    
    def _code(self,direction):
        if direction=="in" and self.encoded:
            raise Exception("Data already been enc.")
        if direction=="out" and self.encoded==False:
            raise Exception("This isn't encoded data.")

        if direction=="out":
            sieve=self.charMap
        elif direction=="in":
            sieve=self.charMapInverse
        else:
            raise Exception(f"Unknown direction {direction}")

        if sieve is None:
            raise Exception("Run getCharMap first.")
        
        self.data.loc[:,self.cols]=self.data.loc[:,self.cols].map(
            lambda w:"".join([sieve[q] for q in list(str(w))]) if isinstance(w,(str,int)) else w
        )

        if direction=="in":
            self.encoded=True
            self.data.loc[:,"enc"]=True
        elif direction=="out":
            self.encoded=False
            self.data.loc[:,"enc"]=False
        
        return self.data.sample(3)
    
    def encode(self):
        return self._code(direction="in")
    
    def decode(self):
        return self._code(direction="out")

def getBySemester(data,semester):
    return data.loc[:,semester].copy()

def getColsOrdered(data,colsOrdered):
    return data.loc[:,colsOrdered].copy()

def pasteClinicalLeader(data,semester,q):
    indices=["trainingCompany","trainingUnit"]
    data=getBySemester(data,semester).set_index(indices)
    q=q.set_index(indices)
    data.update(q,overwrite=True)
    return data.loc[:,BLCOLS].copy()

origin=pd.read_csv(BL)