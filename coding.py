import itertools
import random
import os
import pandas as pd


cols=[
    "trainingSemester",
    "idx",
    "trainingClass",
    "trainingSerie",
    "trainingCompany",
    "trainingPeriod",
    "name",
    "registerProblem",
    "trainingTeacher",
    "trainingUnit",
    "trainingGroup",
    "trainingLeader",
    "trainingLeaderDepartment",
    "trainingLeaderPosition",
    "trainingLeaderDegree",
    "trainingLeaderRn",
    "trainingLeaderExperience",
    "trainingClassYear",
    "trainingClassCredit",
    "trainingClassCreditMoney",
    "trainingTeacherReal",
    "address",
    "contact",
    "klass",
    "enc"
]

colsIncomplete=[
    "trainingGroup",
    "trainingUnit",
    "trainingLeaderDepartment"
]

viewTrainingSemester={
    "3-2.5-2022":("2023","3","동계"),
    "3-1.0-2023":("","",),
    "3-1.5-2023":("",""),
    "3-2.0-2023":("",""),
    "3-2.5-2023":("",""),
    "4-1.0-2023":("",""),
    "4-1.5-2023":("",""),
}


def trDataBongsoon(data:pd.DataFrame,exclude="3-2.5-2023"):
    now=data[data.trainingSemester!=exclude].copy()
    fut=data[data.trainingSemester==exclude].copy()

    q=pd.DataFrame({
        
    })

    return {"now":now,"fut":fut}

def trDataKabone(data:pd.DataFrame,exclude="3-2.5-2023"):
    data=data[data.trainingSemester!=exclude].copy()

    data.loc[:,colsIncomplete]=data.loc[:,colsIncomplete].fillna("#")

    trainingScene=data[[
        "trainingLeader",
        "trainingLeaderPosition",
        "trainingLeaderDegree",
        "trainingLeaderRn",
        "trainingLeaderExperience"
    ]].rename({
        "trainingLeader":"성명",
        "trainingLeaderPosition":"직위",
        "trainingLeaderDegree":"최종학위",
        "trainingLeaderRn":"간호사 면허 유무",
        "trainingLeaderExperience":"임상경력"
    },axis=1)
    trainingScene.loc[:,"간호사 면허 유무"]="유"

    q=pd.DataFrame({
        "연도":"2023",
        "학기":data.trainingSemester.apply(
            lambda q:"1" if q.endswith("1") or q.endswith("2.5") else "2"
        ),
        "실습기관명":data.trainingCompany,
        "실습단위":data[["trainingGroup","trainingUnit","trainingLeaderDepartment"]].apply(
            lambda q:f"{q.iat[0]}({q.iat[1]}/{q.iat[2]})",
            axis=1
        ),
        "교과목명":data.trainingClass
    })

    indices=["연도","학기","실습기관명","실습단위"]

    q=pd.concat([q,trainingScene],axis=1).sort_values(indices).drop_duplicates(indices,ignore_index=True)

    return q


class Tr:
    def __init__(self,data,key,
        charMapDataPath="c:/code/charmapdata.csv"
    ):
        self.data=data.copy()
        self.cols=self.data.select_dtypes(object).columns
        self.key=key

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
    
    def __repr__(self):
        _className=self.__class__.__name__
        return f"{_className}(shape={self.data.shape}, encoded={self.data.enc.all()})"
    
    def _createCharMap(self):
        if self.encoded==False:
            charIn=list(set(itertools.chain.from_iterable(
                ["".join(w) for w in [self.data[q].astype(str).unique() for q in self.cols]]
            )))
            
            gen=np.random.default_rng(seed=key)
            sieve=gen.choice(len(charIn),size=len(charIn),replace=False)
            
            charOut=[charIn[q] for q in sieve]

            if (len(charIn)==len(charOut)) + (len(charIn)==len(sieve)) < 2:
                raise Exception("Char number is unmatching")

            self.charMapData=pd.DataFrame(
                {"charIn":charIn,"charOut":charOut,"sieve":sieve})
        
            try:
                self.charMapData.to_csv(self.charMapDataPath,encoding="utf-8",index=0)
            except Exception as e:
                raise Exception(f"{e}")
            
        self.charMap=dict(zip(self.charMapData.charIn,self.charMapData.charOut))
        self.charMapInverse=dict(zip(self.charMap.values(),self.charMap.keys()))
        self.sieve=self.charMapData.sieve.tolist()
    
    def _code(self,direction):
        if direction=="in" and self.encoded:
            raise Exception("Data already been encoded.")
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


### Hx Methods
def getBySemester(data,semester):
    return data.loc[data.trainingSemester==semester,:].copy()

def pasteClinicalLeader(data,q):
    indices=["trainingSemester","trainingCompany","trainingUnit"]
    data=data.set_index(indices)
    q=q.set_index(indices)
    data.update(q,overwrite=True)
    return data.loc[:,cols]
