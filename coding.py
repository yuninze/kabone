import itertools
import os
import datetime
import numpy as np
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
    "3-2.5-2022":("2023","3","동계","1"),
    "3-1.0-2023":("2023","3","학기중","1"),
    "3-1.5-2023":("2023","3","하계","2"),
    "3-2.0-2023":("2023","3","학기중","2"),
    "3-2.5-2023":("2024","4","동계","1"),
    "4-1.0-2023":("2023","4","학기중","1"),
    "4-1.5-2023":("2023","4","하계","2"),
}

viewTrainingTeacherReal={
    True:"전임",
    False:"비전임"
}

viewTrainingPeriod={
    "성인간호학실습1":15,
    "성인간호학실습2":15,
    "성인간호학실습3":15,
    "아동간호학실습":15,
    "여성건강간호학실습":15,
    "지역사회간호학실습":15,
    "정신간호학실습":15,
    "통합간호실습":15,
    "간호관리학실습":15
}

def kabone(
        data:pd.DataFrame,
        exclude="3-2.5-2023",
        type=None
    )->pd.DataFrame:
    
    def _objectify(d,targetCols=colsIncomplete):
        d.loc[:,targetCols]=d.loc[:,targetCols].fillna("#")
        return d

    def _divide(e):
        divider=e.find("~")
        x=[pd.to_datetime(q,yearfirst=False) for q in (e[:divider].strip(),e[divider+1:].strip())]
        return x
    
    def _mapper(d:pd.DataFrame):
        return pd.DataFrame({
            "학번":d.idx,
            "성명":d.name.apply(
                lambda q:q.strip().replace("*","").replace(" ","")
            ),
            "성별":np.nan,
            "학년":d.trainingClassYear,
            "학기상세":d.trainingSemester.apply(
                lambda q:viewTrainingSemester[q][2]
            ),
            "연도":d.trainingSemester.apply(
                lambda q:viewTrainingSemester[q][0]
            ),
            "학기":d.trainingSemester.apply(
                lambda q:viewTrainingSemester[q][3]
            ),
            "과목명":d.trainingClass,
            "학점":d.trainingClassCredit,
            "시수":d.trainingClassCreditMoney,
            "과목명\n학년\n(학점x시수x주수=실습시간)":d.loc[:,["trainingClass","trainingClassYear","trainingClassCredit","trainingClassCreditMoney"]].apply(
                lambda q:f"{q.iat[0]}\n({q.iat[1]}학년)\n({q.iat[2]}학점\n×\n{q.iat[3]}시수\n×\n{viewTrainingPeriod[q.iat[0]]}주\n=\n{int(q.iat[2])*int(q.iat[3])*viewTrainingPeriod[q.iat[0]]}시간)",
                axis=1
            ),
            "실습기관명":d.trainingCompany,
            "실습병동(진료과)":d.loc[:,["trainingUnit","trainingLeaderDepartment"]].apply(
                lambda q:f"{q.iat[0]}\n({q.iat[1]})",
                axis=1
            ),
            "교과목 담당교원(전임)":d[["trainingTeacher","trainingTeacherReal"]].apply(
                lambda q:q.iat[0] if q.iat[1] else np.nan,
                axis=1
            ),
            "교과목 담당교원(비전임)":d[["trainingTeacher","trainingTeacherReal"]].apply(
                lambda q:q.iat[0] if q.iat[1]==False else np.nan,
                axis=1
            ),
            "교원구분":d.trainingTeacherReal.apply(
                lambda q:viewTrainingTeacherReal[q] if pd.notna(q) else "#"
            ),
            "실습단위":d.trainingGroup.str.strip(),
            "실습일자":d.trainingPeriod.apply(lambda q:q[:q.find("(")].replace(" ","")),
            "실습숙소사용":d.dormUsed.apply(
                lambda q:"N" if q=="#" else "Y"
            )
        })
    
    def _5555(data:pd.DataFrame):
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

        x=pd.DataFrame({
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

        final=pd.concat([x,trainingScene],axis=1).sort_values(indices).drop_duplicates(indices,ignore_index=True)
        
        return q
        
    def _2232(d:pd.DataFrame)->pd.DataFrame:

        trGroupUnitIndices=[
            "연도",
            "학기",
            "과목명\n학년\n(학점x시수x주수=실습시간)",
            "실습기관명",
            "실습병동(진료과)"
        ]
        
        d_=d.loc[:,[
            "연도",
            "학기",
            "과목명",
            "과목명\n학년\n(학점x시수x주수=실습시간)",
            "실습기관명",
            "실습병동(진료과)",
            "교과목 담당교원(전임)",
            "교과목 담당교원(비전임)",
            "실습단위",
            "학번",
            "실습일자",
            "학년",
        ]]

        e=d_.groupby(trGroupUnitIndices)

        trGroups=[", ".join(q) for q in e.실습단위.unique()]
        trPeriod=[",\n".join(q) for q in e.실습일자.unique()]
        trGroupsIdxs=e.학번.nunique().rename("학생배치인원").reset_index().assign(
            실습단위명=trGroups,
            실습기간=trPeriod,
        )

        final=trGroupsIdxs.merge(
            d_[trGroupUnitIndices+["교과목 담당교원(전임)","교과목 담당교원(비전임)"]],
            on=trGroupUnitIndices,
            how="left"
        ).drop_duplicates()

        final=final.assign(
            계=final.loc[:,trGroupUnitIndices[2]].apply(
                lambda q:"".join([w for w in q[-7:] if w.isdigit()])
            )
        )
        
        return final
        
    now=data[data.trainingSemester.ne(exclude)]
    fut=data[data.trainingSemester.eq(exclude)]
    
    if type==5555:
        return _5322(_objectify(now))
    
    x=[_mapper(_objectify(x)) for x in (now,fut)]
    
    if type==2232:
        return _2232(x[0])
    elif type is None:
        return x

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
            
            gen=np.random.default_rng(seed=self.key)
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

### Hx
def getBySemester(data,semester):
    return data.loc[data.trainingSemester==semester,:].copy()

def pasteClinicalLeader(data,q):
    indices=["trainingSemester","trainingCompany","trainingUnit"]
    data=data.set_index(indices)
    q=q.set_index(indices)
    data.update(q,overwrite=True)
    return data.loc[:,cols]

## 산협
def hyoupyak(fpname:str,final:str="2024-03-01")->pd.DataFrame:
    hyColumnView={
        "연도":"hyYear",
        "기관":"trainingCompany",
        "목적":"hyPurpose",
        "기간":"hyDuration",
        "최종일":"hyDurationLast",
        "잔존일":"hyTheta",
        "행사":"hyExec",
    }

    d=pd.read_csv(fpname)

    def _sanitiseComma(d:pd.DataFrame)->pd.DataFrame:
        hy.loc[:,"hyPurpose"]=hy.hyPurpose.apply(
            lambda q:q.strip().replace(" ","").split(",")
        )

    if any(list(map(isascii(),list(d.columns[-1])))):
        hy.columns=list(map(lambda q:hyColumnView[q],hy.columns))
        hy=_sanitiseComma(d).explode("hyPurpose")

    def _sanitiseDatestring(e):
        e=e.strip().replace(" ","")

        if ~e.startswith("~"):
            e=e.replace("~","")
        
        if ~e.endswith("."):
            e=e+"."

        return e

    final=pd.to_datetime(final)

    hy["hyDurationLast"]=hy.hyDuration.apply(
        lambda q:q[-11:]).map(_sanitiseDatestring).pipe(pd.to_datetime,format="mixed",dayfirst=False)
    
    hy["hyTheta"]=(hy.hyDurationLast-final).apply(lambda q:q.days)
    
    hy["hyExec"]=hy.hyTheta.apply(lambda q:True if q>0 else False)

    hy.columns=list(map(
        lambda q:{y:x for x,y in hyColumnView.items()}[q],
        hy.columns
    ))

    return hy

## Assigning Group
from sklearn import cluster
from sklearn import preprocessing

def getTrainingGroup(d)->pd.DataFrame:
    indices=["trainingPeriod","trainingCompany","trainingClass"]
    
    groups=d.groupby(indices).ngroup().factorize()[0]+1
    
    Encoder=preprocessing.OneHotEncoder()
    x=Encoder.fit_transform(d[indices])
    
    Grouper=cluster.AgglomerativeClustering(n_clusters=groups.max())
    Grouper.fit(x.toarray())

    return pd.DataFrame({
        "trainingGroup":(Grouper.labels_)+1,
        "trainingGroupAlt":groups
    })

