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

def trDataBongsoon(data:pd.DataFrame,exclude="3-2.5-2023")->pd.DataFrame:
    now=data[data.trainingSemester.ne(exclude)].copy()
    fut=data[data.trainingSemester.eq(exclude)].copy()

    def _objectify(d:pd.DataFrame,targetCols=colsIncomplete):
        d.loc[:,targetCols]=d.loc[:,targetCols].fillna("#")
        return d

    def _mapper(d:pd.DataFrame):
        return pd.DataFrame({
            "학기상세":d.trainingSemester.apply(
                lambda q:viewTrainingSemester[q][2]
            ),
            "학기":d.trainingSemester.apply(
                lambda q:viewTrainingSemester[q][3]
            ),
            "교과목명":d.trainingClass,
            "학년":d.trainingClassYear,
            "학점":d.trainingClassCredit,
            "시수":d.trainingClassCreditMoney,
            "학번":d.idx,
            "성명":d.name.apply(
                lambda q:q.strip().replace("*","").replace(" ","")
            ),
            "성별":np.nan,
            "실습기관명":d.trainingCompany,
            "실습병동(진료과)":d.loc[:,["trainingUnit","trainingLeaderDepartment"]].apply(
                lambda q:f"{q.iat[0]}({q.iat[1]})",
                axis=1
            ),
            "교과목 담당교원":d.trainingTeacher.apply(
                lambda q:q.strip().replace(" ","")
            ),
            "교원구분":d.trainingTeacherReal.apply(
                lambda q:viewTrainingTeacherReal[q] if pd.notna(q) else "#"
            ),
            "실습단위":d.trainingGroup.str.strip(),
            "실습일자":d.trainingPeriod.str.strip(),
            "실습숙소사용":d.dormUsed.apply(
                lambda q:"Y" if pd.notna(q) and q else "N"
            )
        })

    final=[_mapper(_objectify(x)) for x in (now,fut)]
    return final

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

## 현지 전처리
# meet.apply(
# 	lambda q:f'- {q.at["trainingClass"]} 진행간 목표, 지도 및 평가 방법 논의(기관측 {", ".join(q.at["part1"].split(","))} 참여)\n- 특수 부서 배치를 통한 실습 목표 달성 방안 논의\n- COVID 예방 등 감염관리 교육, 실습생의 능동적 실습 참여 방법 논의',
# 	axis=1
# )

# okzoo["kaboneDatetime"]=okzoo.apply(
# 	lambda q:f'{q.at["date"][:11].strip()}{os.linesep}{q.at["date"][-5:].strip()}{os.linesep}{q.at["location"]}',
# 	axis=1
# )

# ppl=meet[["part0","part1"]]
# ppl=ppl.apply(lambda q:q.str.split(","))

# def peoples(s):
# 	if len(s)==1:
# 		return s[0]
# 	else:
# 		return f"{s[0]} 등 {len(s)}명"

# ppl.map(peoples)


## 숙소 전처리
# sooksou.worksheet
# dormUseCols=[
#     "idx",
#     "trainingSemester",
#     "dormUsed",
#     "dorm"
# ]
# y.loc[:,"hak"]=y.hak.apply(
#     lambda q:[w.strip() for w in q.split(",")] if len(q)>4 else ""
# )
# y=y.explode("hak")
# y.loc[:,"hak"]=y.hak.replace("","#").apply(lambda q:q[0])