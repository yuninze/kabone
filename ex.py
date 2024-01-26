import os
import numpy as np
import pandas as pd

exCol=[
    "seq",
    "major",
    "idx",
    "name",
    "chest",
    "chestIx",
    "havAb",
    "hav",
    "hbvAb",
    "hbv",
    "mmrMeasleAb",
    "mmrMumpsAb",
    "mmrRubellaAb",
    "mmr",
    "vzvAb",
    "vzv",
    "tdTdap",
    "covid0",
    "covid1",
    "covid2",
    "covid3"
]

exColTo=[q for q in exCol if q!="name"]

exData=pd.read_csv("c:/code/ex.csv").fillna("q")



indexLength=len(exData.index)

exDataAfter=pd.DataFrame({
    "seq":[q for q in range(1,len(exData.index)+1)],
    "major":"간호학과",
    "idx":exData.idx,
    "name":np.nan,
    "chest":exData.chestDate,
    "chestIx":exData.chest,
    "havAb":exData[["havAbDate","havAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "hav":exData[["havDate","havDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "hbvAb":exData[["hbvAbDate","hbvAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "hbv":exData[["hbvDate","hbvDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "mmrMeasleAb":exData[["mmrMeasleAbDate","mmrMeasleAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "mmrMumpsAb":exData[["mmrMumpsAbDate","mmrMumpsAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "mmrRubellaAb":exData[["mmrRubellaAbDate","mmrRubellaAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "mmr":exData[["mmrDate","mmrDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "vzvAb":exData[["vzvAbDate","vzvAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "vzv":exData[["vzvDate","vzvDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "tdTdap":exData[["tdDate","tdDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "covid0":exData.covid1,
    "covid1":exData.covid2,
    "covid2":exData.covid3.apply(
        lambda q:f"{q[:4]}. {q[5:7]}. {q[8:10]}." if isinstance(q,str) else q
    ),
    "covid3":exData.covid4
}).loc[:,exColTo].copy().replace(
    {
        "(  )":"",
        "\n":"",
        "q (q)":"",
        "q (q차)":"",
        "q":""
    }
).map(
    lambda q:q.replace("q (","(").replace("\n"," ").replace("q. . .","").replace("(q차)","").replace("차차","차").replace("차추가차","차 추가") if isinstance(q,str) else q
)

exDataAfter.set_index("idx").join(dm.set_index("idx").loc[:,"name"],rsuffix="_was").reset_index().loc[:,exCol].to_clipboard(index=0)