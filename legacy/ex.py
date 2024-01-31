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

ex=pd.read_csv("../ex.csv").fillna("q")

exAfter=pd.DataFrame({
    "seq":[q for q in range(1,len(ex.index)+1)],
    "major":"간호학과",
    "idx":ex.idx,
    "name":np.nan,
    "chest":ex.chestDate,
    "chestIx":ex.chest,
    "havAb":ex[["havAbDate","havAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "hav":ex[["havDate","havDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "hbvAb":ex[["hbvAbDate","hbvAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "hbv":ex[["hbvDate","hbvDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "mmrMeasleAb":ex[["mmrMeasleAbDate","mmrMeasleAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "mmrMumpsAb":ex[["mmrMumpsAbDate","mmrMumpsAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "mmrRubellaAb":ex[["mmrRubellaAbDate","mmrRubellaAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "mmr":ex[["mmrDate","mmrDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "vzvAb":ex[["vzvAbDate","vzvAb"]].apply(
        lambda q:f"{q.iat[0]} ({q.iat[1]})",
    axis=1),
    "vzv":ex[["vzvDate","vzvDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "tdTdap":ex[["tdDate","tdDose"]].apply(
        lambda q:f"{q.iat[0]} ({int(q.iat[1])}차)" if isinstance(q.iat[1],float) else f"{q.iat[0]} ({q.iat[1]}차)",
    axis=1),
    "covid0":ex.covid1,
    "covid1":ex.covid2,
    "covid2":ex.covid3.apply(
        lambda q:f"{q[:4]}. {q[5:7]}. {q[8:10]}." if isinstance(q,str) else q
    ),
    "covid3":ex.covid4
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

dm=pd.read_csv("../dm.csv")

exAfter=exAfter.set_index("idx").join(dm.set_index("idx").loc[:,"name"],rsuffix="_was").reset_index().loc[:,exCol]

exAfter.to_clipboard(index=0)
