import os
import numpy as np
import pandas as pd

os.chdir("d:/")

vaccinationColumns=[
    "seq",
    "idx",
    "name",
    "chestDate",
    "chest",
    "havAb",
    "havDate",
    "hbvAb",
    "hbvAg",
    "hbvDate",
    "tdDate",
    "mmrDate",
    "mmrMeasle",
    "mmrMumps",
    "mmrRubella",
    "vzvAb",
    "vzvDate",
    "covid1",
    "covid2",
    "covid3",
    "fluDate",
    "comment"
]
vaccination=pd.read_excel("23년도 3학년 동계 면역현황.xlsx",skiprows=4)
vaccination.columns=vaccinationColumns

def getDigits(string):
    if isinstance(string,str):
        string=[q for q in string if q.isdigit()]
        if len(string)>1:
            status=0
        else:
            status=1
    else:
        status=1
    return [status,string]

def toEightDigitsDate(numbers):
    if numbers[0]==0:
        if len(numbers[1])>=8:
            string=f"{''.join(numbers[1][:4])}. {''.join(numbers[1][4:6])}. {''.join(numbers[1][6:8])}."
        else:
            print(f"Unusual value ({numbers[1]})")
            string='#'+''.join(numbers[1])
        return string
    else:
        return numbers[1]

def getLastStringChunk(text):
    if pd.isna(text):
        return text
    else:
        texts=text.splitlines()
        if len(texts)==1:
            return texts[0].strip()
        else:
            return texts[-1].strip()

def abag(text):
    if pd.isna(text):
        return [
            np.nan,
            np.nan
        ]
    else:
        strings=text.splitlines()
        if len(strings)==1:
            return [
                strings[0],
                np.nan
            ]
        else:
            if len(strings[0])>=8:
                date=toEightDigitsDate(getDigits(strings[0].strip()))
                return [
                    date,
                    strings[1]
                ]
            else:
                date=toEightDigitsDate(getDigits(strings[1].strip()))
                return [
                    strings[0],
                    date
                ]

chestDate=vaccination.chestDate.apply(getDigits).apply(toEightDigitsDate)

havAb=pd.DataFrame(vaccination.havAb.apply(abag).tolist(),columns=["havAb","havAbDate"])
havDatePrev=vaccination.havDate.apply(getLastStringChunk)
havDatePrev=havDatePrev.apply(lambda numbers:numbers[:15] if pd.notna(numbers) and len(numbers)>11 else numbers).apply(getDigits)
havDate=havDatePrev.apply(toEightDigitsDate)
havDose=havDatePrev.apply(lambda numbers:numbers[1][-1] if numbers[0]==0 else np.nan).rename("havDose")

hbvAb=pd.DataFrame(vaccination.hbvAb.apply(abag).tolist(),columns=["hbvAb","hbvAbDate"])
hbvAg=pd.DataFrame(vaccination.hbvAg.apply(abag).tolist(),columns=["hbvAg","hbvAgDate"])

hbvDatePrev=vaccination.hbvDate.apply(getLastStringChunk)
hbvDatePrev=hbvDatePrev.apply(lambda numbers:numbers[:15] if pd.notna(numbers) and len(numbers)>11 else numbers).apply(getDigits)
hbvDate=hbvDatePrev.apply(toEightDigitsDate)
hbvDose=hbvDatePrev.apply(lambda numbers:numbers[1][-1] if numbers[0]==0 else np.nan).rename("hbvDose")

td=pd.DataFrame(vaccination.tdDate.apply(abag).tolist(),columns=["tdDate","tdDose"])
td.loc[:,"tdDate"]=td.tdDate.apply(getDigits).apply(toEightDigitsDate)

mmrDatePrev=vaccination.mmrDate.apply(getLastStringChunk)
mmrDatePrev=mmrDatePrev.apply(lambda numbers:numbers[:15] if pd.notna(numbers) and len(numbers)>11 else numbers).apply(getDigits)
mmrDate=mmrDatePrev.apply(toEightDigitsDate)
mmrDose=mmrDatePrev.apply(lambda numbers:numbers[1][-1] if numbers[0]==0 else np.nan).rename("mmrDose")

mmrMeasle=pd.DataFrame(vaccination.mmrMeasle.apply(abag).tolist(),columns=["mmrMeasleAb","mmrMeasleAbDate"])
mmrMumps=pd.DataFrame(vaccination.mmrMumps.apply(abag).tolist(),columns=["mmrMumpsAb","mmrMumpsAbDate"])
mmrRubella=pd.DataFrame(vaccination.mmrRubella.apply(abag).tolist(),columns=["mmrRubellaAb","mmrRubellaAbDate"])

vzvAb=pd.DataFrame(vaccination.vzvAb.apply(abag).tolist(),columns=["vzvAb","vzvAbDate"])

covid1=vaccination.covid1.apply(getDigits).apply(toEightDigitsDate)
covid2=vaccination.covid2.apply(getDigits).apply(toEightDigitsDate)
covid3=vaccination.covid3.copy()
covid4=vaccination.covid3.str.contains("나확진")
covid4.loc[pd.isna(covid4)]=False
covid4=covid4.replace({True:"COVID 확진 기왕력 있음",False:np.nan}).rename("covid4")

fluDate=vaccination.fluDate.apply(getDigits).apply(toEightDigitsDate)

result=pd.concat(
    [
        vaccination.seq,
        vaccination.idx,
        vaccination.name.str.strip(),
        chestDate,
        vaccination.chest,
        havAb,
        havDate,
        havDose,
        hbvAb,
        hbvDate,
        hbvDose,
        hbvAg,
        td,
        mmrDate,
        mmrDose,
        mmrMeasle,
        mmrMumps,
        mmrRubella,
        vzvAb,
        covid1,
        covid2,
        covid3,
        covid4,
        fluDate,
        vaccination.comment
    ],
    axis=1
).map(lambda q:"#" if isinstance(q,(list,tuple,bool)) else q)

result.to_csv("vaccination.result.csv",encoding="utf-8-sig",index=0)

def qxAll(indices,data):
    for name in indices.columns:
        data[data.idx.isin(indices[name])]
        result.to_csv(f"{name}.csv",index=0,encoding="utf-8-sig")
        if any(~indices[name]):
            print(f"Missing indices at {name}")
            print(indices[name][~indices[name].isin(result.idx)].to_string(index=False))

def hx():
    cache=vaccinationQuery(indices.iloc[:,0].tolist(),vaccinationData)
    cache=pd.concat(cache)

    # 1. chestDate less than 180 days
    cache.chestDate=pd.to_datetime(cache.chestDate,format="mixed")
    testChestDateOld=(cache.chestDate-pd.Timestamp("2023-12-26")).dt.days<-180
    cache["problemChestDateOld"]=testChestDateOld

    # 2. HBVAb
    problemNegativeHbvAb=cache.hbvAb.str.contains("양성")
    problemNegativeHbvAb[pd.isna(problemNegativeHbvAb)]=True
    cache["problemNegativeHbvAb"]=problemNegativeHbvAb

    # 3. fluDate less than 180 days
    cache.fluDate=pd.to_datetime(cache.fluDate,format="mixed")
    problemNegativeHbvAb[pd.isna(cache.fluDate)]=pd.Timestamp("1990-01-01")
    problemFluDateOld=(cache.fluDate-pd.Timestamp("2023-12-26")).dt.days<-180
    cache["problemFluDateOld"]=problemFluDateOld

    # cache.to_csv
    cache.to_csv("1cha.csv",index=False,encoding="utf-8-sig")
    
    #
    print("Was",indices.columns[0])
    
    return 0


