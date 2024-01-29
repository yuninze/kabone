import pandas as pd

data=pd.read_csv("c:/code/x.csv")

data=data.loc[data.trainingSemester=="2022-3-2.5",:]

indices=["idx","trainingSemester","trainingClass"]

mpInze=data[["idx","trainingSemester","trainingClass","trainingSerie","trainingCompany","trainingPeriod","trainingTeacher"]]

mpInzeSemester=pd.DataFrame({
    'trainingSemester': {
        '2022-3-2.5': '1학기',
        '3-1': '1학기',
        '3-1.5': '여름학기',
        '3-2': '2학기',
        '3-2.5': '겨울학기',
        '4-1': '1학기',
        '4-1.5': '여름학기',
        '4-2': '2학기',
        '4-2.5': '겨울학기'
    }
})

trainingSemesterTo={
    'trainingSemester': {
        '2022-3-2.5': '3-2.5-2022',
        '3-1': '3-1.0-2023',
        '3-1.5': '3-1.5-2023',
        '3-2': '3-2.0-2023',
        '3-2.5': '3-2.5-2023',
        '4-1': '4-1.0-2023',
        '4-1.5': '4-1.5-2023',
        '4-2': '4-2.0-2023',
        '4-2.5': '4-2.5-2023'
    }
}

mpInze=mpInze.merge(
    mpInzeSemester,
    on="trainingSemester"
    )

mpInze=mpInze.assign(
    mpInzeProgramLabel=mpInze.apply(
        lambda q:f"2023학년도 {q.at['trainingSemester'][5]}학년 {q.at['mpInzeSemester']} {q.at['trainingCompany']} 임상실습({q.at['trainingClass']}, {q.at['trainingPeriod']} 동안 교원 {q.at['trainingTeacher']}에 의해 지도됨)",axis=1),
    mpInzeType=31650002,
    mpInzeYear=2023).loc[:,["idx","mpInzeYear","mpInzeSemester","mpInzeType","mpInzeProgramLabel"]]

mpInze.to_excel("d:/mpInze.xlsx",index=False)
