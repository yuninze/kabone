import pandas as pd

data=pd.read_csv("c:/code/data.csv")

indices=["idx","trainingSemester","trainingClass"]

myungFoomInze=data[["idx","trainingSemester","trainingClass","trainingSerie","trainingCompany","trainingPeriod","trainingTeacher"]]

myungFoomInzeSemesterLabelTable=pd.DataFrame(
{'trainingSemester':['3-1',
  '3-1.5',
  '3-2',
  '3-2.5',
  '4-1',
  '4-1.5',
  '4-2',
  '4-2.5'],
 'mpInzeSemesterLabel':['1학기',
  '여름학기',
  '2학기',
  '겨울학기',
  '1학기',
  '여름학기',
  '2학기',
  '겨울학기']
}
)

myungFoomInze=myungFoomInze.merge(
    myungFoomInzeSemesterLabelTable,
    on="trainingSemester"
    )
myungFoomInze.assign(
    mpInzeProgramLabel=myungFoomInze.apply(
        lambda q:f"2023학년도 {q.at['trainingSemester'][0]}학년 {q.at['mpInzeSemesterLabel']} {q.at['trainingCompany']} 임상실습({q.at['trainingClass']}, {q.at['trainingPeriod']} 동안 교원 {q.at['trainingTeacher']}에 의해 지도됨)",axis=1),
    mpInzeType=31650002,
    mpInzeYear=2023).loc[:,["idx","mpInzeYear","mpInzeSemesterLabel","mpInzeType","mpInzeProgramLabel"]]