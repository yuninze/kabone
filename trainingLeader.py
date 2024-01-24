import os
import numpy as np
import pandas as pd

TrColsOrdered=[
    "trainingSemester",
    "trainingClass",
    "idx",
    "trainingSerie",
    "trainingCompany",
    "trainingPeriod",
    "name",
    "klass",
    "trainingTeacher",
    "trainingTeacherReal",
    "trainingUnit",
    "address",
    "registerProblem",
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
    "trainingLeaderExperience"
]

data=pd.read_csv("c:/code/data.csv")

def getColsOrdered(data,colsOrdered):
    return data.loc[:,colsOrdered].copy()

def getBySemester(data,semester):
    return data.loc[:,semester].copy()

def pasteClinicalLeader(data,semester,q):
    indices=["trainingCompany","trainingUnit"]
    data=getBySemester(data,semester).set_index(indices)
    q=q.set_index(indices)
    data.update(q,overwrite=True)
    return data.loc[:,TrColsOrdered].copy()

