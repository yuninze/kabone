import os
import numpy as np
import pandas as pd

from coding import *

BL="c:/code/mkc.ac.kr/data.csv"
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
    "encoded"
]

data_=pd.read_csv(BL)

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

