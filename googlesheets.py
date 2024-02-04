import json
import numpy as np
import pandas as pd
import gspread
from datetime import datetime

datetimeFormat="%Y-%m-%dT%H:%M:%S"
pad=". . ."
key="c:/code/gskey.json"

def ima():
    ima=datetime.now().strftime(datetimeFormat)
    return ima,ima.replace(":","")

class GoogleSheets:
    def __init__(self,key):
        self.key=key
        self.gs=gspread.service_account(self.key)
        self.status=False
        print(pad + "Client: " + json.load(open("c:/code/gskey.json"))["client_x509_cert_url"])

    def open(self,sheetId):
        if len(sheetId)<20:
            raise ValueError("This method accepts a sheet id only")
        self.sheet=self.gs.open_by_key(sheetId)
        print(f"{pad}{self.sheet.title} ({self.sheet.id})")
        return 
        
    def select(self,worksheet):
        self.worksheet=self.sheet.worksheet(worksheet)
        print(f"{pad}{self.worksheet.title} ({self.worksheet.id})")
        self.status=True
        return 
    
    def showStatus(self):
        if self.status:
            return self.sheet.list_permissions()
        print(pad + "Did nothing")

    def giveRole(self,mail,perm_type="user",role="reader",notify=True):
        if self.status:
            self.sheet.share(mail,role=role,perm_type=perm_type,notify=notify)
        print(pad + "Did nothing")
    
    def get(self,worksheet):
        self.select(worksheet)
        x=self.worksheet.get()
        return pd.DataFrame(
            x[1:],
            columns=x[0]
        )

    def add(self,title,size=(1000,10)):
        if self.status:
            if not isinstance(title,str):
                raise TypeError(f"title accepts str, but input is {type(title)}")
            title=title + "_" + datetime.now().strftime(datetimeFormat)
            self.sheet.add_worksheet(title,rows=size[0],cols=size[1])
            wks=self.sheet.worksheets().copy()
            wks.reverse()
            self.sheet.reorder_worksheets(wks)
            self.worksheet=self.sheet.worksheet(title)
            print(pad + "added: " + title)
            return title
        print(pad + "Did nothing")
    
    def set(self,title,data):
        if self.status:
            if isinstance(data,pd.Series):
                data=data.to_frame()
            _title=self.add(title,size=data.shape)
            _data=data.fillna(np.nan).replace([np.nan],[None])
            _data=[_data.columns.values.tolist()]+_data.values.tolist()
            _result=self.worksheet.update(_data)
            self.worksheet.format("1:1",{"textFormat":{"bold":True}})
            return (_result,_title)
        print(pad + "Did nothing")

# open(sheetId) -> select select(worksheet) -> gs.worksheet.get()

    