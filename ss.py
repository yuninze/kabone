import numpy as np
import pandas as pd
from datetime import datetime
import gspread

datetimeFormat="%Y-%m-%d %H:%M:%S"
padder=". . ."

def ima():
    ima=datetime.now().strftime(datetimeFormat)
    return (ima,ima.replace(":",""))

class Gs:
    # gs, sheet, worksheet

    def __init__(self,key):
        self.key=key
        self.gs=gspread.service_account(self.key)
        return None

    def open(self,sheet):
        if len(sheet)<20:
            raise ValueError("This method accepts a sheet id only")
        self.sheet=self.gs.open_by_key(sheet)
        return self.sheet
        
    def select(self,worksheet):
        self.worksheet=self.sheet.worksheet(worksheet)
    
    def get(self,area=None):
        if area is None:
            return self.worksheet.get_all_values()
        else:
            return self.worksheet.get_all_records(area)
    
    def update(self,data):
        data=data.fillna(np.nan).replace([np.nan],[None])
        data=[data.columns.tolist()]+data.values.tolist()
        final=self.worksheet.update(data)
        self.worksheet.format("1:1",{"textFormat":{"bold":True}})
        return final
    
    def status(self):
        return self.sheet.list_permissions()

    def give(self,mail,perm_type="user",role="reader",notify=False):
        self.sheet.share(mail,role=role,perm_type=perm_type,notify=notify)

    def add(self,title=None,size=(1000,10)):
        if title is None:
            title="_"
        _title=f"{datetime.now().strftime(datetimeFormat)}_{title}"
        self.sheet.add_worksheet(_title,rows=size[0],cols=size[1])
        _wks=self.sheet.worksheets()
        _wks.reverse()
        self.sheet.reorder_worksheets(_wks)
        self.worksheet=self.sheet.worksheet(_title)
        return _title
    
    def set(self,title,data):
        if isinstance(data,pd.Series):
            data=data.to_frame()
        _title=self.add(title,size=data.shape)
        _data=data.fillna(np.nan).replace([np.nan],[None])
        _data=[_data.columns.values.tolist()]+_data.values.tolist()
        _result=self.worksheet.update(_data)
        self.worksheet.format("1:1",{"textFormat":{"bold":True}})
        return (_result,_title)

key="c:/code/gskey.json"
gs=Gs(key)

def basis(sheet,worksheet="data",pp=True):
    gs.open(sheet)
    gs.select(worksheet)
    data=gs.worksheet.get()
    data=pd.DataFrame(data[1:],columns=data[0])
    
    return data,ima()[1]

def sookso(sheet,worksheet="sookso",pp=True):
    gs.open(sheet)
    gs.select(worksheet)
    data=gs.worksheet.get("B3:I100")
    data=pd.DataFrame(data).iloc[:,[1,3,6,7]]
    data.name="sookso"
    data.columns=["idx","contact","desiredDorm","paidDorm"]
    
    if pp:
        data.iloc[:,0]=data.idx.astype("object")
        data.iloc[:,1]=data.contact.str.replace("010-","")
    
    return data,ima()[1]

def exec():
    data=basis()[0].set_index("idx")
    this=sookso()[0].set_index("idx")

    if not this.index.isin(data.index).all():
        raise IndexError(f"Unmatched indices {set(this.index)-set(data.index)} in incoming data")

    data.to_csv("d:/dg.data.csv",encoding="utf-8")
    this.to_csv("d:/dg.this.csv",encoding="utf-8")

# basis.combine_first(sookso)

# gs.open("1y72iPCNF-jeQsg6NS-W83IHdBMy08Ega_CjCE5Rv2Dc")
# gs.select("data")
# gs.update(<pd.DataFrame>)