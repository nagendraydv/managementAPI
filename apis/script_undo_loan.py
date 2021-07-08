#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  9 11:52:48 2021

@author: nagendra
"""
import falcon
import json
import requests
import datetime
import os
import xlrd
import csv
#from mintloan_utils import utils
#@staticmethod
def formatValue(y, md=None):
        if y.ctype == 2:
            return str(int(y.value) if y.value % 1 == 0 else y.value)
        elif (y.ctype == 1 and "/" not in y.value):
            return y.value.replace("'", "")
        elif y.ctype == 3:
            return xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d")
        elif "/" in y.value:
            try:
                x = datetime.strptime(y.value, "%d/%m/%Y").strftime("%Y-%m-%d")
                return x
            except:
                return 0

md = xlrd.open_workbook(filename="/home/nagendra/Desktop/undo_disbursement_file.xlsx", encoding_override='unicode-escape')
sheet = md.sheet_by_index(0)
d = [sheet.row_slice(i) for i in range(sheet.nrows)]
mapping1 = {"LOAN_REFERENCE_ID": "LOAN_REFERENCE_ID"}
outDict={"loanID":"","undodisbursal":False,"undoapproval":False,"reject":False,"error":"","errorMessage":""}
ind, h, n = list(zip(*[(i, mapping1[x.value], x.value.lower() in list(mapping1.keys())) for i, x in enumerate(d[0]) if x.value in mapping1]))
#print(ind, h)
#lender="GETCLARITY"
headers = {'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'getclarity'}
data = []
baseUrl="https://mifos.mintwalk.com/fineract-provider/api/v1/loans/"
for i in range(1, len(d)):
    r = dict(list(zip(h, [formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
    loanID=r["LOAN_REFERENCE_ID"]
    if loanID!="":
        #res = requests.post(baseUrl+str(loanID)+"?command=undodisbursal", headers=headers,)
        if True:#res.status_code==200:
            outDict.update({"undodisbursal":True})
            #res = https://mifos.mintwalk.com/fineract-provider/api/v1/loans/17867?command=undoapproval
            if True:#res.status_code==200:
                outDict.update({"undoapproval":True})
                #res = https://mifos.mintwalk.com/fineract-provider/api/v1/loans/17867?command=reject
                if True:#res.status_code==200:
                    outDict.update({"reject":True})
                else:
                    outDict.update({"error":"","errorMessage":""})
            else:
                outDict.update({"error":"","errorMessage":""})
        else:
            outDict.update({"error":"","errorMessage":""})
    data.append(outDict)
keys = data[0].keys()
with open('people.csv', 'w', newline='')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(data)
#with open("/home/nagendra/Downloads/myfile.txt","w") as f:
    #f.write(str(data))