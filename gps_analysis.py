import re, time
from mintloan_utils import DB, utils, datetime, timedelta
import numpy as np
import collections
from pypika import Query, Table, functions
import dateutil.parser
import os
import pandas as pd
import math as m
import datetime
from math import cos, asin, sqrt, pi
def distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 12742 * asin(sqrt(a))
db=DB()
file=os.listdir("/home/nagendra/Downloads/GPS")
file=[ele for ele in file if ele.split("_")[-1]!="proccessed.log"]
for j in range(len(file)):
    df=pd.read_csv("/home/nagendra/Downloads/captureGPS.log",sep='|', index_col=False, 
                 names=['customerID', 'NetworkLongitude', 'NetworkLatitude', 'DeviceLongitude','DeviceLatitude','Category','time'])
    #with open("/home/nagendra/Documents/sms/"+str(file[j])) as f:
    df.dropna(inplace=True)
    df = df.reset_index(drop=True)
    df.sort_values(by=["customerID",'time'],inplace=True)
    custID=[ele for ele in df["customerID"]]
    #print(len(custID))
    custSet=set(custID)
    custID=list(custSet)
    #print(len(custID))
    data=[]
    for j in range(len(custID)):
    #pass
    #print(custID[j])
    #print()
    #df_new=df_Grouped.get_group(custID[j])
    #print(type(df_new))
    #df_new=pd.DataFrame(df_new).set_index('')
    #print(type(df_new))
        filter1 = df["customerID"]==custID[j]
        df_new=df.where(filter1)
    #print(type(df_new))
        df_new=df_new.dropna()
    #print(df_new["DeviceLatitude"])
    #print(df.head())
    #print(df_new)
        df_new=df_new.reset_index(drop=True)
    #print(len(df_new))
    #print(type(df_new))
    #print(df_new)
        for i in range(len(df_new)-1):
        #pass
        #print(i,df_new["DeviceLatitude"])
            distance_tarvelled=distance(df_new["DeviceLatitude"][i],df_new["DeviceLongitude"][i],df_new["DeviceLatitude"][i+1],df_new["DeviceLongitude"][i+1])
        #print(str(distance_tarvelled*1000)+'m')
            customer_id=int(df_new["customerID"][i])
        #print("customerID:"+str(int(customer_id)))
            date1=df_new["time"][i+1]
            date2=df_new["time"][i]
            date1=datetime.datetime.strptime(date1,"%Y-%m-%dT%H:%M:%S")
            date2=datetime.datetime.strptime(date2,"%Y-%m-%dT%H:%M:%S")
            timeTaken=(date1-date2)
        #print(timeTaken)
            data.append([customer_id,distance_tarvelled,timeTaken])
print(data)

#df_Grouped=df.groupby("customerID")
#df_Grouped

    #os.rename("/home/nagendra/Documents/sms/"+str(file[j]),"/home/nagendra/Documents/sms/"+str(file[j]).split(".")[0]+str("_proccessed.log"))