#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 12:57:41 2021

@author: nagendra
"""


from mintloan_utils import DB, utils, datetime, timedelta
from pypika import Query, Table, functions
#import dateutil.parser
import json
import requests
db=DB()
one_cred = Table("mw_customer_login_credentials",schema="one_family")
kyc = Table("mw_cust_kyc_documents",schema="mint_loan")
conf = Table("mw_configuration", schema="mint_loan")
policy="PLHL1Y1FF210000000045"
token = db.runQuery(Query.from_(conf).select(conf.CONFIG_VALUE).where(conf.CONFIG_KEY=="upliftToken"))["data"]
if token!=[]:
    token=token[0]["CONFIG_VALUE"]
else:
    token=""    
if token!='':
    print(token)
    #baseurl = "https://testapism.uttamsolutions.com/api/Policy?Token=7c3489da-55f4-4fe7-9759-7de16ad34336-202105061320200043&OrganisationName=SuperMoney_Test&Language=en"
    baseurl = "https://testapi.sm.uttamsolutions.com/api/Policy?"
    payload={}
    headers = {}
    url = baseurl + "Policy_NR="+ policy + "&Token=" + token + "&OrganisationName=SuperMoney_Test&Language=en"
    print(url)
    #r = requests.request("GET", url)
    r = requests.request("GET", url, headers=headers, data=payload)
    #print(r.json())
    #import requests

    #url = "https://testapi.sm.uttamsolutions.com/api/Policy?Policy_NR=PLHL1Y1FF210000000045&Token=145dd987-cb05-46b6-bdab-d42d8f6603cf-202107060621525414&OrganisationName=Supermoney_Test&Language=en"
    
    

    print(r.text)
    #utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
    if r.status_code==200:
        if type(r.json()[0])  is not list:
            if ((r.json()[0]["StatusCode"]=="401 - Unauthorized")):
                url1 = "https://dev.mintwalk.com/python/mlUpliftGenerateToken"
                payload2 = {'data': {},'msgHeader': {'authToken': '','authLoginID': 'admin@mintloan.com','timestamp': 1583748704, 'ipAddress': '127.0.1'}}
                #payload="{\n\n    \"data\":{\n    },\n    \"msgHeader\":{\n        \"authToken\":\"\",\n        \"authLoginID\":\"admin@mintloan.com\",\n        \"timestamp\":1583748704,\n        \"ipAddress\":\"127.0.1\"\n    }\n\n}"
                headers = {'Content-Type': 'application/json'}
                response = requests.request("POST", url1, headers=headers, data=json.dumps(payload2))
                if response.status_code==200:
                    token = db.runQuery(Query.from_(conf).select(conf.CONFIG_VALUE).where(conf.CONFIG_KEY=="upliftToken"))["data"]
                    if token!=[]:
                        token=token[0]["CONFIG_VALUE"]
                    else:
                        token=""
                    if token!='':
                        #baseurl = "https://testapism.uttamsolutions.com/api/Policy?Token=7c3489da-55f4-4fe7-9759-7de16ad34336-202105061320200043&OrganisationName=SuperMoney_Test&Language=en"
                        baseurl = "https://testapi.sm.uttamsolutions.com/api/Policy?"
                        url = baseurl + "Policy_NR="+ policy + "Token=" + token + "&OrganisationName=SuperMoney_Test&Language=en"
                        r = requests.request("GET", url)
                        if r.status_code==200:
                            if type(r.json()[0])  is not list:
                                resdata = r.json()[1]
                                updated = db.Update(db="one_family", table="mw_customer_policy_details", conditions={"POLICY_ID = ":str(policy)}, POLICY_STATUS=str(resdata["PolicyStatus"]["Code"]))
                                if updated:
                                    print(policy + ": status updated successfully")
                                else:
                                    print(policy + ": status not updated successfully")
            else:
                resdata = r.json()[1]
                print(resdata["PolicyStatus"]["Code"])
                updated = db.Update(db="one_family", table="mw_customer_policy_details", conditions={"POLICY_ID=":policy}, POLICY_STATUS= str(resdata["PolicyStatus"]["Code"]),debug=True)
                if updated:
                    print(policy + ": status updated successfully")
                else:
                    print(policy + ": status not updated successfully 78")
else:
    print("something went wrong")