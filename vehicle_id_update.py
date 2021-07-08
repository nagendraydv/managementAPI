#from _future_ import absolute_import
from __future__ import absolute_import
#from _future_ import print_function
from mintloan_utils import DB, utils, datetime, timedelta
import requests
import json
import time
from pypika import Query, Table, JoinType, functions
import datetime
import os
import json
import logging
import time
from pypika import Order


db = DB()




mw_client_profile = Table("mw_client_profile", schema="mint_loan")

customerIds = Query.from_(mw_client_profile).select(mw_client_profile.CUSTOMER_ID, mw_client_profile.COMPANY_NAME,mw_client_profile.CUSTOMER_DATA,mw_client_profile.VEHICLE_NO).where((mw_client_profile.VEHICLE_NO.isnull()) & (mw_client_profile.COMPANY_NAME=='UDAAN') & (mw_client_profile.CUSTOMER_DATA.like('%vehicalId%')))

dataResults = db.runQuery(customerIds)['data']

   

for result in dataResults:
    customerData=json.loads(result['CUSTOMER_DATA'])
    vehicalId=customerData['vehicalId'].strip()
    q = Query.update(mw_client_profile).set('VEHICLE_NO',vehicalId).where(mw_client_profile.CUSTOMER_ID == str(result['CUSTOMER_ID']))
    print(q)
    print((db.dictcursor.execute(db.pikastr(q))))
    db.mydb.commit()