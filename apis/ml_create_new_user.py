#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 11:17:29 2020

@author: nagendra
"""


from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, Order, JoinType
from uuid import uuid4


class CreateNewUserResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {},"msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "new user created successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
        try:
            if (not validate.Request(api='newuser', request=input_dict)):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],checkLogin=True)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    lm = Table('mw_admin_user_account_type',schema='mint_loan_admin')
                    q1 = Query.from_(lm).select(lm.ACCOUNT_TYPE)
                    loginID = db.runQuery(q1.where(lm.LOGIN==input_dict['msgHeader']['authLoginID']))['data'][0]['ACCOUNT_TYPE']
                    #print(loginID)
                    if loginID=='SUPERUSER':
                        #print('true')
                        uuid = uuid4()
                        #print(uuid)
                        password = generate(db).PasswordHash(input_dict["data"]["password"].encode('utf-8'),update=False)
                        token = generate(db).AuthToken()
                        print(password['hash'])
                        x={'LOGIN':input_dict['data']['loginID'],'UUID':str(uuid),
                           'ACCOUNT_TYPE':input_dict['data']['accountType'],'CREATED_BY':input_dict['msgHeader']['authLoginID'],
                           'CREATED_DATE':datetime.now().strftime("%s")} 
                        inserted = db.Insert(table = 'mw_admin_user_account_type', db = 'mint_loan_admin',debug=False, date =False,compulsory=False,**x)
                        #print(inserted)
                        y={'UUID':str(uuid),'LOGIN':input_dict['data']['loginID'],'PASSWORD':str(password['hash'],'utf-8'),
                           'ACCOUNT_STATUS':input_dict['data']['accountStatus'],'NAME':input_dict['data']['name'],
                           'MOBILE':'0000000000','AUTH_TOKEN':str(token['token']),'CITY':input_dict['data']['city'],
                           'CREATED_BY':input_dict['msgHeader']['authLoginID'],'CREATED_DATE':datetime.now().strftime("%s")}
                        inserted = db.Insert(table="mw_admin_user_master", db="mint_loan_admin",debug =True, date=False,compulsory=False,**y)
                        #print(inserted)
                        if inserted:
                            #print(token)
                            output_dict["msgHeader"].update({"authToken":token['token']})
                            output_dict['data'].update({'error':0,'message':success})
                        else:
                            output_dict['data'].update({'error':1,'message':'something went wrong'})
                    else:
                        output_dict['data'].update({'error':1,'message':'You are not authorized to create user'})
                    resp.body = json.dumps(output_dict)
        except:
            raise