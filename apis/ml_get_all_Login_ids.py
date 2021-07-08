#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 14:06:03 2020

@author: nagendra
"""


from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, Order, JoinType


class AllLOginIDResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"loginsID": [],"message":""},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "all login id found"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if False:#not validate.Request(api='loginID', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],checkLogin=True)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    loginID =list()
                    users = Table("mw_admin_user_master",schema="mint_loan_admin")
                    login = Query.from_(users).select(users.LOGIN).orderby(users.CREATED_DATE, order=Order.desc)
                    logins = db.runQuery(login)["data"]
                    #print(type(loginID))
                    for ele in logins:
                        #print(ele["LOGIN"])
                        loginID.append(ele["LOGIN"])
                    if loginID !=[]:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update({"loginsID":loginID})
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update({"error":1,"message":"no login id found"})
                    resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise
