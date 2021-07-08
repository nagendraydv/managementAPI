#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 26 13:52:12 2020

@author: nagendra
"""


from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils
from pypika import Query, Table, functions, Order, JoinType


class PasswordChangeAdminResource:

    def on_get(self, req, resp):
        """Handles GET requests"""
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Password changed successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='changePasswordAdmin', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                loginID = input_dict["data"]["loginID"]
                adminMaster = Table("mw_admin_user_master",schema="mint_loan_admin")
                userType = Table("mw_admin_user_account_type",schema="mint_loan_admin")
                db = DB(input_dict["data"]["loginID"])
                authLoginID = input_dict['msgHeader']['authLoginID']
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                dbLoginID = db.runQuery(Query.from_(adminMaster).select(adminMaster.LOGIN).where(adminMaster.LOGIN==loginID))
                accType = db.runQuery(Query.from_(userType).select(userType.ACCOUNT_TYPE).where(userType.LOGIN==authLoginID))
                #print(accType['data'][0]['ACCOUNT_TYPE'])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                #elif validate(db).Password(input_dict["data"]["oldPassword"]):
                if accType['data'][0]['ACCOUNT_TYPE'] == "SUPERUSER":
                    if dbLoginID["data"][0]["LOGIN"] == loginID:
                        newPassword = generate(db).PasswordHash(input_dict["data"]["newPassword"].encode('utf-8'))
                        if newPassword['password_ok']:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["msgHeader"]["authToken"] = token["token"]
                                output_dict["data"].update({"error": 0, "message": success})
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["strength"]})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["password"]})
                else:
                    output_dict['data'].update({"error":1, "message":"You are not authenticate to do this task"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
