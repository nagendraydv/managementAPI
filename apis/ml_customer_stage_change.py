#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 11:24:38 2020

@author: nagendra
"""
from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType


class customerStageChangeResource:

    def on_get(self, req, resp):
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTPError, 'error', ex.message)

    def on_post(self, req, resp):
        '''Handles post request'''
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTPError, "invalid json", "json was incorrect")
        try:
            if not validate.Request(api="stageUpdate", request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                #val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     #checkLogin=True)
                if False:#val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    stage = input_dict["data"]["stage"]
                    customerID = input_dict["data"]["customerID"]
                    clprofile = Table("mw_client_profile", schema="mint_loan")
                    q1 = Query.from_(clprofile).select(clprofile.customer_id).where((clprofile.company_name=='clearTax')&(clprofile.customer_id==customerID))
                    data = db.runQuery(q1)
                    if data["data"]!=[]:
                        indict1 = {"STAGE":str(stage)}
                        inserted = db.Update(db="mint_loan", table="mw_customer_login_credentials", checkAll=False,debug =True,
                                                         conditions={"customer_id=": customerID},**indict1)
                        if inserted:
                            token = generate(db).AuthToken()
                            output_dict["msgHeader"].update({"authToken": token["token"]})
                            output_dict["msgHeader"].update({"error": 0, "message": "stage changed successfully"})
                            resp.body = json.dumps(output_dict)
                    else:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"].update({"authToken": token["token"]})
                        output_dict["msgHeader"].update({"error": 0, "message": "stage can not be changed"})
                        resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTPError, "error")
