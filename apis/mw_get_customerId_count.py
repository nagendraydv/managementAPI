#!/usr/bin/env python2
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


class customerIdCount:

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
            if not validate.Request(api="customerIdCount", request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    fund = input_dict["data"]["fund"]
                    custmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    q1 = Query.from_(custmaster).select(functions.Count(custmaster.CUSTOMER_ID).as_(
                        "ACTIVE")).where((custmaster.Fund == fund) & (custmaster.STATUS == "ACTIVE"))
                    q2 = Query.from_(custmaster).select(functions.Count(custmaster.CUSTOMER_ID).as_(
                        "PENDING")).where((custmaster.Fund == fund) & (custmaster.STATUS == "PENDING"))
                    count1 = db.runQuery(q1)
                    count2 = db.runQuery(q2)
            if count1:
                token = generate(db).AuthToken()
                output_dict["msgHeader"].update({"authToken": token})
                output_dict["data"].update(
                    {"Active_CoustomerId": count1["data"][0]["ACTIVE"], "Pending_CustomerId": count2["data"][0]["PENDING"]})
                output_dict["msgHeader"].update(
                    {"error": 0, "message": "data fetched successfully"})
                resp.body = json.dumps(output_dict)
            db._DbClose_()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTPError, "error")
