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


class experianUnprocessedData:

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
            if not validate.Request(api="experianUnprocessedData", request=input_dict):
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
                    bulklog = Table("bulk_experian_upload_log", schema="experian_credit_details")
                    q1 = Query.from_(bulklog).select(bulklog.filename).distinct().where(bulklog.created_date > (datetime.now()-timedelta(days=10)).strftime("%Y-%m-%d"))
                    q1 = db.runQuery(q1)["data"]
                    filename = [ele["filename"] for ele in q1]
                    data =[]
                    data1=[]
                    for i in range(len(filename)):
                        q2 = Query.from_(bulklog).select('*').where((bulklog.filename==filename[i]) & (bulklog.created_date > (datetime.now()-timedelta(days=10)).strftime("%Y-%m-%d")))
                        data1 = db.runQuery(q2)
                        data.append({"filename":filename[i],"data":data1["data"]})
                    if data!=[]:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"].update({"authToken": token["token"]})
                        output_dict.update({"data": data})
                        output_dict["msgHeader"].update({"error": 0, "message": "data fetched successfully"})
                        resp.body = json.dumps(output_dict)
                    else:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"].update({"authToken": token["token"]})
                        output_dict.update({"data": data})
                        output_dict["msgHeader"].update({"error": 0, "message": "data fetched successfully"})
                        resp.body = json.dumps(output_dict)
            db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTPError, "error")
