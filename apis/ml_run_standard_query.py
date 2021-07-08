from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class RunStandardQueryResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},"data": {"outputDump": {}}}
        errors = utils.errors
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:  # not validate.Request(api='', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                dbw = DB(input_dict["msgHeader"]["authLoginID"])
                db = DB(input_dict["msgHeader"]["authLoginID"],filename='mysql-slave.config')
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    allowedUsers = ('shiv@mintwalk.com', 'vinod.yadav@supermoney.in', 'vaibhav.patil@mintwalk.com', '9967299619', 'sandesh.kulkarni@mintwalk.com', 'admin@mintloan.com',
                                    'aparajeeta@mintwalk.com', 'nikhil@mintwalk.com', 'ritesh.mishra@mintwalk.com', 'rashmi.nandanvar@mintwalk.com', 'monalisa@supermoney.in')
                    if input_dict["msgHeader"]["authLoginID"] in allowedUsers:
                        stdq = Table("mw_standard_queries", schema="mint_loan")
                        query = db.runQuery(Query.from_(stdq).select("QUERY", "KEY_ORDER").where(stdq.AUTO_ID == input_dict["data"]["queryID"]))["data"]
                        keyOrder = query[0]["KEY_ORDER"].split(", ") if query else []
                        # if input_dict["data"]["queryID"] in (1,2,4,7):
                        #    keyOrder = query[0]["QUERY"].split("FROM")[0][7:-1].replace("a.`","").replace("b.`","").replace("c.`","").replace("`","").split(", ")
                        # else:
                        #    keyOrder = ["DRIVER_UUID"] if input_dict["data"]["queryID"]==6 else ["CUSTOMER_ID", "PHONE_NUMBERS"] if input_dict["data"]["queryID"]==3 else ["CUSTOMER_ID", "LOAN_REF_ID", "REPAY_INFO", "REPAY_AMOUNT", "REPAY_DATETIME", "MODE_OF_PAYMENT", "ACCEPTED_BY", "CREATED_BY"] if input_dict["data"]["queryID"]==5 else []
                        junk = db.dictcursor.execute(query[0]["QUERY"]) if query else 0
                        output_dump = db.dictcursor.fetchall() if junk > 0 else {}
                    else:
                        output_dump = {}
                    if output_dump:
                        token = generate(dbw).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update(
                                {"error": 0, "message": success, "keyOrder": keyOrder, "outputDump": output_dump})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        input_dict["data"].update(
                            {"error": 1, "message": "could not find data"})
                resp.body = json.dumps(output_dict, ensure_ascii=False)
                # print json.dumps(output_dict)
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
