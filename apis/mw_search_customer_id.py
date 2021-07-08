from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType


class SearchCustomerIdResource:

    def on_get(self, req, resp):
        """Handles GET requests"""
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"custStages": {}}}
        errors = utils.errors
        success = ""
        #logInfo = {'api': 'customerDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            #utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='searchCustomerId', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # , filename='mysql-slave.config') # setting an instance of DB class
                db = DB(input_dict["msgHeader"]["authLoginID"])
                dbw = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(dbw).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    bank_details = Table(
                        "mw_cust_bank_detail", schema="mint_loan")
                    final_query = Query.from_(bank_details).select('customer_id', 'branch', 'bank_name').where(
                        (bank_details.IFSC_CODE == input_dict["data"]["ifsc_code"]) & (bank_details.ACCOUNT_NO == input_dict["data"]["account_no"]))
                    data = db.runQuery(final_query)
                    #custStages = [ele["STAGE"] for ele in data["data"] if ele["STAGE"]]
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"] = data
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                #utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
