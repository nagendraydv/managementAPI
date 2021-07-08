from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class StoreCustomerDataResource:

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
        success = "data stored successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='storeCustomerData', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    ut = utils()
                    data = input_dict["data"]
                    respDict = ut.store_customer_data(dataKey=data["dataKey"], dataValue=data["dataValue"], loginId=data["loginID"],
                                                      adminId="ADMIN")
                    #print(respDict)
                    if (respDict["data"]["successFlag"] if ("successFlag" in respDict["data"] if respDict else False) else False):
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update({"error":1, "message":(respDict["header"]["error"]["errorDesc"] if ("header" in respDict if respDict else None) else "")})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
