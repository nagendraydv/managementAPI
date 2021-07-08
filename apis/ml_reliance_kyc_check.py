from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order


class RelianceKycCheckResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"ifscDetails": []}}
        errors = utils.errors
        success = "data refreshed"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            # not validate.Request(api='insertIfsc', request=input_dict):
            if False:
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
                    custID = input_dict["data"]["customerID"]
                    pan = Table("mw_pan_status", schema="mint_loan")
                    url = "http://13.126.29.47:8080/MintReliance/MintReliance/kycCheck"
                    custPan = Query.from_(pan).select("PAN_NO").where(pan.CUSTOMER_ID == custID).orderby(pan.CREATED_DATE, order=Order.desc)
                    custPan = db.runQuery(custPan.limit(1))
                    custPan = custPan["data"][0]["PAN_NO"] if custPan["data"] else ""
                    r = requests.post(url, data=json.dumps({"pan": custPan}), headers={
                                      "Content-type": "application/json"})
                    if True:  # token["updated"]:
                        output_dict["data"].update({"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
