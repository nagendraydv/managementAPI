from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetLenderResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"lenders": {}}}
        errors = utils.errors
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:  # not validate.Request(api='', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # , filename='mysql-slave.config')
                db = DB(input_dict["msgHeader"]["authLoginID"])
                #dbw = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"], checkToken=False)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    loanAmount = input_dict["data"]["loanAmount"]
                    cond = Table("mw_lender_route_conditions",schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    Lenders = {}
                    for amount in input_dict["data"]["loanAmount"]:
                        lenders = {"CHAITANYA": {"priority": 0, "condition": False}, "GETCLARITY": {
                            "priority": 0, "condition": False}}
                        q = db.runQuery(Query.from_(prof).select(
                            prof.COMPANY_NAME).where(prof.CUSTOMER_ID == custID))
                        comp = q["data"][0]["COMPANY_NAME"] if q["data"] else ""
                        if comp in ("UBER AUTO", "Swiggy"):
                            Lenders.update({amount: {"GETCLARITY": {"priority": 1, "condition": True}, "CHAITANYA": {
                                           "priority": 0, "condition": False}}})
                        else:
                            Lenders.update({amount: {"CHAITANYA": {"priority": 1, "condition": True}, "GETCLARITY": {
                                           "priority": 0, "condition": False}}})
                    if True:  # token["updated"]:
                        output_dict["data"]["lenders"] = Lenders
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                # print output_dict
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
