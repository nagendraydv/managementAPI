from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetBankIfscDetailsResource:

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
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:  # not validate.Request(api='', request=input_dict):
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
                    bank = Table("mw_bank_ifsc_code", schema="mint_loan")
                    exist = db.runQuery(Query.from_(bank).select("BANK", "BANK_CODE").where(
                        bank.IFSC == input_dict["data"]["ifsc"]))["data"]
                    if exist == []:
                        r = Query.from_(bank).select("BANK", "BANK_CODE").where(
                            bank.IFSC.like(input_dict["data"]["ifsc"][0:4] + "%")).limit(1)
                        r = db.runQuery(r)["data"]
                        error, success = (0, "data loaded successfully") if r != [] else (
                            1, "no bank found with this ifsc code")
                    else:
                        r = []
                        success = "ifsc already exist"
                        error = 1
                    if True:  # token["updated"]:
                        output_dict["data"]["ifscDetails"] = utils.camelCase(
                            r[0]) if r != [] else []
                        output_dict["data"].update(
                            {"error": error, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
