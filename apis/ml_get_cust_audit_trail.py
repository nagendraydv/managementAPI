from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetCustAuditTrailResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"custDetails": []}}
        errors = utils.errors
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
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
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    logData = Query.from_(log).select(log.star).where(
                        log.CUSTOMER_ID == input_dict["data"]["customerID"])
                    logData = db.runQuery(logData.orderby(
                        log.AUTO_ID, order=Order.desc))["data"]
                    # print logData
                    if True:  # token["updated"]:
                        output_dict["data"]["custDetails"] = utils.camelCase(
                            logData) if logData != [] else []
                        output_dict["data"].update(
                            {"error": 0, "message": success})
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
