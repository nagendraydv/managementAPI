from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType


class CustBankDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"custCredentials": {
        }, "custDetails": {}, "custBankDetails": [], "mandateData": []}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'custBankDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='custDetails', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # , filename='mysql-slave.config')
                db = DB(input_dict["msgHeader"]["authLoginID"])
                dbw = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    mandate = Table("mw_physical_mandate_status", schema="mint_loan")
                    custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "LAST_LOGIN", "CUSTOMER_ID", "STAGE",
                                                                               "REGISTERED_IP_ADDRESS", "LAST_LOGGED_IN_IP_ADDRESS", "COMMENTS",
                                                                               "DEVICE_ID", "CREATED_DATE", "REJECTED",
                                                                               "REJECTION_REASON").where(custcred.CUSTOMER_ID == custID))
                    custDetails = db.runQuery(Query.from_(profile).select(profile.star).where(profile.CUSTOMER_ID == custID))
                    custBankDetails = db.runQuery(Query.from_(custbank).select(custbank.star).where(custbank.CUSTOMER_ID == custID))
                    mandateData = db.runQuery(Query.from_(mandate).select(mandate.star).where(mandate.CUSTOMER_ID == custID))
                    if (custCredentials["data"]):
                        token = generate(dbw).AuthToken()
                        if token["updated"]:
                            output_dict["data"]["mandateData"] = utils.camelCase(mandateData["data"])
                            output_dict["data"]["custCredentials"] = utils.camelCase(custCredentials["data"][0])
                            output_dict["data"]["custBankDetails"] = utils.camelCase(custBankDetails["data"])
                            output_dict["data"]["custDetails"] = utils.camelCase(custDetails["data"][0]) if custDetails["data"] else []
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
