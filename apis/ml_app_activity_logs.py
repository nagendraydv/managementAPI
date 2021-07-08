from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Table, Query, Order


class AppActivityLogsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"activeUsers": {}}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='appActivityLogs', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    mapp = {"Create Customer V3": "createCustomer", "Login": "login", "Get Aadhaar Kyc  Details": "getAadharDetails",
                            "Basic Profile Update": "profileUpdate", "Get Bank Details": "getBankDetails",
                            "Register Client Fin Flux": "finfluxRegister", "Generate Mandate": "genMandate",
                            "Sumbit loan application": "loanSubmit", "Uploading file to S3 AWS.": "fileUpload"}
                    trans = Table("mw_transaction_details", schema="mint_loan")
                    output_dict["data"]["activeUsers"] = {
                        ele: [] for ele in set(mapp.values())}
                    for ele in output_dict["data"]["activeUsers"]:
                        q = Query.from_(trans).select("CUSTOMER_ID", "LOGIN_ID")
                        q = q.where((trans.TRANSACTION_NAME.isin([k for k, v in mapp.items() if v == ele])) &
                                    (trans.CREATED_DATE >= (datetime.now() - timedelta(days=input_dict["data"]["days"]-1)).strftime("%Y-%m-%d")))
                        output_dict["data"]["activeUsers"][ele] = db.runQuery(q.groupby(trans.LOGIN_ID).orderby(trans.CREATED_DATE,
                                                                                                                order=Order.desc))
                    if True:  # (output_dict["data"]["activeUsers"]!=[]):
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
