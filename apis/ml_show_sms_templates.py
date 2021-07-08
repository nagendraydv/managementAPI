from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType, functions


class ShowSmsTemplateResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {
            "templates": [], "amount": 0, "loginID": "", "docComments": ""}}
        errors = utils.errors
        success = "templates loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='showSmsTemplate', request=input_dict):
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
                    data = input_dict["data"]
                    temp = Table("mw_sms_templates", schema="mint_loan")
                    cred = Table("mw_customer_login_credentials",schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    q = db.runQuery(Query.from_(lm).select(lm.LOAN_ACCOUNT_NO, lm.LOAN_REFERENCE_ID).where((lm.CUSTOMER_ID == data["customerID"]) &
                                                                                                           (lm.STATUS == 'ACTIVE')))
                    if q["data"]:
                        days_7 = (datetime.now()-timedelta(days=7)
                                  ).strftime("%Y-%m-%d")
                        e = Query.from_(emis).select(
                            functions.Sum(emis.OVERDUE_AMOUNT).as_("o"))
                        e = e.where((emis.CUSTOMER_ID == data["customerID"]) & (
                            emis.LOAN_ACCOUNT_NO == q["data"][0]["LOAN_ACCOUNT_NO"]))
                        e = db.runQuery(e.where((emis.DUE_DATE < days_7) & (
                            emis.OVERDUE_AMOUNT > 0)))["data"][0]["o"]
                        if e > 0:
                            d = db.runQuery(Query.from_(repay).select(repay.star).where((repay.LOAN_REF_ID == q["data"][0]["LOAN_REFERENCE_ID"]) &
                                                                                        (repay.FINFLUX_TRAN_ID.isnull())))["data"]
                            dates = [(ele["REPAY_DATETIME"], ele["REPAY_AMOUNT"])
                                     for ele in d]
                            temp_7 = sum(ele[1] for ele in dates if ele[0] > int(
                                (datetime.now() - timedelta(days=7)).strftime("%s")))
                            e = e-temp_7
                        elif e != 0:
                            e = 0
                    else:
                        e = 0
                    q = db.runQuery(Query.from_(cred).select("LOGIN_ID", "DOCUMENT_COMMENTS").where(
                        cred.CUSTOMER_ID == data["customerID"]))
                    docComments = (q["data"][0]["DOCUMENT_COMMENTS"] if q["data"]
                                   [0]["DOCUMENT_COMMENTS"] else '') if q["data"] else ''
                    docComments = (", ".join((ele.split(":")[0] + (("(" + ele.split(":")[1].replace(",", "/") + ")")
                                                                   if ele.split(":")[0] != ele.split(":")[1] else ""))
                                             for ele in docComments.split(";") if len(ele.split(":")) == 2))
                    loginID = q["data"][0]["LOGIN_ID"] if q["data"] else ''
                    q = Query.from_(temp).select(temp.star).where(
                        (temp.LANGUAGE == data["language"]) & (temp.TYPE == data["type"]))
                    x = db.runQuery(q.where(temp.ARCHIVED == 'N'))["data"]
                    if x:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update({"error": 0, "message": success, "templates": utils.camelCase(x), "amount": e,
                                                        "loginID": loginID, "docComments": docComments})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 0, "message": "No template found"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
