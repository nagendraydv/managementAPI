from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType, Order


class StageSyncResource:

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
        success = "Stage sync success"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='stageSync', request=input_dict):
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
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    clientmaster = Table("mw_finflux_client_master", schema="mint_loan")
                    docs = Table("mw_cust_kyc_documents", schema="mint_loan")
                    loanmaster = Table("mw_client_loan_master", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",schema="mint_loan")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    custDocs = Query.from_(docs).select("DOCUMENT_TYPE_ID").where(docs.CUSTOMER_ID == input_dict["data"]["custID"])
                    custDocs = db.runQuery(custDocs.where((docs.VERIFICATION_STATUS == 'Y') | (docs.VERIFICATION_STATUS.isnull())))["data"]
                    aadhar = db.runQuery(Query.from_(kyc).select("CUSTOMER_ID").where((kyc.CREATED_BY == "Admin") &
                                                                                      (kyc.CUSTOMER_ID == input_dict["data"]["custID"])))
                    docsReq = {100, 106} if aadhar["data"] else {
                        100, 106, 108}  # 101
                    docsReq2 = {100, 102} if aadhar["data"] else {
                        100, 102, 108}  # 101
                    allDocs = set([ele["DOCUMENT_TYPE_ID"]
                                   for ele in custDocs])
                    qi = Query.from_(income).select(functions.Count(income.star).as_("c")).where(income.week > (datetime.now()-timedelta(days=10)).strftime("%Y-%m-%d"))
                    qp = db.runQuery(Query.from_(prof).select(prof.COMPANY_NAME).where(prof.CUSTOMER_ID == input_dict["data"]["custID"]))["data"]
                    qc = db.runQuery(Query.from_(custcred).select(custcred.STAGE).where(custcred.CUSTOMER_ID == input_dict["data"]["custID"]))["data"]
                    if any(x in allDocs for x in [113, 127, 128]) & (qc[0]["STAGE"] in (None, "AWAITING_KYC", "AWAITING_AGREEMENT") if qc else False):
                        if (db.runQuery(qi.where(income.CUSTOMER_ID == input_dict["data"]["custID"]))["data"][0]["c"] == 0) & (qp[0]["COMPANY_NAME"].lower() != 'swiggy' if qp else False):
                            stage = ("AWAITING_UBER_DATA" if (docsReq <= allDocs) or (docsReq2 <= allDocs) else "AWAITING_KYC")
                        else:
                            stage = ("AWAITING_VERIFICATION" if (docsReq <= allDocs) or (docsReq2 <= allDocs) else "AWAITING_KYC")
                        if stage != (qc[0]["STAGE"] if qc else ''):
                            db.Update(db="mint_loan", table="mw_customer_login_credentials",conditions={"CUSTOMER_ID = ": str(input_dict["data"]["custID"])}, checkAll=False, STAGE=stage)
                            db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False,
                                      CUSTOMER_ID=str(input_dict["data"]["custID"]), DATA_KEY="STAGE", DATA_VALUE=stage, CREATED_BY="CRON",
                                      CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
