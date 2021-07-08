from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class grantDashboardResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {},"msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "data fetched"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='grantDashboard', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    razorpayout = Table("mw_razor_pay_payout_details", schema="mint_loan")
                    grantDis = Table("mw_grant_disbursement" , schema = 'mint_loan')
                    grantDisReq = Table("mw_grant_disbursement_request", schema="mint_loan")
                    #div=input_dict["data"]["division"]
                    #subDiv = input_dict["data"]["subDivision"]
                    q1 = Query.from_(razorpayout).select(razorpayout.STATUS,razorpayout.UTR,razorpayout.REFERENCE_ID,grantDisReq.AMOUNT,grantDisReq.CREATED_DATE,grantDis.CUSTOMER_ID,grantDis.BENEFICIARY_NAME,grantDis.BENEFICIERY_BANK_ACCOUNT_NO,grantDis.BENEFICIERY_IFSC_CODE) 
                    q1 = q1.join(grantDis, how=JoinType.inner).on(razorpayout.REFERENCE_ID==grantDis.disbursal_id).join(grantDisReq, how=JoinType.inner).on(grantDisReq.AUTO_ID==grantDis.REQUEST_ID)
                    if 'division' in input_dict["data"]:
                        q1=q1.where((grantDisReq.PROGRAM==input_dict["data"]["division"]))
                    if 'subDivision' in input_dict["data"]:
                        q1=q1.where(grantDisReq.PROGRAM_DETAILS==input_dict["data"]["subDivision"])
                    if input_dict["data"]["status"]=='processed':
                        q1=q1.where(razorpayout.STATUS=='processed')
                    else:
                        q1=q1.where(razorpayout.STATUS!='processed')
                    data = db.runQuery(q1)
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"loanData": data['data']})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
