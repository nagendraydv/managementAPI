from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType


class UpdateRepayInfoResource:

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
        success = "Transaction mapped successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='updateRepayInfo', request=input_dict):
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
                    loanID = input_dict["data"]["loanRefID"]
                    loanmaster = Table("mw_client_loan_master", schema="mint_loan")
                    tran = Table("mw_client_loan_transactions",schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    join = Query.from_(tran).join(loanmaster, how=JoinType.left).on_field(
                        "LOAN_ACCOUNT_NO").join(repay, how=JoinType.left)
                    join = join.on(tran.TRANSACTION_ID == repay.FINFLUX_TRAN_ID).select(
                        tran.AMOUNT, tran.TRANSACTION_DATE, tran.PAYMENT_TYPE)
                    q = db.runQuery(join.select(tran.TRANSACTION_ID).where((loanmaster.CUSTOMER_ID == custID) & (loanmaster.LOAN_REFERENCE_ID == loanID)
                                                                           & (repay.FINFLUX_TRAN_ID.isnull()) & (tran.TYPE == "repayment")))["data"]
                    r = db.runQuery(Query.from_(repay).select(repay.star).where(
                        repay.AUTO_ID == input_dict["data"]["repayID"]))
                    updated = False
                    for ele in q:
                        if (r["data"][0]["REPAY_AMOUNT"] == ele["AMOUNT"] if r["data"] else False):
                            updated = db.Update(db="mint_loan", table="mw_loan_repayment_data", checkAll=False,
                                                conditions={"AUTO_ID=": input_dict["data"]["repayID"]}, FINFLUX_TRAN_ID=str(ele["TRANSACTION_ID"]))
                        else:
                            updated = False
#                    token = generate(db).AuthToken()
                    if True:  # token["updated"]:
                        output_dict["data"].update({"error": 0, "message": (success if updated else "Transaction data not available or already mapped")})
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
