from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class ShowRepayInfoResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"repayInfos": []}}
        errors = utils.errors
        success = "tasks loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='showRepayInfo', request=input_dict):
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
                    page = input_dict["data"]['page']
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    q = Query.from_(repay).join(custcred, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(kyc, how=JoinType.left)
                    q = q.on_field("CUSTOMER_ID").select(
                        repay.star, custcred.LOGIN_ID, kyc.NAME)
                    indict = input_dict["data"]
                    if indict["loanID"] != "":
                        q = q.where(repay.LOAN_REF_ID == indict["loanID"])
                    if indict["modeOfPayment"] != "":
                        q = q.where(repay.MODE_OF_PAYMENT ==
                                    indict["modeOfPayment"])
                    if indict["fromDate"]:
                        q = q.where(repay.CREATED_DATE >= indict["fromDate"])
                    if indict["toDate"]:
                        q = q.where(repay.CREATED_DATE <= indict["toDate"])
                    if indict["days"] > 0:
                        q = q.where(repay.CREATED_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    if indict["repayID"] != "":
                        q = q.where(repay.AUTO_ID == indict["repayID"])
                    if (indict["loginID"]!="" if "loginID" in indict else False):
                        q = q.where(repay.CREATED_BY==indict["loginID"])
                    respdict = db.runQuery(q.orderby(repay.AUTO_ID, order=Order.desc).limit(
                        "%i,%i" % (page["startIndex"], page["size"])))
                    # print db.pikastr(q.orderby(repay.AUTO_ID, order=Order.desc))
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"]["repayInfos"] = utils.camelCase(
                            respdict["data"])
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
