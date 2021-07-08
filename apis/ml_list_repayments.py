from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType, Order


class ListRepaymentsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"repaymentInfo": [], "custDetails": {}, "custCredentials": {
        }, "page": {"startIndex": 0, "size": 0, "count": 0}}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='listRepayments', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    page = input_dict["data"]['page']
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    repay = Table(
                        "mw_client_loan_repayment_history_master", schema="mint_loan")
                    join = Query.from_(repay).join(loanmaster, how=JoinType.left).on(
                        loanmaster.LOAN_ACCOUNT_NO == repay.LOAN_ID)
                    join = join.join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    # .join(clientmaster, how=JoinType.left)
                    join = join.join(custcred).on(
                        loanmaster.CUSTOMER_ID == custcred.CUSTOMER_ID)
                    # on(loanmaster.CUSTOMER_ID==clientmaster.CUSTOMER_ID).join(kyc, how=JoinType.left)
                    join = join.join(kyc, how=JoinType.left)
                    join = join.on(loanmaster.CUSTOMER_ID == kyc.CUSTOMER_ID).join(
                        profile, how=JoinType.left)
                    join = join.on(loanmaster.CUSTOMER_ID ==
                                   profile.CUSTOMER_ID)
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, kyc.NAME,  # clientmaster.CLIENT_ID, kyc.NAME,
                                    # clientmaster.FULL_NAME.as_("CLIENT_FULL_NAME"), clientmaster.ACTIVATION_DATE,
                                    profile.name.as_("PROFILE_NAME"),
                                    loanmaster.LOAN_REFERENCE_ID, loanmaster.LOAN_DISBURSED_DATE, loandetails.APPROVED_PRINCIPAL,
                                    loandetails.STATUS, loandetails.EXPECTED_MATURITY_DATE, loandetails.TOTAL_EXPECTED_REPAYMENT,
                                    loandetails.TOTAL_OUTSTANDING, repay.star)
                    q1 = join.select(functions.Count(
                        custcred.CUSTOMER_ID).as_("count"))
                    indict = input_dict['data']
                    # print indict
                    if (indict["custID"] != "" if "custID" in indict else False):
                        q = q.where(loanmaster.CUSTOMER_ID == indict["custID"])
                        q1 = q1.where(loanmaster.CUSTOMER_ID ==
                                      indict["custID"])
                        custDetails = db.runQuery(Query.from_(profile).select(
                            profile.star).where(profile.CUSTOMER_ID == indict["custID"]))
                        custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "LAST_LOGIN", "CUSTOMER_ID", "STAGE",
                                                                                   "REGISTERED_IP_ADDRESS", "LAST_LOGGED_IN_IP_ADDRESS", "COMMENTS",
                                                                                   "DEVICE_ID", "CREATED_DATE", "REJECTED",
                                                                                   "REJECTION_REASON").where(custcred.CUSTOMER_ID == indict["custID"]))
                    else:
                        custDetails, custCredentials = {
                            "data": {}}, {"data": {}}
                    if indict["repayStatus"] != []:
                        q = q.where(repay.TRANSACTION_STATUS.isin(
                            ['SUCCESS', 'S', 'SUCCESSFULL'] if indict["repayStatus"] == ['SUCCESS'] else indict["repayStatus"]))
                        q1 = q1.where(repay.TRANSACTION_STATUS.isin(
                            ['SUCCESS', 'S', 'SUCCESSFULL'] if indict["repayStatus"] == 'SUCCESS' else indict["repayStatus"]))
                    if (indict["repayAmount"] != '' if 'repayAmount' in indict else False):
                        q = q.where(repay.AMOUNT == indict['repayAmount'])
                        q1 = q1.where(repay.AMOUNT == indict['repayAmount'])
                    if indict["days"] > 0:
                        q = q.where(repay.CREATED_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                        q1 = q1.where(repay.CREATED_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    Fields = db.runQuery(q.orderby(repay.ID, order=Order.desc).limit(
                        "%i,%i" % (page["startIndex"], page["size"])))
                    temp = db.runQuery(q1)["data"]
                    Fields.update(temp[0] if temp else {})
                    for ele in Fields["data"]:
                        c = Query.from_(clientmaster).select(
                            "CLIENT_ID", "FULL_NAME", "ACTIVATION_DATE")
                        c = db.runQuery(
                            c.where(clientmaster.CUSTOMER_ID == ele["CUSTOMER_ID"]).limit(1))["data"]
                        ele.update({"CLIENT_ID": c[0]["CLIENT_ID"], "CLIENT_FULL_NAME": c[0]
                                    ["FULL_NAME"], "ACTIVATION_DATE": c[0]["ACTIVATION_DATE"]})
                    if (not Fields["error"]):
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            if Fields["data"] != []:
                                output_dict["data"]["custDetails"] = utils.camelCase(
                                    custDetails["data"][0]) if custDetails["data"] else []
                                output_dict["data"]["custCredentials"] = utils.camelCase(
                                    custCredentials["data"][0]) if custCredentials["data"] else []
                                output_dict["data"].update(
                                    {"repaymentInfo": utils.camelCase(Fields["data"])})
                                output_dict["data"]["page"] = input_dict["data"]["page"]
                                output_dict["data"]["page"].update(
                                    {"count": Fields["count"]})
                                output_dict["data"].update(
                                    {"error": 0, "message": success})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"]["custDetails"] = utils.camelCase(
                                    custDetails["data"][0]) if custDetails["data"] else []
                                output_dict["data"]["custCredentials"] = utils.camelCase(
                                    custCredentials["data"][0])
                                output_dict["msgHeader"]["authToken"] = token["token"]
                                output_dict["data"].update(
                                    {"error": 0, "message": "No data found"})
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
