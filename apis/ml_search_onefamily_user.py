from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType, Order


class searchOnefamilyResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"count": "", "customer_details":[]}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise
        try:
            if not validate.Request(api='onefamilyDashboard', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],checkLogin=True)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    lnlmt = Table("mw_client_loan_limit", schema="mint_loan")
                    one_cred = Table("mw_customer_login_credentials", schema="one_family")
                    join = Query.from_(one_cred).join(profile, how=JoinType.left)
                    join = join.on(one_cred.SUPERMONEY_CUSTOMER_ID==profile.CUSTOMER_ID).join(kyc, how=JoinType.left).on(one_cred.SUPERMONEY_CUSTOMER_ID==kyc.CUSTOMER_ID)
                    #join = join.on(one_cred.SUPERMONEY_CUSTOMER_ID==custcred.CUSTOMER_ID)
                    #join = join.on(custcred.CUSTOMER_ID==lnlmt.CUSTOMER_ID)
                    #join = join.on_field("CUSTOMER_ID").join(one_cred, how=JoinType.left)
                    q = join.select(one_cred.LOGIN_ID, one_cred.ACCOUNT_STATUS, one_cred.LAST_LOGIN, one_cred.COMMENTS,
                                    one_cred.REGISTERED_IP_ADDRESS, one_cred.LAST_LOGGED_IN_IP_ADDRESS, one_cred.DEVICE_ID, one_cred.CHEQUES,
                                    one_cred.CREATED_DATE, one_cred.STAGE, profile.COMPANY_NAME,kyc.NAME,
                                    profile.name.as_("PROFILE_NAME"),
                                    profile.NAME_VERIFIED, profile.NAME_COMMENT, profile.NUMBER_VERIFIED, profile.NUMBER_COMMENT)
                    q = q.where((one_cred.STAGE==input_dict["data"]["stage"]) & (profile.IS_PRIMARY==1))
                    print(db.pikastr(q))
                    data = db.runQuery(q)["data"]
                    if data!=[]:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update({"customer_details": data})
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"customer_details": []})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update({"error": 0, "message": "Results not found"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise
