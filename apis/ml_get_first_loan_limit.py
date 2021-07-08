from __future__ import absolute_import
from mintloan_utils import DB, utils, datetime, timedelta, validate, generate
import requests
import json
import falcon
import inspect
from pypika import Query, Table, JoinType, functions, Order


class GetUpfrontLoanLimitResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"generalLimit": 0, "mobileLimit": 0,
                                "upfrontLimitSet": "0"}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "Login success"
        logInfo = {'api': 'setLoanLimit'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='loanLimit', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                gen = generate(db)
                gen.DBlog(logFrom="setLoanLimit", lineNo=inspect.currentframe().f_lineno, logMessage="Request: " + json.dumps(input_dict))
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"], checkToken=False)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"] if "customerID" in input_dict["data"] else input_dict["data"]["custID"] if "custID" in input_dict["data"] else 0
                    prof = Table("mw_client_profile", schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    ld = Table("mw_client_loan_details", schema="mint_loan")
                    loanlimit = Table("mw_client_loan_limit",schema="mint_loan")
                    refral = Table("mw_user_reference_details",schema="mint_loan")
                    cred = Table("mw_customer_login_credentials",schema="mint_loan")
                    refCode = db.runQuery(Query.from_(refral).join(cred, how=JoinType.left).on_field("LOGIN_ID").select("REFER_CODE").where(cred.CUSTOMER_ID == custID))["data"]
                    ll = db.runQuery(Query.from_(loanlimit).select(loanlimit.star).where(loanlimit.CUSTOMER_ID == custID))["data"]
                    print(ll)
                    if not ll:
                        #wexp = db.runQuery(Query.from_(prof).select("WORK_EXPERIENCE", "COMPANY_NAME").where(prof.CUSTOMER_ID==custID))
                        #wexp, comp = (wexp["data"][0]["WORK_EXPERIENCE"], wexp["data"][0]["COMPANY_NAME"]) if wexp["data"] else (0, "")
                        if ("workExp" in input_dict["data"]) & ("companyName" in input_dict["data"]):
                            wexp, comp, tier = (int(input_dict["data"]["workExp"]) if input_dict["data"]["workExp"] not in (None, '') else 0,
                                                input_dict["data"]["companyName"],
                                                input_dict["data"]["uber_plus_tier"] if "uber_plus_tier" in input_dict["data"] else None)
                        else:
                            wexp = db.runQuery(Query.from_(prof).select("WORK_EXPERIENCE", "COMPANY_NAME").where(prof.CUSTOMER_ID == custID))
                            wexp, comp, tier = (wexp["data"][0]["WORK_EXPERIENCE"], wexp["data"][0]["COMPANY_NAME"], None) if wexp["data"] else (0, "", None)
                        city = input_dict["data"]["currentCity"] if "currentCity" in input_dict["data"] else ""
                        genLimit = 0
                        mobLimit = 0
                        tyreLimit = 0
                        eduLimit = 0
                        insLimit = 0
                        if ((refCode[0]["REFER_CODE"].lower() == "acemoto") if refCode else False):
                            insLimit = "10000" if tier in ('diamond', 'platinum') else "10000" if tier else "5000"
                        elif comp in ('swiggy', 'SWIGGY', 'Swiggy'):
                            genLimit = "1000" if ((wexp > 1) and (wexp <= 3)) else "4000" if (wexp > 3) else "0"  # "2500"
                        elif comp in ('udaan', 'UDAAN', 'Udaan'):
                            genLimit = "5000"
                        elif comp in ('shuttle', 'SHUTTLE', 'Shuttle'):
                            genLimit = "40000"
                        elif comp in ('ServiceMandi', 'servicemandi'):
                            genLimit = "2500" if (
                                wexp < 6) else "5000" if wexp >= 6 else "5000"
                        elif comp not in ('UBER_AUTO', 'uber-auto', 'UBER AUTO'):
                            if tier:
                                genLimit = ("10000" if tier == 'diamond' else "10000" if tier == "platinum" else "5000" if tier == "gold" else "2500"
                                            if (wexp < 6) else "5000")
                            else:
                                genLimit = "2500" if (
                                    wexp < 6) else "5000" if wexp >= 6 else "5000"
                            if city in ('Jaipur', 'Chandigarh', 'Delhi', 'Hyderabad'):
                                tyreLimit = "2000" if tier in (
                                    "none", "blue") else "4000" if tier == "gold" else "8000" if tier else "2000"
                            # (city in ('Delhi', 'Mumbai', 'Chandigarh', 'Pune', 'Kolkata', 'Lucknow')) & (tier is not None):
                            if (tier is not None):
                                # "3500" if tier=="gold" else "2500" if tier=="platinum" else "2000" if tier=="diamond" else "0"
                                eduLimit = "3500" if tier in (
                                    "none", "blue") else "2500" if tier == "gold" else "2000" if tier else "0"
                            if city in ('Kolkata', 'Kolkata '):
                                mobLimit = "5900"
                        elif comp in ('UBER_AUTO', 'uber-auto', 'UBER AUTO'):
                            mobLimit = "6500"  # "5500"
                        db.Insert(db="mint_loan", table="mw_client_loan_limit", CUSTOMER_ID=custID, LOAN_LIMIT=genLimit, date=False,
                                  compulsory=False, MOBILE_LOAN_LIMIT=mobLimit, TYRE_LOAN_LIMIT=tyreLimit, ARCHIVED='N', CREATED_BY='ADMIN',
                                  EDUCATION_LOAN_LIMIT=eduLimit, INSURANCE_LOAN_LIMIT=insLimit, CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        genLimit, mobLimit = ll[0]["LOAN_LIMIT"], ll[0]["MOBILE_LOAN_LIMIT"]
                    #q = Query.from_(lm).select(functions.Count(lm.star).as_("c")).where(lm.CUSTOMER_ID==custID)
                    #loans = db.runQuery(q.where(lm.STATUS.notin(['REJECTED', 'ML_REJECTED', 'REQUESTED'])))["data"]
                    #token = generate(db).AuthToken()
                    if True:  # token["updated"]:
                        output_dict["data"]["generalLimit"] = genLimit
                        output_dict["data"]["mobileLimit"] = mobLimit
                        output_dict["data"]["upfrontLimitSet"] = (
                            0 if ll else 1)
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                        output_dict["data"].update({"error": 0, "message": ""})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["query"]})
                    resp.body = json.dumps(output_dict)
                    utils.logger.debug(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    gen.DBlog(logFrom="setLoanLimit", lineNo=inspect.currentframe(
                    ).f_lineno, logMessage="Response: " + json.dumps(output_dict))
                    db._DbClose_()
        except:
            utils.logger.error("ExecutionError: ",extra=logInfo, exc_info=True)
            raise #falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
