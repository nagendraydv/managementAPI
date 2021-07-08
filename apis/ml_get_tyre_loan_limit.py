from __future__ import absolute_import
from mintloan_utils import DB, utils, datetime, timedelta, validate, generate
import requests
import json
import falcon
import inspect
from pypika import Query, Table, JoinType, functions, Order


class GetTyreLoanLimitResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"loanLimit": 0,
                                "upfrontLimit": 0}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "Login success"
        logInfo = {'api': 'setTyreLoanLimit'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='tyreLimit', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                gen = generate(db)
                gen.DBlog(logFrom="setTyreLoanLimit", lineNo=inspect.currentframe(
                ).f_lineno, logMessage="Request: " + json.dumps(input_dict))
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"], checkToken=False)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    prof = Table("mw_client_profile", schema="mint_loan")
                    loanlimit = Table("mw_client_loan_limit",
                                      schema="mint_loan")
                    ll = db.runQuery(Query.from_(loanlimit).select(
                        loanlimit.star).where(loanlimit.CUSTOMER_ID == custID))["data"]
                    maxl = max(ll[0]["LOAN_LIMIT"], ll[0]["MOBILE_LOAN_LIMIT"], ll[0]
                               ["INSURANCE_LOAN_LIMIT"], ll[0]["TYRE_LOAN_LIMIT"]) if ll else 0
                    wexp = db.runQuery(Query.from_(prof).select(
                        "WORK_EXPERIENCE", "COMPANY_NAME", "CURRENT_CITY").where(prof.CUSTOMER_ID == custID))
                    wexp, comp, city = ((wexp["data"][0]["WORK_EXPERIENCE"], wexp["data"][0]["COMPANY_NAME"], wexp["data"][0]["CURRENT_CITY"])
                                        if wexp["data"] else (0, "", ""))
                    loanLimit = 0
                    city = city.upper() if type(city) == str else ''
                    if city in ('AHMEDABAD', 'DELHI') and wexp > 6:
                        loanLimit = maxl if maxl > 0 else 5000 if ll else 0
                    elif city in ('AHMEDABAD', 'DELHI') and wexp < 6:
                        loanLimit = maxl if maxl > 0 else 2500 if ll else 0
                    else:
                        loanLimit = 0
                    if ll:
                        junk = db.Update(db="mint_loan", table="mw_client_loan_limit", TYRE_LOAN_LIMIT=str(loanLimit),
                                         conditions={"CUSTOMER_ID = ": str(custID)})
                    else:
                        junk = db.Insert(db="mint_loan", table='mw_client_loan_limit', compulsory=False, date=False,
                                         LOAN_LIMIT="0", MOBILE_LOAN_LIMIT="0", COMMENTS="", ARCHIVED="N", TYRE_LOAN_LIMIT=str(loanLimit),
                                         CREATED_BY="CRON", CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    #token = generate(db).AuthToken()
                    if True:  # token["updated"]:
                        output_dict["data"]["loanLimit"] = loanLimit
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                        output_dict["data"].update({"error": 0, "message": ""})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                    resp.body = json.dumps(output_dict)
                    utils.logger.debug(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    gen.DBlog(logFrom="setInsuranceLoanLimit", lineNo=inspect.currentframe(
                    ).f_lineno, logMessage="Response: " + json.dumps(output_dict))
                    db._DbClose_()
        except:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
