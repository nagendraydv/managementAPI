from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
from six.moves import range


class GetLoanLimitResource:

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
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='getLoanLimit', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    mobNumber = input_dict["data"]["mobileNumber"]
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    clientLoanLimit = Table("mw_client_loan_limit", schema="mint_loan")
                    clientProf=Table("mw_client_profile",schema="mint_loan")
                    custCred = db.runQuery(Query.from_(custcred).select("CUSTOMER_ID").where(custcred.LOGIN_ID == '+91'+str(mobNumber)))
                    if custCred["data"]!=[]:
                        token = generate(db).AuthToken()
                        custID=custCred["data"][0]["CUSTOMER_ID"]
                        loanlimit = db.runQuery(Query.from_(clientLoanLimit).select(clientLoanLimit.LOAN_LIMIT).where(clientLoanLimit.CUSTOMER_ID == custID))
                        Division=db.runQuery(Query.from_(clientProf).select(clientProf.DIVISION,clientProf.SUB_DIVISION).where(clientProf.CUSTOMER_ID == custID))
                        print(Division)
                        print(loanlimit)
                        if loanlimit["data"]!=[]:
                            token = generate(db).AuthToken()
                            lnlimit=loanlimit["data"][0]["LOAN_LIMIT"]
                            output_dict["data"].update({"error": 0, "message": "loan limit found"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                            output_dict["data"].update({"customerExist": 1, "loanLimit": lnlimit})
                            if Division["data"]!=[]:
                                token = generate(db).AuthToken()
                                lnlimit=loanlimit["data"][0]["LOAN_LIMIT"]
                                output_dict["data"].update({"error": 0, "message": "loan limit found"})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                                output_dict["data"].update({"customerExist": 1, "loanLimit": lnlimit})
                                output_dict["data"].update({"division":Division["data"][0]["DIVISION"] if Division["data"][0]["DIVISION"]!=None else '', "subDivision":Division["data"][0]["SUB_DIVISION"] if Division["data"][0]["SUB_DIVISION"]!=None else ''})
                            else:
                                token = generate(db).AuthToken()
                                lnlimit=loanlimit["data"][0]["LOAN_LIMIT"]
                                output_dict["data"].update({"error": 0, "message": "loan limit found"})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                                output_dict["data"].update({"customerExist": 1, "loanLimit": lnlimit})
                                output_dict["data"].update({"division":'', "subDivision":''})
                        else:
                            token = generate(db).AuthToken()
                            output_dict["data"].update({"error": 0, "message": "loan limit not found"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                            output_dict["data"].update({"customerExist": 1, "loanLimit":0})
                            output_dict["data"].update({"division":'', "subDivision":''})
                            #resp.body = json.dumps(output_dict)
                    else:
                        token = generate(db).AuthToken()
                        output_dict["data"].update({"error": 0, "message": "customer does not exist"})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update({"customerExist": 0, "loanLimit":0})
                        output_dict["data"].update({"division":'', "subDivision":''})
                    resp.body = json.dumps(output_dict)
                    db._DbClose_()
        except Exception as ex:
            raise
