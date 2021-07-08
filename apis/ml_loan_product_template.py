from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class LoanProductTemplateResource:

    def on_get(self, req, resp):
        """Handles GET requests"""
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise
        try:
            if not True:#validate.Request(api='loan_product_template', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    lptemp = Table("mw_loan_product_template", schema="mint_loan")
                    lm = Table("mw_finflux_loan_product_master", schema="mint_loan")
                    indict = input_dict["data"]
                    q = Query.from_(lptemp).join(lm, how=JoinType.left).on(lm.PRODUCT_ID==lptemp.LOAN_PRODUCT_ID)
                    q = q.select(lptemp.MIN_INTEREST_RATE, lptemp.MAX_INTEREST_RATE, lptemp.DEFAULT_INTEREST_RATE,lptemp.MULTIPLEOF_INTEREST_RATE,
                                   lptemp.MIN_AMOUNT, lptemp.MAX_AMOUNT, lptemp.DEFAULT_AMOUNT, lptemp.MULTIPLEOF_AMOUNT, lptemp.MIN_TENURE, lptemp.MAX_TENURE, lptemp.DEFAULT_TENURE,
                                   lptemp.MULTIPLEOF_TENURE, lptemp.LOAN_PRODUCT_ID, lptemp.LENDER, lptemp.CREATED_BY, lptemp.CREATED_DATE,lptemp.EMI_PACK_ID,lptemp.REPAY_EVERY,lptemp.TERM_FREQUENCY,lptemp.AUTO_ID, lptemp.CITY, lptemp.COMPANY,
                                   lptemp.IS_ACTIVE, lm.CATEGORY)
                    if indict['city']==""  or (indict['city'].lower()=='other'):
                        q1 = q.where(lptemp.COMPANY==indict["company"].upper() & (lptemp.IS_ACTIVE==1))
                        #print(db.pikastr(q))
                        data = db.runQuery(q1)
                    else:
                        q1 = q.where((lptemp.COMPANY==indict["company"].upper()) & (lptemp.CITY==indict["city"]) & (lptemp.IS_ACTIVE==1))
                        #print(db.pikastr(q1))
                        data = db.runQuery(q1)
                        if data["data"]==[]:
                            q1 = q.where((lptemp.COMPANY.isnull()) & (lptemp.CITY.isnull()) & (lptemp.IS_ACTIVE==1))
                            #print(db.pikastr(q1))
                            data = db.runQuery(q1)                            
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"loanProductTemplate":data["data"]})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise

