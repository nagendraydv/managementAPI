from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType


class GetCustInvestmentsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader":{"authToken" : ""},"data":{"investments":[], "portfolios":[], "selectedDreams":[], "invDetails":{}, "revFeed":[]}}
        errors = utils.errors
        success = ""
        #logInfo = {'api': 'customerDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            #utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            # not validate.Request(api='custDetails', request=input_dict):
            if False:
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
                    kyc = Table("pan_status_check", schema="gc_reliance")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    tran = Table("reliance_transactions", schema="gc_reliance")
                    dr = Table("invested_dreams", schema="gc_reliance")
                    invDet = Table("mw_investor_details", schema="gc_reliance")
                    rFeed = Table("mw_reverse_feed", schema="gc_reliance")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    sc = Table("mw_schemes", schema="gc_reliance")
                    sel = Table("selected_dreams", schema="gc_reliance")
                    port = Table("reliance_portfolio_scheme",schema="gc_reliance")
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    sdreams = Query.from_(sel).join(sc, how=JoinType.left).on(sc.MW_SCHEME_CODE == sel.SCHEME_CODE).join(prof, how=JoinType.left)
                    sdreams = sdreams.on(sel.CUSTOMER_ID == functions.Concat('SM', prof.CUSTOMER_ID)).select(prof.COMPANY_ID, sel.star, sc.SCHEME_NAME)
                    sdreams = db.runQuery(sdreams.where((sel.ARCHIVED == 'N') & (sel.CUSTOMER_ID == ('SM'+str(custID)))))["data"]
                    #print(sdreams)
                    inv = Query.from_(tran).join(dr, how=JoinType.left).on(tran.FOLIO == dr.FOLIO).join(pan, how=JoinType.left).on(dr.PAN == pan.PAN_NO).select(tran.star)
                    inv = db.runQuery(inv.where(pan.CUSTOMER_ID == custID))["data"]
                    invDetails = db.runQuery(Query.from_(invDet).join(pan, how=JoinType.left).on(invDet.PAN==pan.PAN_NO).select(invDet.star).where((invDet.ARCHIVED==0) & (pan.CUSTOMER_ID==custID)))["data"]
                    revFeed = db.runQuery(Query.from_(rFeed).join(pan, how=JoinType.left).on(rFeed.PAN==pan.PAN_NO).select(rFeed.star).where((rFeed.ARCHIVED==0) & (pan.CUSTOMER_ID==custID)))["data"]
                    #print(inv)
                    folios = list(set([ele["FOLIO"] for ele in inv]))
                    portfolios = db.runQuery(Query.from_(port).select(port.star).where(port.FOLIO.isin(folios)))["data"]
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"]["investments"] = utils.camelCase(inv)
                        output_dict["data"]["portfolios"] = utils.camelCase(portfolios)
                        output_dict["data"]["selectedDreams"] = utils.camelCase(sdreams)
                        output_dict["data"]["invDetails"] = utils.camelCase(invDetails[0]) if invDetails else {}
                        output_dict["data"]["revFeed"] = utils.camelCase(revFeed)
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)  # i have removed , encoding='unicode-escape' there  no need to encode it 
                #utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
