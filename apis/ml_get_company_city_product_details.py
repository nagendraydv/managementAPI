from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetCompanyCityProductDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {
            "cityData": [], "loanProducts": [], "investmentProducts": []}}
        errors = utils.errors
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:  # not validate.Request(api='', request=input_dict):
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
                    mapp = Table("mw_company_city_product_mapping",
                                 schema="mint_loan")
                    cm = Table("mw_city_master", schema="mint_loan")
                    lpp = Table("mw_loan_product_packs", schema="mint_loan")
                    dpp = Table("mw_dreams_packs_mapping",
                                schema="gc_reliance")
                    dr = Table("dreams", schema="gc_reliance")
                    schemes = Table("mw_schemes", schema="gc_reliance")
                    ipp = Table("mw_insurance_product_packs",
                                schema="mint_loan")
                    prod = Table("mw_finflux_loan_product_master",
                                 schema="mint_loan")
                    r = Query.from_(mapp).join(cm, how=JoinType.left).on(
                        mapp.CITY_ID == cm.CITY_ID).select(mapp.COMPANY_SHORT_NAME, cm.CITY)
                    r = r.select(mapp.DASHBOARD_ID, mapp.LOAN_PACK_ID,
                                 mapp.INSURANCE_PACK_ID, mapp.INVESTMENT_PACK_ID, mapp.AUTO_ID)
                    if (input_dict["data"]["companyName"] != '' if "companyName" in input_dict["data"] else False):
                        r = r.where(mapp.COMPANY_SHORT_NAME ==
                                    input_dict["data"]["companyName"])
                    if (input_dict["data"]["city"] != '' if "city" in input_dict["data"] else False):
                        r = r.where(cm.CITY == input_dict["data"]["city"])
                    # true when neither of companyName and city is provided or their keys are not present
                    if not ((input_dict["data"]["companyName"] != '' if "companyName" in input_dict["data"] else False) or (input_dict["data"]["city"] != '' if "city" in input_dict["data"] else False)):
                        r = r.where(mapp.COMPANY_SHORT_NAME != 'getclarity')
                    r = db.runQuery(r)["data"]
                    outDict = {ele["COMPANY_SHORT_NAME"]: [] for ele in r}
                    for comp in outDict:
                        outDict[comp] = {ele["CITY"]: {"LOAN_PACK_ID": ele["LOAN_PACK_ID"], "INVESTMENT_PACK_ID": ele["INVESTMENT_PACK_ID"],
                                                       "INSURANCE_PACK_ID": ele["INSURANCE_PACK_ID"]} for ele in [x for x in r if x["COMPANY_SHORT_NAME"] == comp]}
                    lproducts = {ele["LOAN_PACK_ID"]: [] for ele in r}
                    q = Query.from_(lpp).join(prod, how=JoinType.left).on(
                        lpp.LOAN_PRODUCT_ID == prod.PRODUCT_ID)
                    q = q.select(prod.PRODUCT_ID, prod.LENDER, prod.LIMIT_TYPE, prod.PRINCIPLE, prod.NUMBER_OF_REPAYMENTS,
                                 prod.INTEREST_RATE_PER_PERIOD, lpp.PRODUCT_PACK_ID)
                    loanPacks = db.runQuery(
                        q.where(lpp.PRODUCT_PACK_ID.isin(list(lproducts.keys()))))["data"]
                    for ele in lproducts:
                        lproducts[ele] = [
                            x for x in loanPacks if x["PRODUCT_PACK_ID"] == ele]
                    invproducts = {ele["INVESTMENT_PACK_ID"]: [] for ele in r}
                    q = Query.from_(dpp).join(
                        dr, how=JoinType.left).on(dpp.DREAM_ID == dr.ID)
                    q = q.select(dpp.PRODUCT_PACK_ID, dpp.DREAM_ID, dr.NAME, dr.DESCRIPTION,
                                 dr.SCHEME_1_CODE, dr.SCHEME_2_CODE, dr.SCHEME_3_CODE)
                    invPacks = db.runQuery(
                        q.where(dpp.PRODUCT_PACK_ID.isin(list(invproducts.keys()))))["data"]
                    sch = set(sum([[ele["SCHEME_1_CODE"], ele["SCHEME_2_CODE"], ele["SCHEME_3_CODE"]]
                                   for ele in invPacks], []))  # get all schemes
                    sch = db.runQuery(Query.from_(schemes).select(
                        "MW_SCHEME_CODE", "SCHEME_NAME").where(schemes.MW_SCHEME_CODE.isin(sch)))["data"]
                    sch = {ele["MW_SCHEME_CODE"]: ele["SCHEME_NAME"]
                           for ele in sch}
                    for ele in invPacks:
                        ele.update({"SCHEME_1_NAME": sch[ele["SCHEME_1_CODE"]],
                                    "SCHEME_2_NAME": '' if ele["SCHEME_2_CODE"] == 0 else sch[ele["SCHEME_2_CODE"]],
                                    "SCHEME_3_NAME": '' if ele["SCHEME_3_CODE"] == 0 else sch[ele["SCHEME_3_CODE"]]})
                    for ele in invproducts:
                        invproducts[ele] = [
                            x for x in invPacks if x["PRODUCT_PACK_ID"] == ele]
                    if True:  # token["updated"]:
                        output_dict["data"]["cityData"] = utils.camelCase(
                            outDict)
                        output_dict["data"]["loanProducts"] = utils.camelCase(
                            lproducts)
                        output_dict["data"]["investmentProducts"] = utils.camelCase(
                            invproducts)
                        output_dict["data"].update(
                            {"error": 0, "message": success})
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
