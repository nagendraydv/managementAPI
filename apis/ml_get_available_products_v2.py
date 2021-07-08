from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetAvailableProductsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"products": {}}}
        errors = utils.errors
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
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
                    token=input_dict["msgHeader"]["authToken"], checkToken=False)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    mapp = Table("mw_company_city_product_mapping",
                                 schema="mint_loan")
                    lp = Table("mw_loan_product_packs", schema="mint_loan")
                    cm = Table("mw_city_master", schema="mint_loan")
                    comp = Table("mw_company_master", schema="mint_loan")
                    cl = Table("mw_finflux_client_master", schema="mint_loan")
                    lmt = Table("mw_client_loan_limit", schema="mint_loan")
                    prod = Table("mw_finflux_loan_product_master",
                                 schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    #ll = db.runQuery(Query.from_(lmt).select("LOAN_LIMIT", "MOBILE_LOAN_LIMIT").where(lmt.CUSTOMER_ID==custID))["data"]
                    products = []
                    indict = input_dict["data"]
                    # (ll[0]["LOAN_LIMIT"]>0 if ll else False):
                    if (indict["type"] == "PERSONAL_LOAN") and (indict["creditLine"] > 0):
                        # q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER",
                        #                             "COMPANIES_MAPPED").where((prod.MAX_PRINCIPLE>=ll[0]["LOAN_LIMIT"]) &
                        #                                                       (prod.MIN_PRINCIPLE<=ll[0]["LOAN_LIMIT"]) &
                        #                                                       (prod.LIMIT_TYPE=="LOAN_LIMIT"))
                        #products += db.runQuery(q)["data"]
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER", "LIMIT_TYPE",  # "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                     #"REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE"
                                                     # ll[0]["LOAN_LIMIT"]) &
                                                     "COMPANIES_MAPPED").where((prod.MIN_PRINCIPLE <= indict["creditLine"]) &
                                                                               (prod.LIMIT_TYPE == "LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                        #products = list(set(products))
                    # (ll[0]["MOBILE_LOAN_LIMIT"]>0 if ll else False):
                    if (indict["type"] == "MOBILE_LOAN") and (indict["creditLine"] > 0):
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER", "LIMIT_TYPE",  # "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                     #"REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE", "CHARGE_AMOUNT",
                                                     # (prod.MAX_PRINCIPLE>=input_dict["data"]["mobileLoanCreditLine"]) &#ll[0]["MOBILE_LOAN_LIMIT"]) &
                                                     "COMPANIES_MAPPED").where(
                            # ll[0]["MOBILE_LOAN_LIMIT"]) &
                            (prod.MIN_PRINCIPLE <= indict["creditLine"]) &
                            (prod.LIMIT_TYPE == "MOBILE_LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                    if (indict["type"] == "TYRE_LOAN") and (indict["creditLine"] > 0):
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER", "LIMIT_TYPE",  # "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                     #"REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE", "CHARGE_AMOUNT",
                                                     # (prod.MAX_PRINCIPLE>=input_dict["data"]["mobileLoanCreditLine"]) &#ll[0]["MOBILE_LOAN_LIMIT"]) &
                                                     "COMPANIES_MAPPED").where(
                            # ll[0]["MOBILE_LOAN_LIMIT"]) &
                            (prod.MIN_PRINCIPLE <= indict["creditLine"]) &
                            (prod.LIMIT_TYPE == "TYRE_LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                    if (indict["type"] == "EDUCATION_LOAN") and (indict["creditLine"] > 0):
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER", "LIMIT_TYPE",  # "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                     #"REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE", "CHARGE_AMOUNT",
                                                     # (prod.MAX_PRINCIPLE>=input_dict["data"]["mobileLoanCreditLine"]) &#ll[0]["MOBILE_LOAN_LIMIT"]) &
                                                     "COMPANIES_MAPPED").where(
                            # ll[0]["MOBILE_LOAN_LIMIT"]) &
                            (prod.MIN_PRINCIPLE <= indict["creditLine"]) &
                            (prod.LIMIT_TYPE == "EDUCATION_LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                    # print products
                    if (indict["type"] == "INSURANCE_LOAN") and (indict["creditLine"] > 0):
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER", "LIMIT_TYPE",  # "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                     #"REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE", "CHARGE_AMOUNT",
                                                     # (prod.MAX_PRINCIPLE>=input_dict["data"]["mobileLoanCreditLine"]) &#ll[0]["MOBILE_LOAN_LIMIT"]) &
                                                     "COMPANIES_MAPPED").where(
                            # ll[0]["MOBILE_LOAN_LIMIT"]) &
                            (prod.MIN_PRINCIPLE <= indict["creditLine"]) &
                            (prod.LIMIT_TYPE == "INSURANCE_LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                    listed_companies = sum([json.loads(ele["COMPANIES_MAPPED"] if ele["COMPANIES_MAPPED"] is not None else "[]")
                                            for ele in db.runQuery(Query.from_(prod).select("COMPANIES_MAPPED"))["data"]], [])
                    finalProducts = []
                    q = Query.from_(prof).join(cm, how=JoinType.left).on(
                        cm.CITY == prof.CURRENT_CITY)
                    q = q.join(comp, how=JoinType.left).on(
                        comp.DISPLAY_NAME == prof.COMPANY_NAME)
                    q = db.runQuery(q.select(comp.SHORT_NAME, prof.COMPANY_NAME, cm.CITY_ID).where(
                        prof.CUSTOMER_ID == custID))
                    comp, compID, cityID = (q["data"][0]["COMPANY_NAME"], q["data"][0]
                                            ["SHORT_NAME"], q["data"][0]["CITY_ID"]) if q["data"] else ("", "", "")
                    qQ = Query.from_(mapp).join(lp, how=JoinType.left).on(
                        mapp.LOAN_PACK_ID == lp.PRODUCT_PACK_ID)
                    qQ = qQ.select(lp.LOAN_PRODUCT_ID).where(
                        (lp.ACTIVE == '1') & (mapp.COMPANY_SHORT_NAME == compID))
                    pa = db.runQuery(qQ.where(mapp.CITY_ID == cityID))
                    pa = [ele["LOAN_PRODUCT_ID"] for ele in pa["data"]] if pa["data"] else db.runQuery(
                        qQ.groupby(lp.LOAN_PRODUCT_ID))
                    productsAvailable = pa if type(pa) == list else [
                        ele["LOAN_PRODUCT_ID"] for ele in pa["data"]] if pa["data"] else []
                    qq = db.runQuery(Query.from_(cl).select(cl.CLIENT_ID).where(
                        (cl.CUSTOMER_ID == custID) & (cl.LENDER == 'GETCLARITY')))["data"]
                    for ele in products:
                        if (ele["LENDER"] == "GETCLARITY") & (ele["PRODUCT_ID"] in productsAvailable):
                            # (comp in (json.loads(ele["COMPANIES_MAPPED"])) if ele["COMPANIES_MAPPED"] is not None else True):
                            if True:
                                if True:  # (str(ele["PRODUCT_ID"])!="5"):
                                    finalProducts.append(
                                        {"AUTO_ID": ele["AUTO_ID"], "PRODUCT_ID": ele["PRODUCT_ID"], "LENDER": "GETCLARITY", "LIMIT_TYPE": ele["LIMIT_TYPE"]})
                                elif (ll[0]["LOAN_LIMIT"] == 2500 if ll else False) or (len(qq) > 0):
                                    finalProducts.append(
                                        {"AUTO_ID": ele["AUTO_ID"], "PRODUCT_ID": ele["PRODUCT_ID"], "LENDER": "GETCLARITY", "LIMIT_TYPE": ele["LIMIT_TYPE"]})
                        # (comp not in listed_companies)
                        if (ele["LENDER"] == "CHAITANYA") & (ele["PRODUCT_ID"] in productsAvailable):
                            finalProducts.append(
                                {"AUTO_ID": ele["AUTO_ID"], "PRODUCT_ID": ele["PRODUCT_ID"], "LENDER": "CHAITANYA", "LIMIT_TYPE": ele["LIMIT_TYPE"]})
                    if True:  # token["updated"]:
                        output_dict["data"]["products"] = finalProducts
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                # print output_dict
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
