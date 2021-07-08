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
                    cl = Table("mw_finflux_client_master", schema="mint_loan")
                    lmt = Table("mw_client_loan_limit", schema="mint_loan")
                    prod = Table("mw_finflux_loan_product_master",
                                 schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    ll = db.runQuery(Query.from_(lmt).select(
                        "LOAN_LIMIT", "MOBILE_LOAN_LIMIT").where(lmt.CUSTOMER_ID == custID))["data"]
                    products = []
                    if (ll[0]["LOAN_LIMIT"] > 0 if ll else False):
                        # q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER",
                        #                             "COMPANIES_MAPPED").where((prod.MAX_PRINCIPLE>=ll[0]["LOAN_LIMIT"]) &
                        #                                                       (prod.MIN_PRINCIPLE<=ll[0]["LOAN_LIMIT"]) &
                        #                                                       (prod.LIMIT_TYPE=="LOAN_LIMIT"))
                        #products += db.runQuery(q)["data"]
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER",
                                                     "COMPANIES_MAPPED").where((prod.MIN_PRINCIPLE <= ll[0]["LOAN_LIMIT"]) &
                                                                               (prod.LIMIT_TYPE == "LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                        #products = list(set(products))
                    if (ll[0]["MOBILE_LOAN_LIMIT"] > 0 if ll else False):
                        q = Query.from_(prod).select("AUTO_ID", "PRODUCT_ID", "LENDER",
                                                     "COMPANIES_MAPPED").where((prod.MAX_PRINCIPLE >= ll[0]["MOBILE_LOAN_LIMIT"]) &
                                                                               (prod.MIN_PRINCIPLE <= ll[0]["MOBILE_LOAN_LIMIT"]) &
                                                                               (prod.LIMIT_TYPE == "MOBILE_LOAN_LIMIT"))
                        products += db.runQuery(q)["data"]
                    # print products
                    listed_companies = sum([json.loads(ele["COMPANIES_MAPPED"] if ele["COMPANIES_MAPPED"] is not None else "[]")
                                            for ele in db.runQuery(Query.from_(prod).select("COMPANIES_MAPPED"))["data"]], [])
                    finalProducts = []
                    q = db.runQuery(Query.from_(prof).select(
                        prof.COMPANY_NAME).where(prof.CUSTOMER_ID == custID))
                    comp = q["data"][0]["COMPANY_NAME"] if q["data"] else ""
                    qq = db.runQuery(Query.from_(cl).select(cl.CLIENT_ID).where(
                        (cl.CUSTOMER_ID == custID) & (cl.LENDER == 'GETCLARITY')))["data"]
                    for ele in products:
                        if (ele["LENDER"] == "GETCLARITY"):
                            if (comp in (json.loads(ele["COMPANIES_MAPPED"])) if ele["COMPANIES_MAPPED"] is not None else True):
                                if (str(ele["PRODUCT_ID"]) != "5"):
                                    finalProducts.append(
                                        {"AUTO_ID": ele["AUTO_ID"], "PRODUCT_ID": ele["PRODUCT_ID"], "LENDER": "GETCLARITY"})
                                elif (ll[0]["LOAN_LIMIT"] == 2500 if ll else False) or (len(qq) > 0):
                                    finalProducts.append(
                                        {"AUTO_ID": ele["AUTO_ID"], "PRODUCT_ID": ele["PRODUCT_ID"], "LENDER": "GETCLARITY"})
                        if (ele["LENDER"] == "CHAITANYA") & (comp not in listed_companies):
                            finalProducts.append(
                                {"AUTO_ID": ele["AUTO_ID"], "PRODUCT_ID": ele["PRODUCT_ID"], "LENDER": "CHAITANYA"})
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
