from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetInsuranceProductsResource:

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
                    lmt = Table("mw_client_loan_limit", schema="mint_loan")
                    prod = Table("mw_finflux_loan_product_master",
                                 schema="mint_loan")
                    ll = Query.from_(lmt).select(
                        "INSURANCE_LOAN_LIMIT", "INSURANCE_UPFRONT_LIMIT").where(lmt.CUSTOMER_ID == custID)
                    ll = db.runQuery(ll)["data"]
                    ll = ll[0] if ll != [] else {
                        "INSURANCE_LOAN_LIMIT": 0, "INSURANCE_UPFRONT_LIMIT": 0}
                    #print(ll)
                    products = []
                    if (ll["INSURANCE_LOAN_LIMIT"] > 0) and (ll["INSURANCE_UPFRONT_LIMIT"] > 0):
                        q = Query.from_(prod).select("LIMIT_TYPE", "PRODUCT_ID", "LENDER", "INTEREST_RATE_PER_PERIOD", "TERM_FREQUENCY",
                                                     "AUTO_ID").where((prod.LIMIT_TYPE.isin(["INSURANCE_LOAN_LIMIT", "INSURANCE_UPFRONT_LIMIT"])))
                        products += db.runQuery(q)["data"]
                    for ele in products:
                        if ele["LIMIT_TYPE"] == "INSURANCE_UPFRONT_LIMIT":
                            ele.update(
                                {"INSURANCE_UPFRONT_LIMIT": ll["INSURANCE_UPFRONT_LIMIT"], "EMI": ll["INSURANCE_UPFRONT_LIMIT"]})
                            ele.pop("LIMIT_TYPE")
                        elif ele["LIMIT_TYPE"] == "INSURANCE_LOAN_LIMIT":
                            emi = (float(ll["INSURANCE_LOAN_LIMIT"])/ele["TERM_FREQUENCY"] +
                                   (7.0/364)*(12.0*ele["INTEREST_RATE_PER_PERIOD"]/100)*ll["INSURANCE_LOAN_LIMIT"])
                            fee = 30 if ll["INSURANCE_LOAN_LIMIT"] > 7500 else 20 if ll["INSURANCE_LOAN_LIMIT"] > 5000 else 10
                            ele.update(
                                {"INSURANCE_LOAN_LIMIT": ll["INSURANCE_LOAN_LIMIT"], "EMI": "%i" % (emi + fee)})
                            ele.pop("LIMIT_TYPE")
                        else:
                            products.pop(ele)
                    if True:  # token["updated"]:
                        output_dict["data"]["products"] = products
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
