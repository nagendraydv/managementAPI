from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetProductLoanDetailsResource:

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
                    Costs = input_dict["data"]["cost"]
                    lmt = Table("mw_client_loan_limit", schema="mint_loan")
                    prod = Table("mw_finflux_loan_product_master",
                                 schema="mint_loan")
                    emi = Table("mw_finflux_emi_packs_master",
                                schema="mint_loan")
                    ll = Query.from_(lmt).select("TYRE_LOAN_LIMIT", "TYRE_UPFRONT_LIMIT").where(
                        lmt.CUSTOMER_ID == custID)
                    ll = db.runQuery(ll)["data"]
                    ll2 = ll[0] if ll != [] else {
                        "TYRE_LOAN_LIMIT": 0, "TYRE_UPFRONT_LIMIT": 0}
                    #print(ll2)
                    loanProducts = {}
                    for prodCost in Costs:
                        # print prodCost
                        ll = {"TYRE_LOAN_LIMIT": max(
                            min(0.8*float(prodCost), ll2["TYRE_LOAN_LIMIT"], 4000), 1000)}
                        # print prodCost
                        # 2000 if ll["TYRE_LOAN_LIMIT"]<2800  else 4000
                        ll["TYRE_LOAN_LIMIT"] = ll["TYRE_LOAN_LIMIT"] - \
                            (ll["TYRE_LOAN_LIMIT"] % 500)
                        ll.update({"TYRE_UPFRONT_LIMIT": (
                            float(prodCost) - ll["TYRE_LOAN_LIMIT"])})
                        # print prodCost
                        products = []
                        if (ll["TYRE_LOAN_LIMIT"] > 0) and (ll["TYRE_UPFRONT_LIMIT"] >= 0):
                            q = Query.from_(prod).select("LIMIT_TYPE", "PRODUCT_ID", "LENDER", "INTEREST_RATE_PER_PERIOD", "TERM_FREQUENCY",
                                                         "AUTO_ID").where((prod.LIMIT_TYPE == "TYRE_LOAN_LIMIT") & (prod.MAX_PRINCIPLE >= ll["TYRE_LOAN_LIMIT"]))
                            products += db.runQuery(q)["data"]
                        # q = Query.from_(prod).select("LIMIT_TYPE", "PRODUCT_ID", "LENDER", "INTEREST_RATE_PER_PERIOD", "TERM_FREQUENCY",
                        #                             "AUTO_ID").where((prod.LIMIT_TYPE=="TYRE_UPFRONT_LIMIT") & (prod.MAX_PRINCIPLE>ll["TYRE_UPFRONT_LIMIT"]))
                        #products += db.runQuery(q)["data"]
                        for ele in products:
                            # if ele["LIMIT_TYPE"]=="TYRE_UPFRONT_LIMIT":
                            #    ele.update({"TYRE_UPFRONT_LIMIT":ll["TYRE_UPFRONT_LIMIT"], "EMI":ll["TYRE_UPFRONT_LIMIT"]})
                            #    ele.pop("LIMIT_TYPE")
                            if ele["LIMIT_TYPE"] == "TYRE_LOAN_LIMIT":
                                # emi = (float(ll["TYRE_LOAN_LIMIT"])/(4) +
                                #       (7.0/364)*(12.0*ele["INTEREST_RATE_PER_PERIOD"]/100)*ll["TYRE_LOAN_LIMIT"])
                                #fee = 30 if ll["TYRE_LOAN_LIMIT"]>7500 else 30 if ll["TYRE_LOAN_LIMIT"]>5000 else 30
                                Emi = db.runQuery(Query.from_(emi).select("EMI").where(
                                    (emi.LOAN_AMOUNT == ll["TYRE_LOAN_LIMIT"]) & (emi.LOAN_PRODUCT_ID == ele["PRODUCT_ID"])))["data"]
                                ele.update({"loanAmount": ll["TYRE_LOAN_LIMIT"], "tenure": 4, "emiAmount": Emi[0]["EMI"] if Emi else 0,  # float("%.2f"%(emi + fee)),
                                            "loanType": "weekly", "upfrontPayment": ll["TYRE_UPFRONT_LIMIT"]})
                                ele.pop("LIMIT_TYPE")
                                ele.pop("AUTO_ID")
                                ele.pop("PRODUCT_ID")
                                ele.pop("LENDER")
                                ele.pop("TERM_FREQUENCY")
                            else:
                                products.pop(ele)
                        # print prodCost
                        loanProducts.update({prodCost: products})
                    if True:  # token["updated"]:
                        output_dict["data"]["products"] = loanProducts
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
