from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType
import six


class SplitRepaymentsResource:

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
        message = "split successfully"
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
                    allowedUsers = ('shiv@mintwalk.com', 'vaibhav.patil@mintwalk.com',
                                    '9967299619', 'sandesh.kulkarni@mintwalk.com', 'admin@mintloan.com')
                    # print input_dict["msgHeader"]["authLoginID"] in allowedUsers, input_dict["msgHeader"]["authLoginID"]
                    if input_dict["msgHeader"]["authLoginID"] in allowedUsers:
                        repay = Table("mw_loan_repayment_data",
                                      schema="mint_loan")
                        query = db.runQuery(Query.from_(repay).select(repay.star).where(
                            repay.AUTO_ID == input_dict["data"]["repayID"]))["data"]
                        repayAmount = query[0]["REPAY_AMOUNT"] if query else 0
                        #print(repayAmount)
                        if (input_dict["data"]["splitAmount1"] + input_dict["data"]["splitAmount2"]) == repayAmount:
                            splitDict = {k: (str(v) if v else None) for k, v in six.iteritems(query[0]) if k not in ("AUTO_ID", "REPAY_AMOUNT","TRANSACTION_REF_NO")}
                            # print splitDict
                            
                            inserted = db.Insert(db="mint_loan", table="mw_loan_repayment_data", compulsory=False, date=False,
                                                 REPAY_AMOUNT=str(input_dict["data"]["splitAmount1"]), 
                                                 TRANSACTION_REF_NO=(query[0]["TRANSACTION_REF_NO"]+"-1") if query[0]["TRANSACTION_REF_NO"] else None,
                                                 **splitDict)
                            if inserted:
                                db.Update(db="mint_loan", table="mw_loan_repayment_data", conditions={"AUTO_ID =": str(query[0]["AUTO_ID"])},
                                          REPAY_AMOUNT=str(
                                              ["data"]["splitAmount2"]))
                            else:
                                message = "split failed"
                        else:
                            message = "split amounts not matching with original amount"
                    else:
                        message = "spliting is now allowed for this user"
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update(
                            {"error": 0, "message": message})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
