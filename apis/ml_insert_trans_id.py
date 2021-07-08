from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class InsertTransIDResource:

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
        message = "inserted successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:  # not validate.Request(api='', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    allowedUsers = ('shiv@mintwalk.com', 'vaibhav.patil@mintwalk.com', '9967299619', 'sandesh.kulkarni@mintwalk.com','admin@mintloan.com')
                    if input_dict["msgHeader"]["authLoginID"] in allowedUsers:
                        repay = Table("mw_loan_repayment_data",schema="mint_loan")
                        lm = Table("mw_client_loan_master", schema="mint_loan")
                        ld = Table("mw_client_loan_details",schema="mint_loan")
                        query = db.runQuery(Query.from_(repay).select(repay.star).where(repay.AUTO_ID == input_dict["data"]["repayID"]))["data"]
                        exist = query[0]["FINFLUX_TRAN_ID"] is None if query else False
                        if exist:
                            updated = db.Update(db="mint_loan", table="mw_loan_repayment_data", conditions={"AUTO_ID =": str(query[0]["AUTO_ID"])},FINFLUX_TRAN_ID=str(input_dict["data"]["tranID"]))
                            # print updated, "wo" in str(input_dict["data"]["tranID"]).lower()
                            if updated & ("wo" in str(input_dict["data"]["tranID"]).lower()):
                                q = Query.from_(lm).join(ld, how=JoinType.left).on(lm.ID == ld.LOAN_MASTER_ID)
                                q = q.select(lm.STATUS, lm.ID, ld.CURRENT_OUTSTANDING).where(lm.LOAN_REFERENCE_ID == query[0]["LOAN_REF_ID"])
                                q = db.runQuery(q)["data"]
                                written_off = (q[0]["STATUS"] == "WRITTEN-OFF") if q else False
                                # print written_off
                                if written_off:
                                    junk = db.Update(db="mint_loan", table="mw_client_loan_details", conditions={"LOAN_MASTER_ID =": str(q[0]["ID"])},
                                                     CURRENT_OUTSTANDING="%.2f" % (q[0]["CURRENT_OUTSTANDING"]-query[0]["REPAY_AMOUNT"]))
                                    # print junk
                        else:
                            message = "insert failed"
                    else:
                        message = "insertion of trans id is now allowed for this user"
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
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
