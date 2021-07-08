from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table


class CreateRepaymentInfoResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"updated": ""}}
        errors = utils.errors
        success = "Repayment info successfully created"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='createRepaymentInfo', request=input_dict):
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
                    boolmap = {True: 1, False: 0}
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    updated = 0
                    if input_dict["data"]["update"] == 1:
                        r = Query.from_(repay).select(
                            "AUTO_ID").where(repay.LOGIN_ID == loginID)
                        r = db.runQuery(
                            r.where(repay.AUTO_ID == input_dict["data"]["repayInfoID"]))
                        if r["data"]:
                            updated = boolmap[db.Update(db="mint_loan", table="mw_loan_repayment_data",
                                                        REPAY_DATETIME=(str(input_dict["data"]["repayDatetime"])
                                                                        if int(input_dict["data"]["repayDatetime"]) > 0 else None),
                                                        DEPOSIT_DATETIME=(str(input_dict["data"]["depositDatetime"])
                                                                          if int(input_dict["data"]["depositDatetime"]) > 0 else None),
                                                        REPAY_AMOUNT=input_dict["data"][
                                                            "amount"] if input_dict["data"]["amount"] else None,
                                                        CUSTOMER_ID=input_dict["data"]["custID"] if input_dict[
                                                            "data"]["custID"] else None,
                                                        LOAN_REF_ID=input_dict["data"]["loanID"] if input_dict[
                                                            "data"]["loanID"] else None,
                                                        REPAY_INFO=input_dict["data"]["repayInfo"] if input_dict[
                                                            "data"]["repayInfo"] else None,
                                                        MODE_OF_PAYMENT=input_dict["data"][
                                                            "payMode"] if input_dict["data"]["payMode"] else None,
                                                        ACCEPTED_BY=input_dict["data"]["acceptBy"] if input_dict[
                                                            "data"]["acceptBy"] else None,
                                                        conditions={
                                                            "custID = ": custID, "AUTO_ID = ": input_dict["data"]["repayID"]},
                                                        MODIFIED_BY=input_dict["msgHeader"]["authLoginID"],
                                                        MODIFIED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))]
                            success = "Task successfully updated"
                        # input_dict["data"]["repayDatetime"]>0 and input_dict["data"]["repayInfo"]:
                        else:
                            updated = boolmap[db.Insert(db="mint_loan", table="mw_loan_repayment_data", compulsory=False,
                                                        REPAY_DATETIME=(str(input_dict["data"]["repayDatetime"])
                                                                        if int(input_dict["data"]["repayDatetime"]) > 0 else None),
                                                        DEPOSIT_DATETIME=(str(input_dict["data"]["depositDatetime"])
                                                                          if int(input_dict["data"]["depositDatetime"]) > 0 else None),
                                                        REPAY_AMOUNT=input_dict["data"][
                                                            "amount"] if input_dict["data"]["amount"] else None,
                                                        CUSTOMER_ID=input_dict["data"]["custID"] if input_dict[
                                                            "data"]["custID"] else None,
                                                        LOAN_REF_ID=input_dict["data"]["loanID"] if input_dict[
                                                            "data"]["loanID"] else None,
                                                        REPAY_INFO=input_dict["data"]["repayInfo"] if input_dict[
                                                            "data"]["repayInfo"] else None,
                                                        MODE_OF_PAYMENT=input_dict["data"][
                                                            "payMode"] if input_dict["data"]["payMode"] else None,
                                                        ACCEPTED_BY=input_dict["data"]["acceptBy"] if input_dict[
                                                            "data"]["acceptBy"] else None,
                                                        CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                        CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))]
                    else:
                        updated = boolmap[db.Insert(db="mint_loan", table="mw_loan_repayment_data", compulsory=False,
                                                    REPAY_DATETIME=(str(input_dict["data"]["repayDatetime"])
                                                                    if int(input_dict["data"]["repayDatetime"]) > 0 else None),
                                                    DEPOSIT_DATETIME=(str(input_dict["data"]["depositDatetime"])
                                                                      if int(input_dict["data"]["depositDatetime"]) > 0 else None),
                                                    REPAY_AMOUNT=input_dict["data"]["amount"] if input_dict["data"]["amount"] else None,
                                                    CUSTOMER_ID=input_dict["data"]["custID"] if input_dict["data"]["custID"] else None,
                                                    LOAN_REF_ID=input_dict["data"]["loanID"] if input_dict["data"]["loanID"] else None,
                                                    REPAY_INFO=input_dict["data"]["repayInfo"] if input_dict[
                                                        "data"]["repayInfo"] else None,
                                                    MODE_OF_PAYMENT=input_dict["data"][
                                                        "payMode"] if input_dict["data"]["payMode"] else None,
                                                    ACCEPTED_BY=input_dict["data"]["acceptBy"] if input_dict[
                                                        "data"]["acceptBy"] else None,
                                                    CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                    CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))]
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"]["updated"] = updated
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                # print output_dict
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
