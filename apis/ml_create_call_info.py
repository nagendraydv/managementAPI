from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType


class CreateCallInfoResource:

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
        success = "Call data recorded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='createCallInfo', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                # print val_error
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = str(input_dict["data"]["customerID"])
                    boolmap = {True: 1, False: 0}
                    calldata = Table("mw_call_data", schema="mint_loan")
                    emi = Table("mw_client_loan_emi_details",
                                schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    q = Query.from_(emi).join(lm, how=JoinType.left).on(
                        lm.LOAN_ACCOUNT_NO == emi.LOAN_ACCOUNT_NO).select("OVERDUE_AMOUNT")
                    q = db.runQuery(q.where((lm.CUSTOMER_ID == custID) & (lm.STATUS.isin(
                        ["ACTIVE", "WRITTEN_OFF"])) & (emi.OVERDUE_AMOUNT > 0)))["data"]
                    overdue = str(int(
                        sum(ele["OVERDUE_AMOUNT"] for ele in q if ele["OVERDUE_AMOUNT"]))) if q else "0"
                    Noverdue = str(len(q))
                    updated = 0
                    if input_dict["data"]["update"] == 1:
                        q = Query.from_(calldata).select("AUTO_ID").where(calldata.CUSTOMER_ID == custID)
                        callInfo = db.runQuery(q.where(calldata.AUTO_ID == input_dict["data"]["id"]))
                        if callInfo["data"]:
                            print("yes")
                            updated = boolmap[db.Insert(db="mint_loan", table="mw_call_data", compulsory=False, CUSTOMER_ID=custID, date=False,
                                                        CREATED_BY=input_dict["msgHeader"][
                                                            "authLoginID"], COMMENTS=input_dict["data"]["comments"],
                                                        INTERACTION_REASON_ID=str(
                                                            input_dict["data"]["reasonID"]),
                                                        INTERACTION_RESOLUTION_ID=str(
                                                            input_dict["data"]["resolutionID"]),
                                                        MOBILE_NUMBER=input_dict["data"][
                                                            "mobileNumber"] if "mobileNumber" in input_dict["data"] else None,
                                                        EMIS_OVERDUE=Noverdue, OVERDUE_AMOUNT=overdue,
                                                        LOAN_ACCOUNT_NO=str(
                                                            input_dict["data"]["loanID"]) if "loanID" in input_dict["data"] else None,
                                                        CITY=input_dict["data"]["city"] if "city" in input_dict["data"] else None,
                                                        CALLBACK_DATETIME=input_dict["data"][
                                                            "datetime"] if "datetime" in input_dict["data"] else None,
                                                        CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))]
                            junk = db.dictcursor.execute("SELECT LAST_INSERT_ID()")
                            id2 = str(db.dictcursor.fetchone()
                                      ["LAST_INSERT_ID()"])
                            updated = boolmap[db.Update(db="mint_loan", table="mw_call_data", conditions={"AUTO_ID=": str(input_dict["data"]["id"])},
                                                        FOLLOW_UP_BY=input_dict["msgHeader"]["authLoginID"], FOLLOW_UP_ID=id2)]
                        else:
                            print("yes")
                            updated = boolmap[db.Insert(db="mint_loan", table="mw_call_data", compulsory=False, CUSTOMER_ID=custID, date=False,
                                                        CREATED_BY=input_dict["msgHeader"][
                                                            "authLoginID"], COMMENTS=input_dict["data"]["comments"],
                                                        INTERACTION_REASON_ID=str(
                                                            input_dict["data"]["reasonID"]),
                                                        INTERACTION_RESOLUTION_ID=str(
                                                            input_dict["data"]["resolutionID"]),
                                                        MOBILE_NUMBER=input_dict["data"][
                                                            "mobileNumber"] if "mobileNumber" in input_dict["data"] else None,
                                                        EMIS_OVERDUE=Noverdue, OVERDUE_AMOUNT=overdue,
                                                        LOAN_ACCOUNT_NO=str(
                                                            input_dict["data"]["loanID"]) if "loanID" in input_dict["data"] else None,
                                                        CITY=input_dict["data"]["city"] if "city" in input_dict["data"] else None,
                                                        CALLBACK_DATETIME=input_dict["data"][
                                                            "datetime"] if "datetime" in input_dict["data"] else None,
                                                        CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))]
                    else:
                        updated = boolmap[db.Insert(db="mint_loan", table="mw_call_data", compulsory=False, CUSTOMER_ID=custID, date=False,
                                                    CREATED_BY=input_dict["msgHeader"][
                                                        "authLoginID"], COMMENTS=input_dict["data"]["comments"],
                                                    INTERACTION_REASON_ID=input_dict["data"]["reasonID"],
                                                    INTERACTION_RESOLUTION_ID=input_dict["data"]["resolutionID"],
                                                    MOBILE_NUMBER=input_dict["data"][
                                                        "mobileNumber"] if "mobileNumber" in input_dict["data"] else None,
                                                    EMIS_OVERDUE=Noverdue, OVERDUE_AMOUNT=overdue,
                                                    LOAN_ACCOUNT_NO=str(
                                                        input_dict["data"]["loanID"]) if "loanID" in input_dict["data"] else None,
                                                    CITY=input_dict["data"]["city"] if "city" in input_dict["data"] else None,
                                                    CALLBACK_DATETIME=input_dict["data"][
                                                        "datetime"] if "datetime" in input_dict["data"] else None,
                                                    CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))]
                        # print updated
                    token = generate(db).AuthToken()
                    # print token
                    if token["updated"]:
                        output_dict["data"]["updated"] = updated
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                # print output_dict
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
