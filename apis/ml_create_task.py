from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table


class CreateTaskResource:

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
        success = "Task successfully created"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='createTask', request=input_dict):
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
                    loginID = input_dict["data"]["loginID"]
                    boolmap = {True: 1, False: 0}
                    tasklist = Table("mw_task_lists", schema="mint_loan_admin")
                    users = Table("mw_admin_user_master",
                                  schema="mint_loan_admin")
                    userTypes = Table(
                        "mw_admin_user_account_type", schema="mint_loan_admin")
                    updated = 0
                    ind = input_dict["data"]
                    q = Query.from_(userTypes).select("ACCOUNT_TYPE").where(
                        userTypes.LOGIN == input_dict["msgHeader"]["authLoginID"])
                    frontDesk = "FRONTDESK" in [ele["ACCOUNT_TYPE"] for ele in db.runQuery(q)[
                        "data"]]
                    if frontDesk:
                        city = [ele["CITY"] for ele in db.runQuery(Query.from_(users).select(
                            "CITY").where(users.LOGIN == input_dict["msgHeader"]["authLoginID"]))["data"]]
                    else:
                        city = []
                    if input_dict["data"]["update"] == 1 or input_dict["data"]["update"] == "1":
                        task = Query.from_(tasklist).select("TASK_LIST_ID").where((tasklist.CREATED_BY == input_dict["msgHeader"]["authLoginID"]) | (tasklist.LOGIN_ID.isin((city + [input_dict["msgHeader"]["authLoginID"]] + ([] if input_dict["msgHeader"]["authLoginID"] not in (
                            'aparajeeta@mintwalk.com', 'poonam@mintwalk.com', '8369956399') else ['disbursementTeam']) + ([] if input_dict["msgHeader"]["authLoginID"] not in ('vaibhav.patil@mintwalk.com', '9967299619') else ['repaymentTeam'])))))
                        task = db.runQuery(task.where(
                            tasklist.TASK_LIST_ID == input_dict["data"]["taskListID"]))
                        if task["data"]:
                            updated = boolmap[db.Update(db="mint_loan_admin", table="mw_task_lists", LOGIN_ID=loginID,
                                                        TASK_DATETIME=(str(input_dict["data"]["taskDatetime"])
                                                                       if input_dict["data"]["taskDatetime"] > 0 else None),
                                                        STATUS=input_dict["data"]["status"] if input_dict["data"]["status"] else None,
                                                        LOAN_REF_ID=input_dict["data"]["loanID"] if input_dict[
                                                            "data"]["loanID"] else None,
                                                        conditions={
                                                            "TASK_LIST_ID = ": input_dict["data"]["taskListID"]},
                                                        MODIFIED_BY=input_dict["msgHeader"]["authLoginID"],
                                                        TASK=input_dict["data"]["task"].strip(
                                                        ) if input_dict["data"]["task"] else None,
                                                        MODIFIED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))]
                            success = "Task successfully updated"
                        # input_dict["data"]["taskDatetime"]>0 and input_dict["data"]["task"]:
                        else:
                            # boolmap[ db.Insert(db="mint_loan_admin", table="mw_task_lists", compulsory=False, LOGIN_ID=loginID,
                            updated = 0
                            #             TASK_DATETIME=(str(input_dict["data"]["taskDatetime"])
                            #                           if input_dict["data"]["taskDatetime"]>0 else None),
                            #           TASK=input_dict["data"]["task"] if input_dict["data"]["task"] else None,
                            #          CUSTOMER_ID = input_dict["data"]["custID"] if input_dict["data"]["custID"] else None,
                            #         LOAN_REF_ID = input_dict["data"]["loanID"] if input_dict["data"]["loanID"] else None,
                            #        STATUS = input_dict["data"]["status"] if input_dict["data"]["status"] else "PENDING",
                            #       CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                            #      CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))]
                    elif ind["custID"] and loginID and ind["task"] and ind["taskDatetime"]:
                        updated = boolmap[db.Insert(db="mint_loan_admin", table="mw_task_lists", compulsory=False, LOGIN_ID=loginID,
                                                    TASK_DATETIME=(str(input_dict["data"]["taskDatetime"])
                                                                   if input_dict["data"]["taskDatetime"] > 0 else None),
                                                    TASK=input_dict["data"]["task"].strip(
                                                    ) if input_dict["data"]["task"] else None,
                                                    CUSTOMER_ID=input_dict["data"]["custID"] if input_dict["data"]["custID"] else None,
                                                    LEAD_ID=input_dict["data"]["leadID"] if  input_dict["data"]['leadID'] else None,
                                                    LOAN_REF_ID=input_dict["data"]["loanID"] if input_dict["data"]["loanID"] else None,
                                                    STATUS=input_dict["data"]["status"] if input_dict["data"]["status"] else "PENDING",
                                                    CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                    CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))]
                    else:
                        updated = 0
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"]["updated"] = updated
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
