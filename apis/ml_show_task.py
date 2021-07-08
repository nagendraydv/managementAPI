from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class ShowTaskResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {
            "tasks": [], "loans": [], "logins": [], "custData": {}}}
        errors = utils.errors
        success = "tasks loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='showTask', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # , filename='mysql-slave.config') # setting an instance of DB class
                db = DB(input_dict["msgHeader"]["authLoginID"])
                dbw = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(dbw).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    Data = {}
                    page = input_dict["data"]['page']
                    loginID = input_dict["data"]["loginID"]
                    tasklist = Table("mw_task_lists", schema="mint_loan_admin")
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    users = Table("mw_admin_user_master", schema="mint_loan_admin")
                    userTypes = Table(
                        "mw_admin_user_account_type", schema="mint_loan_admin")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    q = Query.from_(userTypes).select("LOGIN").where(
                        userTypes.ACCOUNT_TYPE == "FRONTDESK")
                    frontDesks = db.runQuery(Query.from_(users).select(
                        "LOGIN", "CITY").where(users.LOGIN.isin(q)))["data"]
                    numberLogins = {ele["LOGIN"]: ele["NAME"] for ele in db.runQuery(Query.from_(
                        users).select("LOGIN", "NAME").where(users.LOGIN.like('%@%').negate()))["data"]}
                    cities = [ele["CITY"] for ele in frontDesks]
                    fdSorted = {ele["LOGIN"]: ele["CITY"]
                                for ele in frontDesks}
                    loans = []
                    q = Query.from_(tasklist).select(tasklist.star)
                    indict = input_dict["data"]
                    if indict["custID"] != "":
                        q = q.where(tasklist.CUSTOMER_ID == indict["custID"])
                        q1 = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                            loanmaster.ID == loandetails.LOAN_MASTER_ID)
                        q1 = q1.select(loanmaster.LOAN_REFERENCE_ID, loandetails.EXPECTED_MATURITY_DATE,
                                       loandetails.PRINCIPAL, loanmaster.STATUS)
                        loans = db.runQuery(
                            q1.where(loanmaster.CUSTOMER_ID == indict["custID"]))["data"]
                        q2 = Query.from_(custcred).join(
                            profile, how=JoinType.left).on_field("CUSTOMER_ID")
                        Data = db.runQuery(q2.select(custcred.LOGIN_ID, custcred.CUSTOMER_ID, profile.NAME).where(
                            custcred.CUSTOMER_ID == indict["custID"]))["data"]
                        # print Data
                    if indict["loanID"] != "":
                        q = q.where(tasklist.LOAN_REF_ID == indict["loanID"])
                    if indict["loginID"] != "":
                        if indict["loginID"] in ('aparajeeta@mintwalk.com', 'poonam@mintwalk.com', '8369956399'):
                            q = q.where(tasklist.LOGIN_ID.isin(
                                [indict["loginID"], 'disbursementTeam']))
                        elif indict["loginID"] in ('vaibhav.patil@mintwalk.com', '9967299619'):
                            q = q.where(tasklist.LOGIN_ID.isin(
                                [indict["loginID"], 'repaymentTeam']))
                        elif indict["loginID"] in fdSorted:
                            q = q.where(tasklist.LOGIN_ID.isin(
                                [indict["loginID"], fdSorted[indict["loginID"]]]))
                        else:
                            q = q.where(tasklist.LOGIN_ID == indict["loginID"])
                    if (indict["createdBy"] != "" if ("createdBy" in indict) else False):
                        q = q.where(tasklist.CREATED_BY == indict["createdBy"])
                    if indict["status"] != "":
                        q = q.where(tasklist.STATUS == indict["status"])
                    if indict["fromDate"]:
                        if indict["toDate"]:
                            # .where(tasklist.STATUS.notin(["COMPLETED", "CANCEL"]))
                            q = q.where(tasklist.TASK_DATETIME >=
                                        indict["fromDate"])
                        else:
                            # .where(tasklist.STATUS.notin(["COMPLETED", "CANCEL"]))
                            q = q.where(tasklist.TASK_DATETIME >
                                        indict["fromDate"])
                    if indict["toDate"]:
                        q = q.where(tasklist.TASK_DATETIME < indict["toDate"])
                        # if not indict["fromDate"]:
                        #    q = q.where(tasklist.STATUS.notin(["COMPLETED", "CANCEL"]))
                    elif indict["days"] > 0:
                        q = q.where(tasklist.TASK_DATETIME >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    if indict["taskListID"] != "":
                        q = q.where(tasklist.TASK_LIST_ID ==
                                    indict["taskListID"])
                    respdict = db.runQuery(q.orderby(tasklist.TASK_LIST_ID, order=Order.desc).limit(
                        "%i,%i" % (page["startIndex"], page["size"])))
                    # print db.runQuery(q.orderby(tasklist.TASK_LIST_ID, order=Order.desc).limit("%i,%i"%(page["startIndex"], page["size"])))
                    for ele in respdict["data"]:
                        if ele["LOGIN_ID"] in numberLogins:
                            ele["LOGIN_ID"] = numberLogins[ele["LOGIN_ID"]]
                        if ele["CREATED_BY"] in numberLogins:
                            ele["CREATED_BY"] = numberLogins[ele["CREATED_BY"]]
                    logins = Query.from_(users).select(users.LOGIN).where(
                        users.ACCOUNT_STATUS == 'A').orderby(users.CREATED_DATE, order=Order.desc)
                    logins = list(set([ele["LOGIN"] for ele in db.runQuery(logins)["data"]] + ["disbursementTeam", "repaymentTeam", "MUM", "BAN", "KOL", "PUNE", "CHENNAI", "HYD", "DEL", "KOC", "CHD", "JAI", "AHD", "LUC"]) - set(fdSorted.keys()) - set(["sandesh.kulkarni@mintwalk.com", "admin@mintloan.com", "9967299619", "vinay.sawant@mintwalk.com",
                                                                                                                                                                                                                                                            "9136579921_LEFT", "spgupta@purshottaminvestofin.in_test312324", "prasanna@mintwalk.com", "nivedita.nayak@chaitanyaindia.in", "nitin.chorge@mintwalk.com", "aparajeeta@mintwalk.com", "poonam@mintwalk.com", "8369956399", "7021756843", "harilal.ayyappan@mintwalk.com", "vaibhav.patil@mintwalk.com"]))
                    token = generate(dbw).AuthToken()
                    if token["updated"]:
                        output_dict["data"]["tasks"] = utils.camelCase(
                            respdict["data"])
                        output_dict["data"]["loans"] = utils.camelCase(loans)
                        output_dict["data"]["logins"] = logins
                        output_dict["data"].update(
                            {"custData": utils.camelCase(Data[0]) if Data else {}})
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
