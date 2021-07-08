from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions
from six.moves import range


class DashboardOutcallResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"firstPriority": 0, "secondPriority": 0, "thirdPriority": 0, "fourthPriority": 0, "todaysTasks": 0,
                                "pendingTasks": 0, "upcomingTasks": 0}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            # not validate.Request(api='dashboard', request=input_dict):
            if False:
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    users = Table("mw_admin_user_master",
                                  schema="mint_loan_admin")
                    userTypes = Table(
                        "mw_admin_user_account_type", schema="mint_loan_admin")
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",
                                 schema="mint_loan")
                    tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    clog = Table("mw_customer_change_log", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    log = Table("mw_call_data", schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    days_2 = (datetime.now() - timedelta(days=2)
                              ).strftime("%Y-%m-%d 00:00:00")
                    days_7 = (datetime.now() - timedelta(days=7)
                              ).strftime("%Y-%m-%d 00:00:00")
                    days_31 = (datetime.now() - timedelta(days=15)
                               ).strftime("%Y-%m-%d 00:00:00")
                    today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                    q = Query.from_(userTypes).select("LOGIN").where(
                        userTypes.ACCOUNT_TYPE == "FRONTDESK")
                    frontDesks = db.runQuery(Query.from_(users).select(
                        "LOGIN", "CITY").where(users.LOGIN.isin(q)))["data"]
                    cities = [ele["CITY"] for ele in frontDesks]
                    fdSorted = {ele["LOGIN"]: ele["CITY"]
                                for ele in frontDesks}
                    callback = Query.from_(calldata).select("CUSTOMER_ID").distinct().where((calldata.CREATED_DATE > today) |
                                                                                            (calldata.CALLBACK_DATETIME > today))
                    cities = {"BAN": ['Bangalore rural', 'bangalore kasavanalli', 'Bangalore', 'banglore', 'BAN', 'bangaloor', 'Bangalore ',
                                      'Bamgalore'], "PUNE": ['PUNE', 'punr'], "CHENNAI": ["Chennai"], "DEL": ["delhi"],
                              "HYD": ["hyderabad", "HYDERABAD"], "KOL": ["kolkata", "Kolkata"], "KOC": ["KOCHI"], "CHD": ["CHANDIGARH"], "JAI": ["JAIPUR"],
                              "AHD": ["AHMEDABAD"], "LUC": ["LUCKNOW"]}
                    if "city" in input_dict["data"]:
                        if input_dict["data"]["city"] in ("BAN", "PUNE", "CHENNAI", "DEL", "HYD", "KOL", "KOC", "CHD", "JAI", "AHD", "LUC"):
                            Q = Query.from_(profile).select("CUSTOMER_ID").where(
                                profile.CURRENT_CITY.isin(cities[input_dict["data"]["city"]]))
                            Q = Q.where(profile.COMPANY_NAME.notin(["UBER AUTO"])).where(
                                profile.CUSTOMER_ID.notin(callback))
                        elif input_dict["data"]["city"] == "MUM":
                            Q = Query.from_(profile).select("CUSTOMER_ID").where(
                                profile.CURRENT_CITY.notin(sum(list(cities.values()), [])))
                            Q = Q.where(profile.COMPANY_NAME.notin(["UBER AUTO"])).where(
                                profile.CUSTOMER_ID.notin(callback))
                        else:
                            Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                                profile.CUSTOMER_ID.notin(callback))
                            Q = Q.where(
                                profile.COMPANY_NAME.notin(["UBER AUTO"]))
                    else:
                        Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                            profile.CUSTOMER_ID.notin(callback))
                        Q = Q.where(profile.COMPANY_NAME.notin(["UBER AUTO"]))
                    q11 = Query.from_(clog).select("CUSTOMER_ID").distinct().where((clog.DATA_VALUE == "AWAITING_ADDITIONAL_DOCS") &
                                                                                   (clog.CREATED_DATE < (datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d")))
                    q1 = Query.from_(kycdocs).select("CUSTOMER_ID").distinct().where((kycdocs.CREATED_DATE > days_2) &
                                                                                     (kycdocs.VERIFICATION_STATUS.isnull()))  # |
                    # (kycdocs.CUSTOMER_ID.isin(q11))) This slows the query
                    q4 = Query.from_(custcred).select("CUSTOMER_ID").where(
                        custcred.STAGE.isin(['AWAITING_ADDITIONAL_DOCS', 'AWAITING_LOAN_APPLICATION']))
                    qc = Query.from_(custcred).select(functions.Count(custcred.CUSTOMER_ID).distinct(
                    ).as_("c")).where(custcred.CUSTOMER_ID.isin(Q.where(profile.CUSTOMER_ID.isin(q4))))
                    QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                        loanmaster.STATUS.isin(['ACTIVE', 'WRITTEN-OFF']))
                    fdata = db.runQuery(qc.where((custcred.CUSTOMER_ID.notin(q1)) & (
                        custcred.STAGE == "AWAITING_ADDITIONAL_DOCS")))["data"][0]["c"]
                    q4 = Query.from_(custcred).select("CUSTOMER_ID").where(
                        custcred.STAGE.isin(['LOAN_ACTIVE']))
                    q = Query.from_(emis).select(functions.Count(emis.CUSTOMER_ID).distinct().as_(
                        "c")).where(emis.CUSTOMER_ID.isin(Q.where(profile.CUSTOMER_ID.isin(q4))))
                    sdata = db.runQuery(q.where((emis.LOAN_ACCOUNT_NO.isin(QQ1)) & (
                        emis.DUE_DATE < days_31) & (emis.OVERDUE_AMOUNT > 0)))["data"][0]["c"]
                    tdata = db.runQuery(q.where((emis.LOAN_ACCOUNT_NO.isin(QQ1)) & (emis.DUE_DATE < days_7) & (
                        emis.DUE_DATE > days_31) & (emis.OVERDUE_AMOUNT > 0)))["data"][0]["c"]
                    frdata = db.runQuery(qc.where((custcred.STAGE == 'AWAITING_LOAN_APPLICATION')))[
                        "data"][0]["c"]
                    Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                        profile.CUSTOMER_ID.notin(callback))
                    Q = Q.where(profile.COMPANY_NAME.isin(["UBER AUTO"]))
                    q = Query.from_(emis).select(functions.Count(
                        emis.CUSTOMER_ID).distinct().as_("c")).where(emis.CUSTOMER_ID.isin(Q))
                    f2data = db.runQuery(q.where((emis.DUE_DATE < days_31) & (
                        emis.OVERDUE_AMOUNT > 0)))["data"][0]["c"]
                    s2data = db.runQuery(q.where((emis.DUE_DATE < days_7) & (
                        emis.DUE_DATE > days_31) & (emis.OVERDUE_AMOUNT > 0)))["data"][0]["c"]
                    q = Query.from_(tasks).select(functions.Count(tasks.star).as_('C')).where(tasks.LOGIN_ID.isin(
                        [input_dict["msgHeader"]["authLoginID"], fdSorted[input_dict["msgHeader"]["authLoginID"]] if input_dict["msgHeader"]["authLoginID"] in fdSorted else 'a']))
                    q1 = q.where((tasks.TASK_DATETIME >= int(datetime.now().date().strftime("%s"))) & (tasks.STATUS.notin(["COMPLETED", "CANCEL"]) &
                                                                                                       (tasks.TASK_DATETIME < int((datetime.now()+timedelta(days=1)).date().strftime("%s")))))
                    selfCash = {}
                    for i in range(7):
                        epoch = (datetime.now() + timedelta(days=i-3)
                                 ).date().strftime("%s")
                        epoch2 = (datetime.now() + timedelta(days=i-2)
                                  ).date().strftime("%s")
                        epoch3 = (datetime.now() + timedelta(days=i-3)
                                  ).date().strftime("%Y-%m-%d")
                        sq = Query.from_(repay).select(functions.Sum(repay.REPAY_AMOUNT).as_("s")).where((repay.MODE_OF_PAYMENT == "CASH") &
                                                                                                         (repay.CREATED_DATE > epoch) &
                                                                                                         (repay.CREATED_DATE < epoch2))
                        selfCash.update({epoch3: db.runQuery(sq.where(
                            repay.CREATED_BY == input_dict["msgHeader"]["authLoginID"]))["data"][0]["s"]})
                        selfCash[epoch3] = selfCash[epoch3] if selfCash[epoch3] is not None else 0
                    tTasks = db.runQuery(q1.orderby(
                        tasks.TASK_DATETIME))["data"]
                    tTasks = tTasks[0]["C"] if tTasks else 0
                    yTasks = db.runQuery(q.where((tasks.TASK_DATETIME < int(datetime.now().date().strftime("%s"))) &
                                                 (tasks.STATUS.notin(["COMPLETED", "CANCEL"]))).orderby(tasks.TASK_DATETIME))
                    yTasks = yTasks["data"][0]["C"] if yTasks["data"] else 0
                    uTasks = db.runQuery(q.where((tasks.TASK_DATETIME > int((datetime.now()+timedelta(days=1)).date().strftime("%s"))) &
                                                 (tasks.STATUS.notin(["COMPLETED", "CANCEL"]))).orderby(tasks.TASK_DATETIME))
                    uTasks = uTasks["data"][0]["C"] if uTasks["data"] else 0
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"firstPriority": fdata, "secondPriority": sdata, "thirdPriority": tdata,
                                                    "fourthPriority": 0, "uberAuto": {"firstPriority": f2data, "secondPriority": s2data},
                                                    "uberCar": {"firstPriority": fdata, "secondPriority": sdata, "thirdPriority": tdata,
                                                                "fourthPriority": 0}, "selfCash": selfCash if selfCash else 0,
                                                    "todaysTasks": tTasks, "pendingTasks": yTasks, "upcomingTasks": uTasks})
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
