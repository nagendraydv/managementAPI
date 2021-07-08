from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType, Order
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
        output_dict = {"data": {"dueNow": 0, "due1Month": 0, "due3Month": 0, "writtenOff": 0, "largeOverdue": 0, "mediumOverdue": 0, "smallOverdue": 0,
                                "largeLoan": 0, "mediumLoan": 0, "smallLoan": 0, "companies": [], "todaysTasks": 0, "pendingTasks": 0, "upcomingTasks": 0},
                       "msgHeader": {"authToken": ""}}
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
                    prof = Table("mw_client_profile", schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    days_2 = (datetime.now() - timedelta(days=2)
                              ).strftime("%Y-%m-%d 00:00:00")
                    days_7 = (datetime.now() - timedelta(days=7)
                              ).strftime("%Y-%m-%d 00:00:00")
                    days_31 = (datetime.now() - timedelta(days=31)
                               ).strftime("%Y-%m-%d 00:00:00")
                    days_180 = (datetime.now() - timedelta(days=180)
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
                    q = Query.from_(prof).join(loanmaster, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select(prof.COMPANY_NAME).distinct()
                    companiesLive = db.runQuery(
                        q.where(loanmaster.STATUS.isin(['ACTIVE', 'REPAID'])))["data"]
                    companiesLive = [ele["COMPANY_NAME"]
                                     for ele in companiesLive]
                    if ("city" in input_dict["data"]):
                        if input_dict["data"]["city"] in ("BAN", "PUNE", "CHENNAI", "DEL", "HYD", "KOL", "KOC", "CHD", "JAI", "AHD", "LUC"):
                            Qb = Query.from_(profile).select("CUSTOMER_ID").where(
                                profile.CURRENT_CITY.isin(cities[input_dict["data"]["city"]]))
                            Q = Qb.where(profile.COMPANY_NAME == input_dict["data"]["company"]).where(
                                profile.CUSTOMER_ID.notin(callback))
                        elif input_dict["data"]["city"] == "MUM":
                            Qb = Query.from_(profile).select("CUSTOMER_ID").where(
                                profile.CURRENT_CITY.notin(sum(list(cities.values()), [])))
                            Q = Qb.where(profile.COMPANY_NAME == input_dict["data"]["company"]).where(
                                profile.CUSTOMER_ID.notin(callback))
                        else:
                            Qb = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                                profile.CUSTOMER_ID.notin(callback))
                            Q = Qb.where(profile.COMPANY_NAME ==
                                         input_dict["data"]["company"])
                    else:
                        Qb = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                            profile.CUSTOMER_ID.notin(callback))
                        Q = Qb.where(profile.COMPANY_NAME ==
                                     input_dict["data"]["company"])
                    Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                        datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                    QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                        loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                    QQ2 = Query.from_(loanmaster).select(functions.Count(loanmaster.LOAN_ACCOUNT_NO).as_(
                        "c")).where(loanmaster.STATUS.isin(['WRITTEN-OFF', 'WRITTEN_OFF']))
                    q4 = Query.from_(custcred).select("CUSTOMER_ID").where(custcred.STAGE.isin(
                        ['LOAN_ACTIVE']))  # .where(custcred.CUSTOMER_ID.notin(Qtemp))
                    q = Query.from_(emis).select(functions.Count(emis.CUSTOMER_ID).distinct().as_(
                        "c")).where(emis.CUSTOMER_ID.isin(Q.where(profile.CUSTOMER_ID.isin(q4))))
                    #q = Query.from_(emis).join(loanmaster, how=JoinType.left).on_field("CUSTOMER_ID").select(emis.CUSTOMER_ID, emis.OVERDUE_AMOUNT, emis.DUE_DATE, loanmaster.LOAN_PRODUCT_ID).where(emis.CUSTOMER_ID.isin(Q.where(profile.CUSTOMER_ID.isin(q4))))
                    # data=db.runQuery(q.where(emis.OVERDUE_AMOUNT>0).where(loanmaster.STATUS=='ACTIVE'))
                    dueNow = db.runQuery(q.where((emis.LOAN_ACCOUNT_NO.isin(QQ1)) & (emis.DUE_DATE > days_7) & (emis.OVERDUE_AMOUNT == (emis.TOTAL_DUE_FOR_PERIOD-emis.ADVANCE_PAYMENT_AMOUNT)) & (
                        emis.OVERDUE_AMOUNT > 500)))["data"][0]["c"]  # len({ele["CUSTOMER_ID"] for ele in filter(lambda x:x["DUE_DATE"]>days_7 and x["OVERDUE_AMOUNT"]>500, data["data"])})
                    due1Month = db.runQuery(q.where((emis.LOAN_ACCOUNT_NO.isin(QQ1)) & (emis.DUE_DATE < days_7) & (emis.DUE_DATE > days_180) & (emis.OVERDUE_AMOUNT > 500)))[
                        "data"][0]["c"]  # len({ele["CUSTOMER_ID"] for ele in filter(lambda x:x["DUE_DATE"]>days_7 and x["OVERDUE_AMOUNT"]>500, data["data"])})
                    due3Month = db.runQuery(q.where((emis.LOAN_ACCOUNT_NO.isin(QQ1)) & (emis.DUE_DATE < days_180) & (emis.OVERDUE_AMOUNT > 500)))[
                        "data"][0]["c"]  # len({ele["CUSTOMER_ID"] for ele in filter(lambda x:x["DUE_DATE"]<days_180 and x["OVERDUE_AMOUNT"]>500, data["data"])})
                    writtenOff = db.runQuery(QQ2.where(loanmaster.CUSTOMER_ID.isin(
                        Q.where(profile.CUSTOMER_ID.isin(q4)))))["data"][0]["c"]
                    largeOverdue = q.where(emis.LOAN_ACCOUNT_NO.isin(QQ1)).having(
                        functions.Sum(emis.OVERDUE_AMOUNT) > 5000).groupby(emis.CUSTOMER_ID)
                    #largeOverdue = db.runQuery(Query.from_(largeOverdue).select(functions.Count(largeOverdue.c).as_("c")))["data"][0]["c"]
                    mediumOverdue = q.where(emis.LOAN_ACCOUNT_NO.isin(QQ1)).having((functions.Sum(
                        emis.OVERDUE_AMOUNT) > 1500) & (functions.Sum(emis.OVERDUE_AMOUNT) < 5000)).groupby(emis.CUSTOMER_ID)
                    #mediumOverdue = db.runQuery(Query.from_(mediumOverdue).select(functions.Count(mediumOverdue.c).as_("c")))["data"][0]["c"]
                    smallOverdue = q.where(emis.LOAN_ACCOUNT_NO.isin(QQ1)).having((functions.Sum(
                        emis.OVERDUE_AMOUNT) < 1500) & (functions.Sum(emis.OVERDUE_AMOUNT) > 100)).groupby(emis.CUSTOMER_ID)
                    #smallOverdue = db.runQuery(Query.from_(smallOverdue).select(functions.Count(smallOverdue.c).as_("c")))["data"][0]["c"]
                    largeLoan = db.runQuery(q.where(emis.LOAN_ACCOUNT_NO.isin(QQ1.where(loanmaster.LOAN_PRODUCT_ID.isin([2, 12, 16])))).where(emis.OVERDUE_AMOUNT == emis.TOTAL_DUE_FOR_PERIOD))[
                        "data"][0]["c"]  # len({ele["CUSTOMER_ID"] for ele in filter(lambda x:x["LOAN_PRODUCT_ID"] in ['2','12','16'] and x["OVERDUE_AMOUNT"]>500, data["data"])})
                    mediumLoan = db.runQuery(q.where(emis.LOAN_ACCOUNT_NO.isin(QQ1.where(loanmaster.LOAN_PRODUCT_ID.isin([43, 44, 45, 52, 13])))).where(emis.OVERDUE_AMOUNT == emis.TOTAL_DUE_FOR_PERIOD))[
                        "data"][0]["c"]  # len({ele["CUSTOMER_ID"] for ele in filter(lambda x:x["LOAN_PRODUCT_ID"] in ['43','44','45','52','13'] and x["OVERDUE_AMOUNT"]>500, data["data"])})
                    smallLoan = db.runQuery(q.where(emis.LOAN_ACCOUNT_NO.isin(QQ1.where(loanmaster.LOAN_PRODUCT_ID.notin([2, 12, 16, 43, 44, 45, 52, 13])))).where(emis.OVERDUE_AMOUNT == emis.TOTAL_DUE_FOR_PERIOD))[
                        "data"][0]["c"]  # len({ele["CUSTOMER_ID"] for ele in filter(lambda x:x["LOAN_PRODUCT_ID"] not in ['43','44','45','52','13', '2', '12', '16'] and x["OVERDUE_AMOUNT"]>500, data["data"])})
                    action = db.runQuery(Query.from_(calldata).select(functions.Count(calldata.CUSTOMER_ID).distinct().as_("c")).where(
                        (calldata.INTERACTION_RESOLUTION_ID == '91') & (calldata.CUSTOMER_ID.isin(Qb.where(profile.CUSTOMER_ID.isin(q4))))))["data"][0]["c"]
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
                        output_dict["data"].update({"dueNow": dueNow, "due1Month": due1Month, "due3Month": due3Month, "writtenOff": writtenOff,
                                                    # "largeOverdue":largeOverdue, "mediumOverdue":mediumOverdue, "smallOverdue":smallOverdue,
                                                    "largeLoan": largeLoan, "mediumLoan": mediumLoan, "smallLoan": smallLoan, "action": action,
                                                    "selfCash": selfCash if selfCash else 0, "companies": companiesLive,
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
