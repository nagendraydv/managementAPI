from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class DashboardInvestmentTeamResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"successTransactions": {"today": 0, "lastWeek": 0, "lastMonth": 0}, "todaysTasks": 0, "pendingTasks": 0, "upcomingTasks": 0,
                                "unsuccessTransactions": {"today": 0, "lastWeek": 0, "lastMonth": 0, "last3Month": 0}, "kycDone": {"today": 0, "lastWeek": 0, "lastMonth": 0, "last3Month": 0},
                                "kycDoneNoInvetment": {"today": 0, "lastWeek": 0, "lastMonth": 0, "last3Month": 0},
                                "callbackData": {"scheduledToday": 0, "scheduledLastWeek": 0, "scheduledLastMonth": 0, "upcoming": 0}},
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
                    kyc = Table("pan_status_check", schema="gc_reliance")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    purch = Table("reliance_purchase", schema="gc_reliance")
                    tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    users = Table("mw_admin_user_master",
                                  schema="mint_loan_admin")
                    userTypes = Table(
                        "mw_admin_user_account_type", schema="mint_loan_admin")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                    days_7 = (datetime.now() - timedelta(days=7)
                              ).strftime("%Y-%m-%d %H:%M:%S")
                    days_30 = (datetime.now() - timedelta(days=30)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                    days_90 = (datetime.now() - timedelta(days=90)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                    days_plus_1 = (datetime.now() + timedelta(days=1)
                                   ).strftime("%Y-%m-%d %H:%M:%S")
                    q = Query.from_(userTypes).select("LOGIN").where(
                        userTypes.ACCOUNT_TYPE == "FRONTDESK")
                    frontDesks = db.runQuery(Query.from_(users).select(
                        "LOGIN", "CITY").where(users.LOGIN.isin(q)))["data"]
                    cities = [ele["CITY"] for ele in frontDesks]
                    fdSorted = {ele["LOGIN"]: ele["CITY"]
                                for ele in frontDesks}
                    cities = {"BAN": ['Bangalore rural', 'bangalore kasavanalli', 'Bangalore', 'banglore', 'BAN', 'bangaloor', 'Bangalore ',
                                      'Bamgalore'], "PUNE": ['PUNE', 'punr'], "CHENNAI": ["Chennai"], "DELHI": ["delhi"],
                              "HYD": ["hyderabad", "HYDERABAD"], "KOL": ["kolkata", "Kolkata"], "KOC": ["KOCHI"], "CHD": ["CHANDIGARH"], "JAI": ["JAIPUR"],
                              "AHD": ["AHMEDABAD"], "LUC": ["LUCKNOW"]}
                    if "city" in input_dict["data"]:
                        if input_dict["data"]["city"] in ("BAN", "PUNE", "CHENNAI", "DELHI", "HYD", "KOL", "KOC", "CHD", "JAI", "AHD"):
                            QQ = Query.from_(profile).select("CUSTOMER_ID").where(
                                profile.CURRENT_CITY.isin(cities[input_dict["data"]["city"]]))
                        elif input_dict["data"]["city"] == "MUM":
                            QQ = Query.from_(profile).select("CUSTOMER_ID").where(
                                profile.CURRENT_CITY.notin(sum(list(cities.values()), [])))
                        else:
                            QQ = Query.from_(profile).select(
                                "CUSTOMER_ID").distinct()
                    else:
                        QQ = Query.from_(profile).select(
                            "CUSTOMER_ID").distinct()
                    Q = Query.from_(calldata).select(functions.Count(calldata.star).as_("c")).where(
                        calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19]))
                    Q = Q.where(calldata.CUSTOMER_ID.isin(QQ))
                    #Q11 = Query.from_(calldata).select(functions.Max(calldata.CREATED_DATE)).where(calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19])).groupby(calldata.CUSTOMER_ID)
                    #Q1 = Q1.join(Q11, how=JoinType.left).on_field("CUSTOMER_ID")
                    #Q2 = Query.from_(calldata).select("CUSTOMER_ID", "CALLBACK_DATETIME").where(calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19]))
                    #Q21 = Query.from_(calldata).select(functions.Max(calldata.CALLBACK_DATETIME)).where(calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19])).groupby(calldata.CUSTOMER_ID)
                    #Q2 = Q2.join(Q21, how=JoinType.left).on_field("CUSTOMER_ID")
                    # Q = Query.from_(Q1.as("a")).join(Q2, how=JoinType.left).on_field("CUSTOMER_ID").select("CUSTOMER_ID", "CREATED_DATE", "CALLBACK_DATETIME").where(Q2.CALLBACK_DATETIME<Q1.CREATED_DATE)
                    callbackPending = db.runQuery(
                        Q.where(calldata.CALLBACK_DATETIME > days_30))["data"]
                    callbackToday = db.runQuery(
                        Q.where(calldata.CALLBACK_DATETIME > today))["data"]
                    callbackWeek = db.runQuery(
                        Q.where(calldata.CALLBACK_DATETIME > days_7))["data"]
                    callbackUpcoming = db.runQuery(
                        Q.where(calldata.CALLBACK_DATETIME > days_plus_1))["data"]
                    q = Query.from_(kyc).join(
                        pan, how=JoinType.left).on(kyc.PAN == pan.PAN_NO)
                    q = q.select(functions.Count(
                        pan.CUSTOMER_ID).distinct().as_("c"))
                    inv = Query.from_(purch).select(purch.PAN).distinct().where(
                        purch.TRANSACTION_STATUS == 'Success')
                    kycToday = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (kyc.CREATED_AT > today) & (
                        pan.CUSTOMER_ID.isin(QQ))))["data"]  # pan.CUSTOMER_ID.notnull()
                    noInvToday = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (kyc.CREATED_AT > today) & (kyc.PAN.notin(inv)) &
                                                     (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    kyc7Days = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (
                        kyc.CREATED_AT > days_7) & (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    noInv7Days = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (kyc.CREATED_AT > days_7) & (kyc.PAN.notin(inv)) &
                                                     (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    kyc30Days = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (
                        kyc.CREATED_AT > days_30) & (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    noInv30Days = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (kyc.CREATED_AT > days_30) & (kyc.PAN.notin(inv)) &
                                                      (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    kyc90Days = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (
                        kyc.CREATED_AT > days_90) & (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    noInv90Days = db.runQuery(q.where((kyc.KYC_FLAG == 'Y') & (kyc.CREATED_AT > days_90) & (kyc.PAN.notin(inv)) &
                                                      (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    q = Query.from_(purch).join(
                        pan, how=JoinType.left).on(purch.PAN == pan.PAN_NO)
                    q = q.select(functions.Count(
                        pan.CUSTOMER_ID).distinct().as_("c"))
                    invSucToday = db.runQuery(q.where((purch.TRANSACTION_STATUS == 'Success') & (purch.CREATED_AT > today) &
                                                      (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invSuc7Days = db.runQuery(q.where((purch.TRANSACTION_STATUS == 'Success') & (purch.CREATED_AT > days_7) &
                                                      (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invSuc30Days = db.runQuery(q.where((purch.TRANSACTION_STATUS == 'Success') & (purch.CREATED_AT > days_30) &
                                                       (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invSuc90Days = db.runQuery(q.where((purch.TRANSACTION_STATUS == 'Success') & (purch.CREATED_AT > days_90) &
                                                       (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invFailToday = db.runQuery(q.where((purch.TRANSACTION_STATUS != 'Success') & (purch.CREATED_AT > today) &
                                                       (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invFail7Days = db.runQuery(q.where((purch.TRANSACTION_STATUS != 'Success') & (purch.CREATED_AT > days_7) &
                                                       (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invFail30Days = db.runQuery(q.where((purch.TRANSACTION_STATUS != 'Success') & (purch.CREATED_AT > days_30) &
                                                        (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    invFail90Days = db.runQuery(q.where((purch.TRANSACTION_STATUS != 'Success') & (purch.CREATED_AT > days_90) &
                                                        (pan.CUSTOMER_ID.isin(QQ))))["data"]
                    q = Query.from_(tasks).select(
                        functions.Count(tasks.star).as_('C'))
                    q = q.where(tasks.LOGIN_ID.isin(
                        [input_dict["msgHeader"]["authLoginID"], 'disbursementTeam']))
                    q1 = q.where((tasks.TASK_DATETIME >= int(datetime.now().date().strftime("%s"))) & (tasks.STATUS.notin(["COMPLETED", "CANCEL"]) &
                                                                                                       (tasks.TASK_DATETIME < int((datetime.now()+timedelta(days=1)).date().strftime("%s")))))
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
                        output_dict["data"].update({"callbackData": {"scheduledToday": callbackToday, "scheduledLastWeek": callbackWeek, "scheduledLastMonth": callbackPending, "upcoming": callbackUpcoming},
                                                    "successTransactions": {"today": invSucToday, "lastWeek": invSuc7Days, "lastMonth": invSuc30Days, "last3Month": invSuc90Days},
                                                    "unsuccessTransactions": {"today": invFailToday, "lastWeek": invFail7Days,
                                                                              "lastMonth": invFail30Days, "last3Month": invFail90Days},
                                                    "kycDone": {"today": kycToday, "lastWeek": kyc7Days, "lastMonth": kyc30Days, "last3Month": kyc90Days},
                                                    "kycDoneNoInvetment": {"today": noInvToday, "lastWeek": noInv7Days, "lastMonth": noInv30Days, "last3Month": noInv90Days},
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
