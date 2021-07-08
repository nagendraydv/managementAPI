from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class DashboardVerificationTeamResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"uberCar": {"firstPriority": 0, "secondPriority": 0, "thirdPriority": 0, "fourthPriority": 0, "fifthPriority": 0, "sixthPriority": 0},"enkash":{"firstPriority":0}, "swiggy": {}, "company": {},
                                "uberAuto": {}, "todaysTasks": 0, "pendingTasks": 0, "upcomingTasks": 0},
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
            if not validate.Request(api='dashboard', request=input_dict):
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
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    bank = Table("mw_cust_bank_detail", schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",schema="mint_loan")
                    derived = Table("mw_customer_derived_data",
                                    schema="mint_loan")
                    leadProfile=Table("lead_profile",schema="mw_lead")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    cprof = Table("mw_profile_info", schema="mw_company_3")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    days_7 = (datetime.now() - timedelta(days=7)
                              ).strftime("%Y-%m-%d %H:%M:%S")
                    # FIND MAXDATE OF CHANGELOG FOR EACH CUSTOMER
                    q4 = Query.from_(custcred).join(profile, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select("CUSTOMER_ID")
                    q4 = q4.where(custcred.STAGE == 'AWAITING_ADDITIONAL_DOCS').where(
                        profile.COMPANY_NAME.isin(["UBER", "UBER AUTO"]))
                    q3 = Query.from_(log).select(
                        log.CUSTOMER_ID, functions.Max(log.CREATED_DATE).as_("maxdate"))
                    q3 = q3.where(log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS').groupby(
                        log.CUSTOMER_ID)
                    q2 = Query.from_(log).join(q3).on_field("CUSTOMER_ID").select(log.star).where((log.CREATED_DATE == q3.maxdate) &
                                                                                                  (log.CUSTOMER_ID.isin(q4)) &
                                                                                                  (log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS'))
                    q1 = Query.from_(kycdocs).join(
                        q2, how=JoinType.left).on_field("CUSTOMER_ID")
                    q1 = q1.select("CUSTOMER_ID").distinct().where((kycdocs.CUSTOMER_ID.isin(q4)) & (kycdocs.CREATED_DATE > days_7) & (kycdocs.VERIFICATION_STATUS.isnull()) &
                                                                   ((q2.CREATED_DATE.isnull()) | (kycdocs.CREATED_DATE > q2.CREATED_DATE)))
                    #q4 = Query.from_(income).select("CUSTOMER_ID").distinct().where(income.CUSTOMER_ID.isin(q1))
                    q = Query.from_(custcred).select(functions.Count(
                        custcred.CUSTOMER_ID).distinct().as_("c"))
                    junk = db.dictcursor.execute(db.pikastr(q.where(
                        (custcred.CUSTOMER_ID.isin(q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))))
                    fdata = db.dictcursor.fetchall()
                    fdata = fdata[0]["c"]
                    q = Query.from_(profile).join(custcred).on_field("CUSTOMER_ID").select(
                        functions.Count(profile.CUSTOMER_ID).distinct().as_("c"))
                    loginID = input_dict["msgHeader"]["authLoginID"]
                    sdata = db.runQuery(q.where((custcred.STAGE != "REJECTED") &
                                                (profile.COMPANY_NAME.isin(["UBER", "UBER AUTO"])) & ((profile.NAME_VERIFIED == 'P') |
                                                                                                      (profile.NUMBER_VERIFIED == 'P')) & (profile.NAME.notnull())))["data"][0]["c"]
                    q1 = Query.from_(custcred).select("CUSTOMER_ID").where(
                        custcred.STAGE.isin(["AWAITING_VERIFICATION", "AWAITING_UBER_DATA"]))
                    q = Query.from_(profile).select(functions.Count(
                        profile.CUSTOMER_ID).distinct().as_("c"))
                    tdata = db.runQuery(q.where((profile.COMPANY_NAME.isin(["UBER", "UBER AUTO"])) & (profile.NAME_VERIFIED == '1') & (profile.NUMBER_VERIFIED == '1') &
                                                (profile.CUSTOMER_ID.isin(q1))))["data"][0]["c"]
                    #q = Query.from_(kycdocs).join(custcred).on_field("CUSTOMER_ID").select(functions.Count(kycdocs.CUSTOMER_ID).distinct().as_("c"))
                    # frdata = db.runQuery(q.where((custcred.STAGE!="REJECTED") & (kycdocs.DOCUMENT_TYPE_ID=='113') &
                    #                             (kycdocs.VERIFICATION_STATUS.isnull())))["data"][0]["c"]
                    # .union(Query.from_(kycdocs).select(kycdocs.CUSTOMER_ID).distinct())
                    q2 = Query.from_(kyc).select(
                        kyc.CUSTOMER_ID).where(kyc.NAME.notnull())
                    q3 = Query.from_(cprof).select(cprof.CUSTOMER_ID).where(
                        (cprof.CONFIRMED_CUSTOMER_ID != 0) & (cprof.CONFIRMED_CUSTOMER_ID.notnull()))
                    q1 = Query.from_(profile).select(profile.CUSTOMER_ID).where((profile.CUSTOMER_ID.isin(
                        q2)) | (profile.NAME.notnull()))  # .where(profile.CUSTOMER_ID.notin(q3))
                    q = Query.from_(cprof).select(functions.Count(cprof.CUSTOMER_ID).distinct().as_("c")).where((cprof.CUSTOMER_ID.isin(q1)) | (
                        cprof.CUSTOMER_ID.isin(Query.from_(kycdocs).select(kycdocs.CUSTOMER_ID).distinct()))).where(cprof.CUSTOMER_ID.notin(q3))
                    frdata = db.runQuery(q.where(cprof.CONFIRMED_CUSTOMER_ID.isnull()))["data"][0]["c"]
                    max_week = db.runQuery(Query.from_(derived).select(
                        functions.Max(derived.WEEK).as_("mweek")))["data"][0]["mweek"]
                    q22 = Query.from_(lm).select(lm.CUSTOMER_ID).where(
                        lm.LOAN_PRODUCT_ID.isin([1, 6, 7, 8, 9, 11, 14]))
                    q2 = Query.from_(derived).select(
                        derived.CUSTOMER_ID).distinct()
                    q2 = q2.where((derived.WEEK == max_week) & (derived.AVERAGE_3_WEEK >= 18000) & (derived.AVERAGE_10_WEEK >= 18000) & (derived.LAST_WEEK_INCOME > 3000) &
                                  (derived.WEEK_FOR_LATEST_DATA > (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d")))
                    #q1 = Query.from_(log).select("CUSTOMER_ID").where(log.DATA_VALUE=='LOW_INCOME').where(log.CUSTOMER_ID.isin(q2))
                    # q1 = q1.where(((log.RETAINED_DATE<days_7) | log.RETAINED_DATE.isnull())) & (log.CREATED_DATE<days_7)).groupby(log.CUSTOMER_ID)
                    q = Query.from_(custcred).select(functions.Count(
                        custcred.CUSTOMER_ID).distinct().as_("c"))
                    ftdata = db.runQuery(q.where((custcred.STAGE == 'LOW_INCOME') & (
                        custcred.CUSTOMER_ID.isin(q2)) & (custcred.CUSTOMER_ID.notin(q22))))["data"][0]["c"]
                    q23 = Query.from_(kycdocs).select(kycdocs.CUSTOMER_ID).distinct().where((kycdocs.DOCUMENT_TYPE_ID.isin(
                        [102, 106, 117, 118])) & ((kycdocs.VERIFICATION_STATUS.isnull()) | (kycdocs.VERIFICATION_STATUS == 'Y')))
                    q = Query.from_(bank).join(custcred, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    q = q.select(functions.Count(bank.CUSTOMER_ID).as_("c")).where(
                        bank.ADDITIONALLY_ADDED == 1).where(bank.DELETE_STATUS_FLAG == 0)
                    q = q.where(profile.COMPANY_NAME.isin(
                        ["UBER", "UBER AUTO"]))
                    q = q.where(custcred.STAGE.isin(
                        ["AWAITING_LOAN_APPLICATION", "AWAITING_RE-VERIFICATION", "LOAN_APPROVED", "LOAN_IN_PROCESS"]))
                    sxdata = db.runQuery(q.where(((bank.DOCUMENT_STATUS.isin(['P', 'R'])) | (bank.DOCUMENT_STATUS.isnull())) & (
                        bank.DEFAULT_STATUS_FLAG == 0) & (bank.CUSTOMER_ID.isin(q23))))["data"][0]["c"]
                    company = {}
                    qq = Query.from_(custcred).select(functions.Count(custcred.CUSTOMER_ID).as_(
                        "c")).where(custcred.STAGE == 'COMPANY_LOAN_APPROVED')
                    #q = Query.from_(profile).select(functions.Count(profile.CUSTOMER_ID).as_("c"))
                    #q = q.where(profile.COMPANY_NAME=='swiggy').where(profile.CUSTOMER_ID.isin(qq))
                    company.update(
                        {"awaitingVerification": db.runQuery(qq)["data"][0]["c"]})
                    swiggy = {}
                    q4 = Query.from_(custcred).join(profile, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select("CUSTOMER_ID")
                    q4 = q4.where(custcred.STAGE == 'AWAITING_ADDITIONAL_DOCS').where(
                        profile.COMPANY_NAME == "SWIGGY")
                    q3 = Query.from_(log).select(
                        log.CUSTOMER_ID, functions.Max(log.CREATED_DATE).as_("maxdate"))
                    q3 = q3.where(log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS').groupby(
                        log.CUSTOMER_ID)
                    q2 = Query.from_(log).join(q3).on_field("CUSTOMER_ID").select(log.star).where((log.CREATED_DATE == q3.maxdate) &
                                                                                                  (log.CUSTOMER_ID.isin(q4)) &
                                                                                                  (log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS'))
                    q1 = Query.from_(kycdocs).join(
                        q2, how=JoinType.left).on_field("CUSTOMER_ID")
                    q1 = q1.select("CUSTOMER_ID").distinct().where((kycdocs.CUSTOMER_ID.isin(q4)) & (kycdocs.CREATED_DATE > days_7) & (kycdocs.VERIFICATION_STATUS.isnull()) &
                                                                   ((q2.CREATED_DATE.isnull()) | (kycdocs.CREATED_DATE > q2.CREATED_DATE)))
                    #q4 = Query.from_(income).select("CUSTOMER_ID").distinct().where(income.CUSTOMER_ID.isin(q1))
                    q = Query.from_(custcred).select(functions.Count(
                        custcred.CUSTOMER_ID).distinct().as_("c"))
                    junk = db.dictcursor.execute(db.pikastr(q.where(
                        (custcred.CUSTOMER_ID.isin(q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))))
                    swiggy_fdata = db.dictcursor.fetchall()
                    swiggy_fdata = swiggy_fdata[0]["c"]
                    q = Query.from_(profile).join(custcred).on_field("CUSTOMER_ID").select(
                        functions.Count(profile.CUSTOMER_ID).distinct().as_("c"))
                    swiggy_sdata = db.runQuery(q.where((custcred.STAGE != "REJECTED") &
                                                       (profile.COMPANY_NAME == "SWIGGY") & ((profile.NAME_VERIFIED == 'P') |
                                                                                             (profile.NUMBER_VERIFIED == 'P')) & (profile.NAME.notnull())))["data"][0]["c"]
                    q1 = Query.from_(custcred).select("CUSTOMER_ID").where(
                        custcred.STAGE.isin(["AWAITING_VERIFICATION", "AWAITING_UBER_DATA"]))
                    q = Query.from_(profile).select(functions.Count(
                        profile.CUSTOMER_ID).distinct().as_("c"))
                    swiggy_tdata = db.runQuery(q.where((profile.COMPANY_NAME == "SWIGGY") & (profile.NAME_VERIFIED == '1') & (profile.NUMBER_VERIFIED == '1') &
                                                       (profile.CUSTOMER_ID.isin(q1))))["data"][0]["c"]
                    qq = Query.from_(custcred).select("CUSTOMER_ID").where(
                        custcred.STAGE == 'AWAITING_RE-VERIFICATION')
                    q = Query.from_(profile).select(
                        functions.Count(profile.CUSTOMER_ID).as_("c"))
                    q = q.where(profile.COMPANY_NAME == 'swiggy').where(
                        profile.CUSTOMER_ID.isin(qq))
                    swiggy.update({"awaitingVerification": db.runQuery(q)["data"][0]["c"], "firstPriority": swiggy_fdata, "secondPriority": swiggy_sdata,
                                   "thirdPriority": swiggy_tdata})
                    uberAuto = {}
                    q = Query.from_(profile).select(
                        functions.Count(profile.CUSTOMER_ID).as_("c"))
                    q = q.where(profile.COMPANY_NAME.isin(
                        ['UBER_AUTO', 'uber-auto', 'UBER AUTO']))
                    # db.runQuery(q)["data"][0]["c"]})
                    uberAuto.update({"registeredCustomers": 0})
                    q1 = Query.from_(profile).select("CUSTOMER_ID").where(
                        profile.COMPANY_NAME == 'UBER AUTO')
                    q = Query.from_(lm).select(functions.Count(lm.CUSTOMER_ID).as_("c")).where(
                        (lm.CUSTOMER_ID.isin(q1)) & (lm.STATUS == 'PENDING'))
                    # db.runQuery(q)["data"][0]["c"]})
                    uberAuto.update({"loansPending": 0})
                    q = Query.from_(lm).select(functions.Count(lm.CUSTOMER_ID).as_("c")).where(
                        (lm.CUSTOMER_ID.isin(q1)) & (lm.STATUS == 'REQUESTED'))
                    # db.runQuery(q)["data"][0]["c"]})
                    uberAuto.update({"loansRequested": 0})
                    q5 = Query.from_(leadProfile).select(functions.Count(
                        leadProfile.CUSTOMER_ID).distinct().as_("c")).where((leadProfile.COMPANY=='enkash')&(leadProfile.CUSTOMER_ID!=0))
                    enkash=db.runQuery(q5)["data"]
                    enkash=enkash[0]["c"] if enkash else 0
                    q6=Query.from_(custcred).join(profile, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select("CUSTOMER_ID")
                    q6 = q6.select(functions.Count(
                        profile.CUSTOMER_ID).distinct().as_("c")).where((profile.DIVISION=='kotak')&(profile.SUB_DIVISION=='full')&(custcred.STAGE=='AWAITING_VERIFICATION'))
                    kotak=db.runQuery(q6)["data"]
                    kotak=kotak[0]["c"] if kotak else 0
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
                        output_dict["data"].update({"firstPriority": fdata, "secondPriority": sdata, "thirdPriority": tdata,
                                                    "fourthPriority": frdata, "fifthPriority": ftdata, "sixthPriority": sxdata,
                                                    "uberAuto": uberAuto, "todaysTasks": tTasks, "swiggy": swiggy, "company": company,
                                                    "uberCar": {"firstPriority": fdata, "secondPriority": sdata, "thirdPriority": tdata,
                                                                "fourthPriority": frdata, "fifthPriority": ftdata, "sixthPriority": sxdata},
                                                    "enkash":{"firstPriority":enkash},"kotak":{"awaitingVerification":kotak},
                                                    "pendingTasks": yTasks, "upcomingTasks": uTasks})
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
