from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType, Order


class SearchUserResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"customerInfo": [], "page": {
            "startIndex": 0, "size": 0, "count": 0}}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='searchUser', request=input_dict):
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
                    page = input_dict["data"]['page']
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    custcredm = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    income2 = Table("mw_derived_income_data",
                                    schema="mw_company_3")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    bank = Table("mw_cust_bank_detail", schema="mint_loan")
                    lnlmt = Table("mw_client_loan_limit", schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",
                                 schema="mint_loan")
                    cdata = Table("mw_customer_data", schema="mint_loan")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    derived = Table("mw_customer_derived_data",
                                    schema="mint_loan")
                    clog = Table("mw_customer_change_log", schema="mint_loan")
                    gckyc = Table("pan_status_check", schema="gc_reliance")
                    purch = Table("reliance_purchase", schema="gc_reliance")
                    cprof = Table("mw_profile_info", schema="mw_company_3")
                    #repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                    days_7 = (datetime.now() - timedelta(days=7)
                              ).strftime("%Y-%m-%d %H:%M:%S")
                    days_plus_7 = (datetime.now() + timedelta(days=7)
                                   ).strftime("%Y-%m-%d %H:%M:%S")
                    days_30 = (datetime.now() - timedelta(days=30)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                    days_90 = (datetime.now() - timedelta(days=90)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                    join = Query.from_(custcred).join(lnlmt, how=JoinType.left)
                    join = join.on_field("CUSTOMER_ID").join(kyc, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(profile, how=JoinType.left)
                    join = join.on_field("CUSTOMER_ID")
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.ACCOUNT_STATUS, custcred.LAST_LOGIN, custcred.COMMENTS,
                                    custcred.REGISTERED_IP_ADDRESS, custcred.LAST_LOGGED_IN_IP_ADDRESS, custcred.DEVICE_ID, custcred.CHEQUES,
                                    custcred.CREATED_DATE, custcred.STAGE, kyc.NAME, profile.COMPANY_NAME, lnlmt.LOAN_LIMIT,
                                    profile.name.as_("PROFILE_NAME"),
                                    profile.NAME_VERIFIED, profile.NAME_COMMENT, profile.NUMBER_VERIFIED, profile.NUMBER_COMMENT)
                    q1 = join.select(functions.Count(custcred.CUSTOMER_ID).as_("count"))
                    indict = input_dict['data']
                    if "priority" in list(indict.keys()):
                        if indict["priority"] == "firstPriority":
                            days_7 = (datetime.now() - timedelta(days=7)
                                      ).strftime("%Y-%m-%d %H:%M:%S")
                            q5 = Query.from_(custcred).select("CUSTOMER_ID").where(
                                custcred.STAGE == 'AWAITING_ADDITIONAL_DOCS')
                            q3 = Query.from_(log).select(
                                log.CUSTOMER_ID, functions.Max(log.CREATED_DATE).as_("maxdate"))
                            q3 = q3.where(log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS').groupby(
                                log.CUSTOMER_ID)
                            q2 = Query.from_(log).join(q3).on_field("CUSTOMER_ID").select(
                                log.star).where(log.CREATED_DATE == q3.maxdate)
                            q2 = q2.where((log.CUSTOMER_ID.isin(q5)) & (
                                log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS'))
                            q4 = Query.from_(kycdocs).join(
                                q2, how=JoinType.left).on_field("CUSTOMER_ID")
                            Q1 = q4.select("CUSTOMER_ID").distinct().where((kycdocs.CUSTOMER_ID.isin(q5)) & (kycdocs.CREATED_DATE > days_7) &  # (kycdocs.DOCUMENT_TYPE_ID.notin(["114","115"])) &
                                                                           (kycdocs.VERIFICATION_STATUS.isnull()) &
                                                                           (((q2.CREATED_DATE.isnull()) | (kycdocs.CREATED_DATE > q2.CREATED_DATE))))
                            #Q1 = Query.from_(income).select("CUSTOMER_ID").distinct().where(income.CUSTOMER_ID.isin(q4))
                        elif indict["priority"] == "secondPriority":
                            Q1 = Query.from_(profile).select("CUSTOMER_ID").where(((profile.NAME_VERIFIED == 'P') | (
                                profile.NUMBER_VERIFIED == 'P')) & (profile.NAME.notnull()))
                        elif indict["priority"] == "thirdPriority":
                            stages = ["GOOD_TO_LEND", "READY_TO_LEND", "LOAN_IN_PROCESS", "CUSTOMER", "REJECTED",
                                      "LOW_INCOME", "AWAITING_DOCS", "AWAITING_INFO", "AWAITING_AGREEMENT", "REVIEW_LATER"]
                            q2 = Query.from_(custcred).select("CUSTOMER_ID").where(custcred.STAGE.isin(
                                ["AWAITING_VERIFICATION", "AWAITING_UBER_DATA", "AWAITING_AGREEMENT"]))
                            Q1 = Query.from_(profile).select("CUSTOMER_ID").where((profile.NAME_VERIFIED == '1') & (profile.NUMBER_VERIFIED == '1') &
                                                                                  (profile.CUSTOMER_ID.isin(q2)))
                        elif indict["priority"] == "fourthPriority":
                            Q32 = Query.from_(kyc).select(
                                kyc.CUSTOMER_ID).where(kyc.NAME.notnull())
                            Q42 = Query.from_(cprof).select(cprof.CUSTOMER_ID).where(
                                (cprof.CONFIRMED_CUSTOMER_ID != 0) & (cprof.CONFIRMED_CUSTOMER_ID.notnull()))
                            Q22 = Query.from_(profile).select(profile.CUSTOMER_ID).where(
                                (profile.CUSTOMER_ID.isin(Q32)) | (profile.NAME.notnull()))
                            Q1 = Query.from_(cprof).select("CUSTOMER_ID").distinct().where((cprof.CUSTOMER_ID.isin(Q22)) | (cprof.CUSTOMER_ID.isin(
                                Query.from_(kycdocs).select(kycdocs.CUSTOMER_ID).distinct()))).where((cprof.CUSTOMER_ID.notin(Q42)))
                            Q1 = Q1.where(cprof.CONFIRMED_CUSTOMER_ID.isnull())
                            # Q1 = Query.from_(kycdocs).select("CUSTOMER_ID").where((kycdocs.DOCUMENT_TYPE_ID=='113') &
                            #                                                      (kycdocs.VERIFICATION_STATUS.isnull()))
                        elif indict["priority"] == "fifthPriority":
                            q22 = Query.from_(loanmaster).select(loanmaster.CUSTOMER_ID).where(
                                loanmaster.LOAN_PRODUCT_ID.isin([1, 6, 7, 8, 9, 11, 14]))
                            max_week = db.runQuery(Query.from_(derived).select(
                                functions.Max(derived.WEEK).as_("mweek")))["data"][0]["mweek"]
                            Q1 = Query.from_(derived).select(derived.CUSTOMER_ID).distinct().where((derived.CUSTOMER_ID.notin(q22)) & (derived.WEEK == max_week) & (derived.AVERAGE_3_WEEK >= 18000) & (
                                derived.AVERAGE_10_WEEK >= 18000) & (derived.LAST_WEEK_INCOME > 3000) & (derived.WEEK_FOR_LATEST_DATA > (datetime.now()-timedelta(days=7)).strftime("%Y-%m-%d")))
                        elif indict["priority"] == "sixthPriority":
                            q23 = Query.from_(kycdocs).select(kycdocs.CUSTOMER_ID).distinct().where((kycdocs.DOCUMENT_TYPE_ID.isin(
                                [102, 106, 117, 118])) & ((kycdocs.VERIFICATION_STATUS.isnull()) | (kycdocs.VERIFICATION_STATUS == 'Y')))
                            Q1 = Query.from_(bank).select(bank.CUSTOMER_ID).where(
                                bank.ADDITIONALLY_ADDED == 1).where(bank.DELETE_STATUS_FLAG == 0)
                            Q1 = Q1.where(((bank.DOCUMENT_STATUS.isin(['P', 'R'])) | (bank.DOCUMENT_STATUS.isnull())) & (
                                bank.DEFAULT_STATUS_FLAG == 0) & (bank.CUSTOMER_ID.isin(q23)))
                            #days_7 = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
                            #Q1 = Query.from_(log).select("CUSTOMER_ID").where(log.DATA_VALUE=='LOW_INCOME').where(log.CUSTOMER_ID.isin(q2))
                            #Q1 = Q1.where(((log.RETAINED_DATE<days_7) | (log.RETAINED_DATE.isnull())) & (log.CREATED_DATE<days_7)).groupby(log.CUSTOMER_ID)
                        #elif indict["priority"] == "verificationv2":
                            #print(db.pikastr(q))
                            #q = Query.from_(profile).join(custcred, how=JoinType.left).on_field("CUSTOMER_ID")
                            #q = q.select(functions.Count(profile.CUSTOMER_ID).as_("C"))
                            #q = q.where(profile.COMPANY_NAME==input_dict["data"]["company_name"])
                            #q = q.where(profile.COMPANY_NAME=='UDAAN')#input_dict["data"]["searchText"])
                            #db.pikastr(q)
                            #print(db.pikastr(q))
                            #pass
                        elif indict["priority"] in ("firstPriorityOutcall", "fourthPriorityOutcall"):
                            today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                            cities = {"BAN": ['Bangalore rural', 'bangalore kasavanalli', 'Bangalore', 'banglore', 'BAN', 'bangaloor', 'Bangalore ',
                                              'Bamgalore'], "PUNE": ['PUNE', 'punr'], "CHENNAI": ["Chennai"], "DELHI": ["delhi"],
                                      "HYD": ["HYDERABAD", "hyderbad", "hyd"], "KOL": ["kolkata", "Kolkata"], "KOC": ["KOCHI"], "CHD": ["CHANDIGARH"],
                                      "JAI": ["JAIPUR"], "AHD": ["AHMEDABAD"], "LUC": ["LUCKNOW"]}
                            clback = Query.from_(calldata).select("CUSTOMER_ID").where((calldata.CREATED_DATE > today) |
                                                                                       (calldata.CALLBACK_DATETIME > today))
                            if "city" in input_dict["data"]:
                                q = q.where(profile.CUSTOMER_ID.notin(clback))
                                q1 = q1.where(
                                    profile.CUSTOMER_ID.notin(clback))
                                if input_dict["data"]["city"] in ("BAN", "PUNE", "CHENNAI", "DELHI", "HYD", "KOL"):
                                    #Q = Query.from_(profile).select("CUSTOMER_ID").where(profile.CUSTOMER_ID.notin(clback))
                                    #Q = Q.where(profile.CURRENT_CITY.isin(cities[input_dict["data"]["city"]]))
                                    q = q.where(profile.CURRENT_CITY.isin(
                                        cities[input_dict["data"]["city"]]))
                                    q = q.where(
                                        profile.COMPANY_NAME.notin(["UBER AUTO"]))
                                    q1 = q1.where(profile.CURRENT_CITY.isin(
                                        cities[input_dict["data"]["city"]]))
                                    q1 = q1.where(
                                        profile.COMPANY_NAME.notin(["UBER AUTO"]))
                                elif input_dict["data"]["city"] == "MUM":
                                    #Q = Query.from_(profile).select("CUSTOMER_ID").where(profile.CURRENT_CITY.notin(sum(cities.values(), [])))
                                    #Q = Q.where(profile.CUSTOMER_ID.notin(clback))
                                    q = q.where(profile.CURRENT_CITY.notin(
                                        sum(list(cities.values()), [])))
                                    q1 = q1.where(profile.CURRENT_CITY.notin(
                                        sum(list(cities.values()), [])))
                                # else:
                                    #Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(profile.CUSTOMER_ID.notin(clback))
                            # else:
                                #Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(profile.CUSTOMER_ID.notin(clback))
                            days_2 = (datetime.now() - timedelta(days=2)
                                      ).strftime("%Y-%m-%d 00:00:00")
                            q11 = Query.from_(clog).select("CUSTOMER_ID").distinct().where((clog.DATA_VALUE == "AWAITING_ADDITIONAL_DOCS") &
                                                                                           (clog.CREATED_DATE < (datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d")))
                            Q1 = Query.from_(kycdocs).select("CUSTOMER_ID").distinct().where((kycdocs.CREATED_DATE > days_2) &
                                                                                             # (kycdocs.CUSTOMER_ID.isin(q11)) & #was Q before
                                                                                             (kycdocs.VERIFICATION_STATUS.isnull()))  # |
                            # (kycdocs.CUSTOMER_ID.isin(q11)))
                            # print db.pikastr(Q1)
                        elif indict["priority"] == "secondPriorityOutcall":
                            days_7 = (datetime.now() - timedelta(days=7)
                                      ).strftime("%Y-%m-%d 00:00:00")
                            QQQ = Query.from_(loanmaster).select(
                                "LOAN_ACCOUNT_NO").where(loanmaster.PRODUCT_ID != '1')
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(
                                (emis.DUE_DATE < days_7) & (emis.OVERDUE_AMOUNT > 0) & (emis.LOAN_ACCOUNT_NO.notin(QQQ)))
                        q = (q.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE != "REJECTED")) if indict["priority"] == "secondPriority"
                             else q.where(custcred.CUSTOMER_ID.isin(Q1)) if indict["priority"] == "fourthPriority"
                             else q.where((custcred.CUSTOMER_ID.isin(Q1)) &
                                          (custcred.STAGE.isin(["AWAITING_LOAN_APPLICATION", "AWAITING_RE-VERIFICATION",  # "LOAN_ACTIVE",
                                                                "LOAN_APPROVED", "LOAN_IN_PROCESS"]))) if indict["priority"] == "sixthPriority"
                             else q.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))
                             if indict["priority"] == "firstPriority"
                             else q.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE.isin(["AWAITING_UBER_DATA", "AWAITING_VERIFICATION"])))
                             if indict["priority"] == "thirdPriority"
                             else q.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE == 'LOW_INCOME')) if indict["priority"] == "fifthPriority"
                             else q.where((custcred.CUSTOMER_ID.notin(Q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))
                             if indict["priority"] == "firstPriorityOutcall" else q.where(custcred.STAGE == "AWAITING_LOAN_APPLICATION")
                             if indict["priority"] == "fourthPriorityOutcall" else q)
                        # print indict["priority"]=="firstPriorityOutcall", db.pikastr(q)
                        q1 = (q1.where(custcred.CUSTOMER_ID.isin(Q1) & (custcred.STAGE != "REJECTED")) if indict["priority"] in ["secondPriority", "fourthPriority", "sixthPriority"]
                              else q1.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))
                              if indict["priority"] == "firstPriority"
                              else q1.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE.isin(["AWAITING_UBER_DATA", "AWAITING_VERIFICATION"])))
                              if indict["priority"] == "thirdPriority"
                              else q1.where((custcred.CUSTOMER_ID.isin(Q1)) & (custcred.STAGE == 'LOW_INCOME')) if indict["priority"] == "fifthPriority"
                              else q1.where((custcred.CUSTOMER_ID.notin(Q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))
                              if indict["priority"] == "firstPriorityOutcall" else q1.where(custcred.STAGE == "AWAITING_LOAN_APPLICATION")
                              if indict["priority"] == "fourthPriorityOutcall" else q1)
                    if indict["accountStatus"] != "":
                        q = q.where(custcred.ACCOUNT_STATUS ==
                                    indict["accountStatus"])
                        q1 = q1.where(custcred.ACCOUNT_STATUS ==
                                      indict["accountStatus"])
                    if (indict["companyName"] != [] if "companyName" in list(indict.keys()) else False):
                        q = q.where(profile.COMPANY_NAME.isin(
                            indict["companyName"]))
                        q1 = q1.where(profile.COMPANY_NAME.isin(
                            indict["companyName"]))
                    if (indict["bankAccount"] != "" if "bankAccount" in list(indict.keys()) else False):
                        Q = Query.from_(bank).select(bank.CUSTOMER_ID).where(
                            bank.ACCOUNT_NO == indict["bankAccount"])
                        q = q.where(custcred.CUSTOMER_ID.isin(Q))
                        q1 = q1.where(custcred.CUSTOMER_ID.isin(Q))
                    if (indict["uberAgreement"] if "uberAgreement" in list(indict.keys()) else False):
                        Q = Query.from_(kycdocs).select("CUSTOMER_ID").distinct().where(
                            kycdocs.DOCUMENT_TYPE_ID == "113")
                        q = q.where(custcred.CUSTOMER_ID.isin(Q))
                    if indict["searchBy"] in ("invCallToday", "invCallPending", "invCallLastWeek", "invCallUpcoming"):
                        Q = Query.from_(calldata).select("CUSTOMER_ID")
                        Q = Q.where((calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19])) &
                                    (calldata.CALLBACK_DATETIME > (today if indict["searchBy"] == "invCallToday" else days_30
                                                                   if indict["searchBy"] == "invCallPending" else days_7 if indict["searchBy"] == "invCallLastWeek" else days_plus_7)))
                        q = q.where(custcred.CUSTOMER_ID.isin(Q))
                        q1 = q1.where(custcred.CUSTOMER_ID.isin(Q))
                    if indict["searchBy"] in ("successTransactions", "unsuccessTransactions"):
                        Q = Query.from_(purch).join(
                            pan, how=JoinType.left).on(purch.PAN == pan.PAN_NO)
                        Q = Q.select(pan.CUSTOMER_ID).distinct()
                        if indict["searchBy"] == "successTransactions":
                            Q = Q.where(purch.TRANSACTION_STATUS == 'Success')
                        else:
                            Q = Q.where(purch.TRANSACTION_STATUS != 'Success')
                        if indict["searchText"] == "today":
                            Q = Q.where((purch.CREATED_AT > today)
                                        & (pan.CUSTOMER_ID.notnull()))
                        elif indict["searchText"] == "lastWeek":
                            Q = Q.where((purch.CREATED_AT > days_7)
                                        & (pan.CUSTOMER_ID.notnull()))
                        elif indict["searchText"] == "lastMonth":
                            Q = Q.where((purch.CREATED_AT > days_30)
                                        & (pan.CUSTOMER_ID.notnull()))
                        elif indict["searchText"] == "last3Month":
                            Q = Q.where((purch.CREATED_AT > days_90)
                                        & (pan.CUSTOMER_ID.notnull()))
                        q = q.where(custcred.CUSTOMER_ID.isin(Q))
                        q1 = q1.where(custcred.CUSTOMER_ID.isin(Q))
                    if indict["searchBy"] in ("kycDoneNoInvestment", "kycDone"):
                        Q = Query.from_(gckyc).join(
                            pan, how=JoinType.left).on(gckyc.PAN == pan.PAN_NO)
                        Q = Q.select(pan.CUSTOMER_ID).distinct().where(
                            gckyc.KYC_FLAG == 'Y')
                        inv = Query.from_(purch).select(purch.PAN).distinct().where(
                            purch.TRANSACTION_STATUS == 'Success')
                        if indict["searchBy"] == "kycDoneNoInvestment":
                            Q = Q.where(gckyc.PAN.notin(inv))
                        if indict["searchText"] == "today":
                            Q = Q.where((gckyc.CREATED_AT > today)
                                        & (pan.CUSTOMER_ID.notnull()))
                        elif indict["searchText"] == "lastWeek":
                            Q = Q.where((gckyc.CREATED_AT > days_7)
                                        & (pan.CUSTOMER_ID.notnull()))
                        elif indict["searchText"] == "lastMonth":
                            Q = Q.where((gckyc.CREATED_AT > days_30)
                                        & (pan.CUSTOMER_ID.notnull()))
                        elif indict["searchText"] == "last3Month":
                            Q = Q.where((gckyc.CREATED_AT > days_90)
                                        & (pan.CUSTOMER_ID.notnull()))
                        q = q.where(custcred.CUSTOMER_ID.isin(Q))
                        q1 = q1.where(custcred.CUSTOMER_ID.isin(Q))
                    if indict["searchBy"] == "listCustomers":
                        q = q.where(custcred.STAGE != 'REJECTED')
                        q1 = q1.where(custcred.STAGE != 'REJECTED')
                        if indict["days"] > 0:
                            q = q.where(custcred.CREATED_DATE >= (
                                datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                            q1 = q1.where(custcred.CREATED_DATE >= (
                                datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    elif indict["searchBy"] in ["name", "loginID", "email", "pan", "aadhar", "customerID", "stage", "company", "notCompany", "companyStage", "fund","companyLoanStage"]:
                        if indict["searchBy"] == "stage":
                            # indict["searchText"]=="AWAITING_RE-VERIFICATION": #temporary addition of the if block. remove afterwards
                            if False:
                                Q111 = Query.from_(income).select(income.CUSTOMER_ID).distinct().where(income.WEEK == '2019-10-07').union(
                                    Query.from_(income2).select(income2.CUSTOMER_ID).distinct().where(income2.WEEK == '2019-10-07'))
                                Q111 = Query.from_(Q111).select(
                                    "CUSTOMER_ID").distinct()
                                q = q.where(custcred.CUSTOMER_ID.isin(Q111)).where(
                                    custcred.STAGE == indict["searchText"])  # temp addition of 1st where
                                q1 = q1.where(custcred.CUSTOMER_ID.isin(Q111)).where(
                                    custcred.STAGE == indict["searchText"])  # temp addition of 1st where
                            else:
                                # temp addition of 1st where
                                q = q.where(custcred.STAGE ==
                                            indict["searchText"])
                                #print("true")
                                #print(db.pikastr(q))
                                # temp addition of 1st where
                                q1 = q1.where(custcred.STAGE ==
                                              indict["searchText"])
                                #print(db.pikastr(q1))
                        if indict["searchBy"] == "pan":
                            QQ = Query.from_(pan).select(pan.CUSTOMER_ID).where(
                                pan.PAN_NO == indict["searchText"])
                            q = q.where(custcred.CUSTOMER_ID.isin(QQ))
                            q1 = q1.where(custcred.CUSTOMER_ID.isin(QQ))
                        if indict["searchBy"] == "fund":
                            QQ = Query.from_(loanmaster).select(loanmaster.CUSTOMER_ID).where(
                                loanmaster.FUND == indict["searchText"])
                            q = q.where(custcred.CUSTOMER_ID.isin(QQ))
                            q1 = q1.where(custcred.CUSTOMER_ID.isin(QQ))
                        if indict["searchBy"] == "companyStage":
                            q = q.where((profile.COMPANY_NAME == indict["searchText"].split(
                                "-")[0]) & (custcred.STAGE == indict["searchText"].split("-")[1]))
                            #print(db.pikastr(q))
                            q1 = q1.where((profile.COMPANY_NAME == indict["searchText"].split(
                                "-")[0]) & (custcred.STAGE == indict["searchText"].split("-")[1]))
                        if indict["searchBy"] == "companyLoanStage":
                            q = q.where((profile.COMPANY_NAME == indict["searchText"].split(
                                "-")[0]) & (custcred.STAGE == indict["searchText"].split("-")[1]))
                            if 'city' in indict:
                                q=q.where(profile.CURRENT_CITY ==indict['city'])
                            q1 = q1.where((profile.COMPANY_NAME == indict["searchText"].split(
                                "-")[0]) & (custcred.STAGE == indict["searchText"].split("-")[1]))
                            if 'city' in indict:
                                q1=q1.where(profile.CURRENT_CITY ==indict['city'])
                        if indict["searchBy"] == "company":
                            q = q.where(profile.COMPANY_NAME ==
                                        indict["searchText"])
                            q1 = q1.where(profile.COMPANY_NAME ==
                                          indict["searchText"])
                        if indict["searchBy"] == "notCompany":
                            try:
                                q = q.where(profile.COMPANY_NAME.notin(
                                    json.loads(indict["searchText"])))
                                q1 = q1.where(profile.COMPANY_NAME.notin(
                                    json.loads(indict["searchText"])))
                            except:
                                pass
                        if indict["searchBy"] == 'loginID':
                            #qq = Query.from_(cdata).select(cdata.CUSTOMER_ID).distinct().where((cdata.DATA_KEY=="COMPANY_NUMBER") & (cdata.DATA_VALUE.like("%" + indict["searchText"] + "%")))
                            lll = Query.from_(custcredm).select("CUSTOMER_ID").where(
                                custcredm.LOGIN_ID.like("%" + indict["searchText"] + "%"))
                            q = q.where((custcred.LOGIN_ID.like("%" + indict["searchText"] + "%")).__or__(profile.COMPANY_NUMBER.like(
                                "%" + indict["searchText"] + "%")).__or__(custcred.CUSTOMER_ID.isin(lll)))  # .__or__(custcred.CUSTOMER_ID.isin(qq)))
                            q1 = q1.where((custcred.LOGIN_ID.like("%" + indict["searchText"] + "%")).__or__(profile.COMPANY_NUMBER.like(
                                "%" + indict["searchText"] + "%")).__or__(custcred.CUSTOMER_ID.isin(lll)))  # .__or__(custcred.CUSTOMER_ID.isin(qq)))
                        if indict["searchBy"] == 'customerID':
                            # customerID"])
                            q = q.where(custcred.CUSTOMER_ID ==
                                        indict["searchText"])
                            # customerID"])
                            q1 = q1.where(custcred.CUSTOMER_ID ==
                                          indict["searchText"])
                        if indict["searchBy"] == 'name':
                            q = q.where((kyc.NAME.like("%" + indict["searchText"] + "%")).__or__(
                                profile.NAME.like("%" + indict["searchText"] + "%")))
                    elif indict["searchBy"] == "date":
                        q = q.where(custcred.CREATED_DATE >=
                                    indict["fromDate"])
                        q = q.where(custcred.CREATED_DATE <= indict["toDate"])
                        q1 = q1.where(custcred.CREATED_DATE >=
                                      indict["fromDate"])
                        q1 = q1.where(custcred.CREATED_DATE <=
                                      indict["toDate"])
                    elif indict["searchBy"] == "listClients":
                        q = q.where(clientmaster.CUSTOMER_ID.notnull())
                        q1 = q1.where(clientmaster.CUSTOMER_ID.notnull())
                    elif indict["searchBy"] in ("listLoanApplied", "listLoanPending", "listLoanRequested"):
                        q = Query.from_(loanmaster).join(
                            custcred).on_field("CUSTOMER_ID")
                        join = q.join(lnlmt, how=JoinType.left).on_field(
                            "CUSTOMER_ID").join(kyc, how=JoinType.left)
                        join = join.on_field("CUSTOMER_ID").join(
                            profile, how=JoinType.left).on_field("CUSTOMER_ID")
                        status = ('ACTIVE' if indict["searchBy"] == "listLoanApplied" else "PENDING" if indict["searchBy"] == "listLoanPending"
                                  else "REQUESTED")
                        q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.ACCOUNT_STATUS, custcred.LAST_LOGIN,
                                        custcred.REGISTERED_IP_ADDRESS, custcred.LAST_LOGGED_IN_IP_ADDRESS, custcred.DEVICE_ID,
                                        loanmaster.CREATED_DATE, custcred.STAGE, lnlmt.LOAN_LIMIT,
                                        profile.name.as_("PROFILE_NAME"))
                        if indict["searchText"] != "":
                            q = q.where(profile.COMPANY_NAME ==
                                        indict["searchText"])
                        q1 = join.select(functions.Count(custcred.CUSTOMER_ID).as_(
                            "count").distinct()).where(loanmaster.STATUS == status)
                        if indict["searchText"] != "":
                            q1 = q1.where(profile.COMPANY_NAME ==
                                          indict["searchText"])
                        if indict["accountStatus"] != "":
                            q = q.where(custcred.ACCOUNT_STATUS ==
                                        indict["accountStatus"])
                            q1 = q1.where(custcred.ACCOUNT_STATUS ==
                                          indict["accountStatus"])
                    if (indict["numberVerified"] in ("1", "0", "P") if "numberVerified" in indict else False):
                        q = q.where(profile.NUMBER_VERIFIED ==
                                    indict["numberVerified"])
                        q1 = q1.where(profile.NUMBER_VERIFIED ==
                                      indict["numberVerified"])
                    if (indict["numberVerified"] == "NULL" if "numberVerified" in indict else False):
                        q = q.where(profile.NUMBER_VERIFIED.isnull())
                        q1 = q1.where(profile.NUMBER_VERIFIED.isnull())
                    if (indict["nameVerified"] in ("1", "0", "P") if "nameVerified" in indict else False):
                        q = q.where(profile.NAME_VERIFIED ==
                                    indict["nameVerified"])
                        q1 = q1.where(profile.NAME_VERIFIED ==
                                      indict["nameVerified"])
                    if (indict["nameVerified"] == "NULL" if "nameVerified" in indict else False):
                        q = q.where(profile.NAME_VERIFIED.isnull())
                        q1 = q1.where(profile.NAME_VERIFIED.isnull())
                    if indict["searchBy"] not in ["listLoanApplied", "listClients", "listLoanPending", "listLoanRequested"]:
                        if "orderby" in list(indict.keys()) and "order" in list(indict.keys()):
                            orderPref = Order.asc if indict["order"] == "asc" else Order.desc
                            if indict["orderby"] == "stage":
                                q = q.orderby(custcred.STAGE, order=orderPref).limit(
                                    "%i,%i" % (page["startIndex"], page["size"]))
                            elif indict["orderby"] == "customerID":
                                q = q.orderby(custcred.CUSTOMER_ID, order=orderPref).limit(
                                    "%i,%i" % (page["startIndex"], page["size"]))
                            elif indict["orderby"] == "name":
                                q = q.orderby(kyc.NAME, order=orderPref).limit(
                                    "%i,%i" % (page["startIndex"], page["size"]))
                            elif indict["orderby"] == "loginID":
                                q = q.orderby(custcred.LOGIN_ID, order=orderPref).limit(
                                    "%i,%i" % (page["startIndex"], page["size"]))
                            elif indict["orderby"] == "clientID":
                                q = q.orderby(clientmaster.CLIENT_ID, order=orderPref).limit(
                                    "%i,%i" % (page["startIndex"], page["size"]))
                            elif indict["orderby"] == "createdDate":
                                q = q.orderby(custcred.CREATED_DATE, order=orderPref).limit(
                                    "%i,%i" % (page["startIndex"], page["size"]))
                            Fields = db.runQuery(q)
                            #print(db.pikastr(q))
                        else:
                            Fields = db.runQuery(q.orderby(custcred.CUSTOMER_ID, order=Order.desc).limit(
                                "%i,%i" % (page["startIndex"], page["size"])))
                            # print db.pikastr(q.orderby(custcred.CUSTOMER_ID, order=Order.desc))
                    else:
                        Fields = (q.orderby(loanmaster.CREATED_DATE, order=Order.desc) if indict["searchBy"] in ('listLoanApplied', 'listLoanPending', 'listLoanRequested')
                                  else q.orderby(custcred.CUSTOMER_ID, order=Order.desc))
                        Fields = db.runQuery(Fields.groupby(custcred.CUSTOMER_ID).limit(
                            "%i,%i" % (page["startIndex"], page["size"])))
                        # print db.pikastr(q.orderby(custcred.CUSTOMER_ID,order=Order.desc).groupby(custcred.CUSTOMER_ID).limit("%i,%i"%(page["startIndex"], page["size"])))
                    if (("EXTNBFC" in accTypes) if "accTypes" in locals() else False):
                        cid = db.runQuery(Query.from_(loanmaster).select(
                            loanmaster.CUSTOMER_ID).distinct().where(loanmaster.FUND == 'POONAWALLA'))["data"]
                        cid = [c["CUSTOMER_ID"] for c in cid]
                        Fields["data"] = [datum for datum in Fields["data"]
                                          if datum["CUSTOMER_ID"] in cid]
                    for datum in Fields["data"]:
                        if indict["searchBy"] in ("successTransactions", "unsuccessTransactions", "kycDoneNoInvetment", "kycDone"):
                            mfCall = Query.from_(calldata).select(
                                "COMMENTS", "CREATED_DATE")
                            mfCall = db.runQuery(mfCall.where((calldata.CUSTOMER_ID == datum["CUSTOMER_ID"]) &
                                                              (calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19]))))["data"]
                            invInfo = Query.from_(gckyc).join(
                                pan, how=JoinType.left).on(gckyc.PAN == pan.PAN_NO)
                            invInfo = invInfo.select(
                                gckyc.STATUS_CHECK_TIME, gckyc.KYC_FLAG, gckyc.KYC_STATUS, pan.PAN_NO)
                            invInfo = db.runQuery(invInfo.where(
                                pan.CUSTOMER_ID == datum["CUSTOMER_ID"]))["data"]
                            if invInfo:
                                invInfo = invInfo[0]
                                tr = db.runQuery(Query.from_(purch).select(functions.Count(purch.star).as_('C')).where((purch.TRANSACTION_STATUS == 'Success') &
                                                                                                                       (purch.PAN == invInfo["PAN_NO"])))
                                invInfo.update(
                                    {"transactions": tr["data"][0]["C"] if tr["data"] else 0})
                        else:
                            mfCall, invInfo = [], {}
                        # client = Query.from_(clientmaster).select(clientmaster.CLIENT_ID, clientmaster.FULL_NAME.as_("CLIENT_FULL_NAME"),
                        #                                          clientmaster.ACTIVATION_DATE)
                        #client = client.where(clientmaster.CUSOTMER_ID==datum["CUSTOMER_ID"]).orderby(clientmaster.AUTO_ID, order=Order.desc)
                        #client = db.runQuery(client.limit(1))["data"]
                        loginID = db.runQuery(Query.from_(custcredm).select("LOGIN_ID").where(
                            (custcredm.CUSTOMER_ID == datum["CUSTOMER_ID"]) & (custcredm.ACTIVE == '1')))["data"]
                        datum["LOGIN_ID"] = loginID[0]["LOGIN_ID"] if loginID else datum["LOGIN_ID"]
                        loanStatus = Query.from_(loanmaster).select("STATUS", "AMOUNT", "LOAN_REFERENCE_ID", "FUND", "LENDER").where(
                            loanmaster.CUSTOMER_ID == datum["CUSTOMER_ID"])
                        loanStatus = db.runQuery(loanStatus.orderby(
                            loanmaster.CREATED_DATE, order=Order.desc).limit(1))["data"]
                        # d = db.runQuery(Query.from_(repay).select(repay.star).where((repay.LOAN_REF_ID==loanStatus["LOAN_REFERENCE_ID"]) &
                        #                                                            (repay.FINFLUX_TRAN_ID.isnull())))["data"]
                        #dates = [(ele["REPAY_DATETIME"], ele["REPAY_AMOUNT"]) for ele in d]
                        #temp_7 = sum(ele[1] for ele in dates if ele[0] > int((datetime.now() - timedelta(days=7)).strftime("%s")))
                        # Query.from_(kycdocs).select("CREATED_DATE").where(kycdocs.CUSTOMER_ID==datum["CUSTOMER_ID"])
                        docUpload = False
                        #docUpload = db.runQuery(docUpload.orderby(kycdocs.CREATED_DATE, order=Order.desc).limit(1))["data"]
                        #mfCall = db.runQuery(Query.from_(calldata).select("COMMENTS", "CREATED_DATE").where((calldata.CUSTOMER_ID==datum["CUSTOMER_ID"]) & (calldata.INTERACTION_REASON_ID.isin(["17", "18", "19", 17, 18, 19]))))["data"]
                        #custPan = Query.from_(pan).select("PAN_NO").where(pan.CUSTOMER_ID==datum["CUSTOMER_ID"])
                        #panNo = db.runQuery(custPan.orderby(pan.CREATED_DATE, order=Order.desc).limit(1))["data"]
                        datum.update({"PAN_NO": invInfo["PAN_NO"] if invInfo else "", "LOAN_STATUS": loanStatus[0]["STATUS"] if loanStatus else "", "mfInteractions": mfCall, "invInfo": invInfo,
                                      # "TEMPORARY_PAYMENT":temp_7,
                                      "LOAN_AMOUNT": loanStatus[0]["AMOUNT"] if loanStatus else 0, "LENDER": loanStatus[0]["LENDER"] if loanStatus else '',
                                      "DOC_UPLOAD": docUpload[0]["CREATED_DATE"] if docUpload else "", "FUND": loanStatus[0]["FUND"] if loanStatus else '',
                                      "LOAN_LIMIT": datum["LOAN_LIMIT"] if (datum["LOAN_LIMIT"] not in ('', None) if "LOAN_LIMIT" in datum else False) else 0})
                        #datum.update(client[0] if client else {})
                    if indict["orderby"] == "loanStatus" if "orderby" in list(indict.keys()) else False:
                        Fields["data"] = sorted(
                            Fields["data"], key=lambda x: x["LOAN_STATUS"], reverse=False if indict["order"] == "asc" else True)
                    temp = db.runQuery(q1)["data"]
                    #print(temp)
                    Fields.update(temp[0] if temp else {})
                    #print(Fields)
                    if (not Fields["error"]):
                        # if input_dict["msgHeader"]["authLoginID"]!="dharam@mintwalk.com" else {"updated":True, "token":"scGEx8.gYYwXlGxTGiIMZO2OJ7qdIcuZy0vUA4sPsFc.!rxNMy4BkRT/P6pxWvjP2G6iDwIFukf+o"}
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            if Fields["data"] != []:
                                output_dict["data"].update(
                                    {"customerInfo": utils.camelCase(Fields["data"])})
                                output_dict["data"]["page"] = input_dict["data"]["page"]
                                output_dict["data"]["page"].update(
                                    {"count": Fields["count"]})
                                output_dict["data"].update(
                                    {"error": 0, "message": success})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["msgHeader"]["authToken"] = token["token"]
                                output_dict["data"].update(
                                    {"error": 0, "message": "Results not found"})
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)
                # print output_dict["msgHeader"]
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise


app = falcon.API()
searchUser = SearchUserResource()

app.add_route('/searchUser', searchUser)
