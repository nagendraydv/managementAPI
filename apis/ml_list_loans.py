from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta, math
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType, Order


class ListLoansResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"loanInfo": [], "logins": [], "custData": {}, "page": {"startIndex": 0, "size": 0, "count": 0}},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            # print raw_json
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='listLoans', request=input_dict):
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
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    bank = Table("mw_cust_bank_detail", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    users = Table("mw_admin_user_master",
                                  schema="mint_loan_admin")
                    mandate = Table(
                        "mw_physical_mandate_status", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    docs = Table("mw_cust_kyc_documents", schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",
                                 schema="mint_loan")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    mapp = {'LOAN_LIMIT': 'PERSONAL', 'MOBILE_LOAN_LIMIT': 'MOBILE',
                            'TYRE_LOAN_LIMIT': 'TYRE', 'EDUCATION_LOAN_LIMIT': 'EDUCATION'}
                    join = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    join = join.join(loanprod, how=JoinType.left).on(
                        loanprod.PRODUCT_ID == loanmaster.LOAN_PRODUCT_ID)
                    # .join(clientmaster, how=JoinType.left)
                    join = join.join(custcred).on(
                        loanmaster.CUSTOMER_ID == custcred.CUSTOMER_ID)
                    join = join.join(kyc, how=JoinType.left).on_field(
                        "CUSTOMER_ID")
                    #join = join.join(profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.STAGE, kyc.NAME, loanprod.LIMIT_TYPE,
                                    loanmaster.star, loandetails.star)  # .where(loanmaster.STATUS!='ML_REJECTED')
                    q1 = join.select(functions.Count(
                        custcred.CUSTOMER_ID).as_("count"))
                    indict = input_dict['data']
                    if (indict["custID"] != "" if "custID" in indict else False):
                        q = q.where(loanmaster.CUSTOMER_ID == indict["custID"])
                        q1 = q1.where(loanmaster.CUSTOMER_ID ==
                                      indict["custID"])
                    if (indict["weekly"] == "1" if "weekly" in indict else False):
                        pass  # q = q.where(loanmaster.LOAN_PRODUCT_ID!="43")
                        #q1 = q1.where(loanmaster.LOAN_PRODUCT_ID!="43")
                    if indict["loanStatus"] != []:
                        #print(indict["loanStatus"])
                        #q = q.where(loanmaster.STATUS==indict["loanStatus"])
                        q = q.where(loanmaster.STATUS.isin([indict["loanStatus"]]))
                    if "priority" in indict:
                        today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                        cities = {"BAN": ['Bangalore rural', 'bangalore kasavanalli', 'Bangalore', 'banglore', 'BAN', 'bangaloor', 'Bangalore ',
                                          'Bamgalore'], "PUNE": ['PUNE', 'punr'], "CHENNAI": ["Chennai"], "DEL": ["delhi"],
                                  "HYD": ["HYDERABAD", "hyderbad", "hyd"], "KOL": ["kolkata", "Kolkata"], "KOC": ["KOCHI"], "CHD": ["CHANDIGARH"],
                                  "JAI": ["JAIPUR"], "AHD": ["AHMEDABAD"], "LUC": ["LUCKNOW"]}
                        clback = Query.from_(calldata).select("CUSTOMER_ID").where(
                            (calldata.CREATED_DATE > today) | (calldata.CALLBACK_DATETIME > today))
                        if "city" in input_dict["data"]:
                            if input_dict["data"]["city"] in ("BAN", "PUNE", "CHENNAI", "DEL", "HYD", "KOL", "KOC", "CHD", "JAI", "AHD", "LUC"):
                                Q = Query.from_(profile).select("CUSTOMER_ID").where(profile.CURRENT_CITY.isin(
                                    cities[input_dict["data"]["city"]]))  # .where(profile.COMPANY_NAME.notin(["UBER AUTO"]))
                                Qb = (Q.where(profile.COMPANY_NAME == input_dict["data"]["company"])) if "company" in input_dict["data"] else (
                                    Q.where(profile.COMPANY_NAME.notin(["UBER AUTO"])))
                                Q = Qb.where(profile.CUSTOMER_ID.notin(clback))
                            elif input_dict["data"]["city"] == "MUM":
                                Q = Query.from_(profile).select("CUSTOMER_ID").where(
                                    profile.CURRENT_CITY.notin(sum(list(cities.values()), [])))
                                Qb = (Q.where(profile.COMPANY_NAME == input_dict["data"]["company"])) if "company" in input_dict["data"] else (
                                    Q.where(profile.COMPANY_NAME.notin(["UBER AUTO"])))
                                Q = Qb.where(profile.CUSTOMER_ID.notin(clback))
                            else:
                                Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                                    profile.CUSTOMER_ID.notin(clback)).where(profile.COMPANY_NAME.notin(["UBER AUTO"]))
                        else:
                            Q = Query.from_(profile).select("CUSTOMER_ID").distinct().where(
                                profile.CUSTOMER_ID.notin(clback)).where(profile.COMPANY_NAME.notin(["UBER AUTO"]))
                        if indict["priority"] == "firstPriorityOutcall":
                            days_2 = (datetime.now() - timedelta(days=2)
                                      ).strftime("%Y-%m-%d 00:00:00")
                            Q1 = Query.from_(kycdocs).select("CUSTOMER_ID").distinct().where((kycdocs.CREATED_DATE > days_2) &
                                                                                             (kycdocs.CUSTOMER_ID.notin(Q)) &
                                                                                             (kycdocs.VERIFICATION_STATUS.isnull()))
                            q = q.where((loanmaster.STATUS != 'REPAID') & (custcred.CUSTOMER_ID.notin(
                                Q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))
                        elif indict["priority"] == "secondPriorityOutcall":
                            days_31 = (datetime.now() - timedelta(days=15)
                                       ).strftime("%Y-%m-%d 00:00:00")
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS.isin(['ACTIVE', 'WRITTEN-OFF']))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(
                                (emis.DUE_DATE < days_31) & (emis.OVERDUE_AMOUNT > 0) & (emis.LOAN_ACCOUNT_NO.isin(QQ1)))
                            q = q.where((loanmaster.STATUS.notin(['REPAID', 'WRITTEN__OFF'])) & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "thirdPriorityOutcall":
                            days_31 = (datetime.now() - timedelta(days=15)
                                       ).strftime("%Y-%m-%d 00:00:00")
                            days_7 = (datetime.now() - timedelta(days=7)
                                      ).strftime("%Y-%m-%d 00:00:00")
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS.isin(['ACTIVE', 'WRITTEN-OFF']))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where((emis.DUE_DATE < days_7) & (emis.DUE_DATE > days_31) &
                                                                                             (emis.OVERDUE_AMOUNT > 0) & (emis.LOAN_ACCOUNT_NO.isin(QQ1)))
                            q = q.where((loanmaster.STATUS != 'REPAID') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "fourthPriorityOutcall":
                            q = q.where((custcred.STAGE == "AWAITING_LOAN_APPLICATION") & (
                                custcred.CUSTOMER_ID.notin(Q)))
                        elif indict["priority"] == "action":
                            Q1 = Query.from_(calldata).select(calldata.CUSTOMER_ID).distinct().where(
                                calldata.INTERACTION_RESOLUTION_ID == '91')
                            q = q.where((custcred.CUSTOMER_ID.isin(Q1)) & (
                                custcred.CUSTOMER_ID.isin(Qb)))
                        elif indict["priority"] == "dueNowOutcall":
                            days_7 = (datetime.now() - timedelta(days=7)
                                      ).strftime("%Y-%m-%d 00:00:00")
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(
                                (emis.DUE_DATE > days_7) & (emis.OVERDUE_AMOUNT > 500) & (emis.LOAN_ACCOUNT_NO.isin(QQ1)))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "due1MonthOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            days_7 = (datetime.now() - timedelta(days=7)
                                      ).strftime("%Y-%m-%d 00:00:00")
                            days_180 = (
                                datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d 00:00:00")
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where((emis.DUE_DATE < days_7) & (emis.DUE_DATE > days_180) &
                                                                                             (emis.OVERDUE_AMOUNT > 500) & (emis.LOAN_ACCOUNT_NO.isin(QQ1)))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "due3MonthOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            days_180 = (
                                datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d 00:00:00")
                            days_31 = (datetime.now() - timedelta(days=31)
                                       ).strftime("%Y-%m-%d 00:00:00")
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where((emis.DUE_DATE < days_180) &
                                                                                             (emis.OVERDUE_AMOUNT > 500) & (emis.LOAN_ACCOUNT_NO.isin(QQ1)))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "largeLoanOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where((emis.OVERDUE_AMOUNT > 500) &
                                                                                             (emis.LOAN_ACCOUNT_NO.isin(QQ1.where(loanmaster.LOAN_PRODUCT_ID.isin([2, 12, 16])))))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "mediumLoanOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where((emis.OVERDUE_AMOUNT > 500) &
                                                                                             (emis.LOAN_ACCOUNT_NO.isin(QQ1.where(loanmaster.LOAN_PRODUCT_ID.isin([43, 44, 45, 52, 13])))))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "smallLoanOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where((emis.OVERDUE_AMOUNT > 500) &
                                                                                             (emis.LOAN_ACCOUNT_NO.isin(QQ1.where(loanmaster.LOAN_PRODUCT_ID.notin([2, 12, 16, 43, 44, 45, 52, 13])))))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "smallOverdueOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(emis.LOAN_ACCOUNT_NO.isin(QQ1)).groupby(
                                emis.CUSTOMER_ID).having((functions.Sum(emis.OVERDUE_AMOUNT) < 1500) & (functions.Sum(emis.OVERDUE_AMOUNT) > 100))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "mediumOverdueOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(emis.LOAN_ACCOUNT_NO.isin(QQ1)).groupby(
                                emis.CUSTOMER_ID).having((functions.Sum(emis.OVERDUE_AMOUNT) < 5000) & (functions.Sum(emis.OVERDUE_AMOUNT) > 1500))
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "largeOverdueOutcall":
                            Qtemp = Query.from_(repay).select("CUSTOMER_ID").distinct().where((repay.REPAY_DATETIME > (
                                datetime.now()-timedelta(days=4)).date().strftime("%s")) & (repay.FINFLUX_TRAN_ID.isnull()))
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS == 'ACTIVE').where(loanmaster.CUSTOMER_ID.notin(Qtemp))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(emis.LOAN_ACCOUNT_NO.isin(
                                QQ1)).groupby(emis.CUSTOMER_ID).having(functions.Sum(emis.OVERDUE_AMOUNT) > 5000)
                            q = q.where((loanmaster.STATUS == 'ACTIVE') & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                        elif indict["priority"] == "writtenOffOutcall":
                            QQ1 = Query.from_(loanmaster).select("LOAN_ACCOUNT_NO").where(
                                loanmaster.STATUS.isin(['WRITTEN_OFF', 'WRITTEN-OFF', 'WRITTEN__OFF']))
                            Q1 = Query.from_(emis).select(emis.CUSTOMER_ID).distinct().where(
                                (emis.OVERDUE_AMOUNT > 0) & (emis.LOAN_ACCOUNT_NO.isin(QQ1)))
                            q = q.where((loanmaster.STATUS.isin(['WRITTEN_OFF', 'WRITTEN-OFF', 'WRITTEN__OFF'])) & (
                                custcred.CUSTOMER_ID.isin(Q1)) & (custcred.CUSTOMER_ID.isin(Q)))
                    elif indict["days"] > 0:
                        q = q.where(loanmaster.CREATED_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                        q1 = q1.where(loanmaster.CREATED_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    elif (indict["fund"] != '' if ("fund" in indict) else False):
                        q = q.where(loanmaster.fund == indict["fund"])
                        q1 = q1.where(loanmaster.fund == indict["fund"])
                    elif (indict["dueThisWeek"] == 1 if "dueThisWeek" in list(indict.keys()) else False):
                        date1_prev_month = (
                            datetime.today() + relativedelta(months=-1)).strftime("%Y-%m-%d")
                        date2_prev_month = (datetime.today(
                        ) + relativedelta(months=-1) + relativedelta(days=7)).strftime("%Y-%m-%d")
                        q = q.where((loanmaster.LOAN_DISBURSED_DATE >= date1_prev_month) & (loanmaster.LOAN_DISBURSED_DATE <= date2_prev_month)
                                    & (loanmaster.STATUS == 'ACTIVE')).orderby(loanmaster.LOAN_DISBURSED_DATE)
                        q1 = q1.where((loanmaster.LOAN_DISBURSED_DATE >= date1_prev_month) & (loanmaster.LOAN_DISBURSED_DATE <= date2_prev_month)
                                      & (loanmaster.STATUS == 'ACTIVE'))
                    elif (indict["dueAll"] == 1 if "dueAll" in list(indict.keys()) else False):
                        date2_prev_month = (datetime.today(
                        ) + relativedelta(months=-1) + relativedelta(days=-1)).strftime("%Y-%m-%d")
                        q = q.where((loanmaster.LOAN_DISBURSED_DATE <= date2_prev_month) & (
                            loanmaster.STATUS == "ACTIVE"))
                        q1 = q1.where((loanmaster.LOAN_DISBURSED_DATE <= date2_prev_month) & (
                            loanmaster.STATUS == "ACTIVE"))
                    elif (indict["mandate"] == 1 if "mandate" in list(indict.keys()) else False):
                        join = join.join(mandate, how=JoinType.left).on_field(
                            "CUSTOMER_ID")
                        q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.STAGE, kyc.NAME,  # clientmaster.CLIENT_ID, kyc.NAME,
                                        #clientmaster.FULL_NAME.as_("CLIENT_FULL_NAME"), clientmaster.ACTIVATION_DATE, profile.name.as_("PROFILE_NAME"),
                                        loanmaster.star, loandetails.star).where(loanmaster.STATUS != 'ML_REJECTED')
                        q = q.where(mandate.MANDATE_STATUS == 'Active').where(
                            loanmaster.STATUS == 'ACTIVE')
                        q1 = join.select(functions.Count(custcred.CUSTOMER_ID).as_("count")).where(
                            mandate.MANDATE_STATUS == 'Active').where(loanmaster.STATUS == 'ACTIVE')
                    elif (indict["custID"] == "" if "custID" in indict else True) & (indict["weekly"] != "1" if "weekly" in indict else True) & (indict["stage"] == "" if "stage" in indict else True):
                        q = q.where(custcred.STAGE == 'LOAN_APPROVED').where(
                            loanmaster.STATUS.notin(["REPAID", "REJECTED", "ML_REJECTED"]))
                        q3 = Query.from_(income).select(
                            income.CUSTOMER_ID).distinct()
                        q2 = Query.from_(docs).select(docs.CUSTOMER_ID).where(
                            (docs.DOCUMENT_TYPE_ID == '113') & (docs.CUSTOMER_ID.isin(q3)))
                        data2 = db.runQuery(Query.from_(custcred).select("CUSTOMER_ID").where((custcred.STAGE.isin(["GOOD_TO_LEND", "AWAITING_RE-VERIFICATION"])) &
                                                                                              (custcred.CUSTOMER_ID.isin(q2))).groupby(custcred.CUSTOMER_ID))["data"]
                    elif (indict["stage"] == "REFUND_INITIATED" if "stage" in indict else False):
                        q = q.where(custcred.STAGE == 'REFUND_INITIATED').where(loanmaster.STATUS.notin(
                            ["REPAID", "REJECTED", "ML_REJECTED", "PENDING", "REQUESTED"]))
                        data2 = db.runQuery(Query.from_(custcred).select("CUSTOMER_ID").where(custcred.STAGE.isin(
                            ["GOOD_TO_LEND", "AWAITING_RE-VERIFICATION"])).groupby(custcred.CUSTOMER_ID))["data"]
                        # print data2, db.pikastr(Query.from_(custcred).select("CUSTOMER_ID").where((custcred.STAGE=="GOOD_TO_LEND") & (custcred.CUSTOMER_ID.isin(q2))))
                        for datum in data2:
                            d2 = db.runQuery(Query.from_(loanmaster).select("STATUS").where((loanmaster.CUSTOMER_ID == datum["CUSTOMER_ID"]) &
                                                                                            (loanmaster.STATUS.isin(["REQUESTED", "PENDING"]))))
                            # if d2["data"]:
                            # db.Update(db="mint_loan", table="mw_customer_login_credentials",
                            #          conditions={"CUSTOMER_ID = ": str(datum["CUSTOMER_ID"])}, checkAll=False, STAGE="READY_TO_LEND")
                    if "orderby" in list(indict.keys()) and "order" in list(indict.keys()):
                        orderPref = Order.asc if indict["order"] == "asc" else Order.desc
                        if indict["orderby"] == "principal":
                            q = q.orderby(loandetails.PRINCIPAL, order=orderPref).limit(
                                "%i,%i" % (page["startIndex"], page["size"]))
                        elif indict["orderby"] == "outstanding":
                            q = q.orderby(loandetails.TOTAL_OUTSTANDING, order=orderPref).limit(
                                "%i,%i" % (page["startIndex"], page["size"]))
                        elif indict["orderby"] == "expiryDate":
                            q = q.orderby(loandetails.EXPECTED_MATURITY_DATE, order=orderPref).limit(
                                "%i,%i" % (page["startIndex"], page["size"]))
                        elif indict["orderby"] == "disbursedDate":
                            q = q.orderby(loanmaster.LOAN_DISBURSED_DATE, order=orderPref).limit(
                                "%i,%i" % (page["startIndex"], page["size"]))
                        elif indict["orderby"] == "createdDate":
                            q = q.orderby(loanmaster.CREATED_DATE, order=orderPref).limit(
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
                        else:
                            q = q.orderby(loandetails.TOTAL_OUTSTANDING, order=orderPref).limit(
                                "%i,%i" % (page["startIndex"], page["size"]))
                    else:
                        q = q.orderby(loandetails.TOTAL_OUTSTANDING, order=Order.desc).limit(
                            "%i,%i" % (page["startIndex"], page["size"]))
                    Fields = db.runQuery(
                        q.groupby(custcred.CUSTOMER_ID)) if 'priority' in indict else db.runQuery(q)
                    if (indict["custID"] != "" if "custID" in indict else False) and ([x for x in Fields["data"] if x["STATUS"] == "WRITTEN-OFF"]) != []:
                        ele1 = filter(
                            lambda x: x["STATUS"] == "WRITTEN-OFF", Fields["data"])[0]
                        ele2 = [x for x in Fields["data"]
                                if x["STATUS"] == "ACTIVE"]
                        if len(ele2) == 1:
                            ele2[0]["APPROVED_PRINCIPAL"] = ele1["APPROVED_PRINCIPAL"]
                            ele2[0]["PROPOSED_PRINCIPAL"] = ele1["PROPOSED_PRINCIPAL"]
                            ele2[0]["TOTAL_EXPECTED_REPAYMENT"] = ele1["TOTAL_EXPECTED_REPAYMENT"]
                            ele2[0]["TOTAL_REPAYMENT"] = ele1["TOTAL_EXPECTED_REPAYMENT"] - \
                                ele2[0]["PRINCIPAL"]
                            ele2[0]["PRINCIPAL"] = ele1["PRINCIPAL"]
                            ele2[0]["EXPECTED_MATURITY_DATE"] = ele1["EXPECTED_MATURITY_DATE"]
                            ele2[0]["ACTUAL_DISBURSEMENT_DATE"] = ele1["ACTUAL_DISBURSEMENT_DATE"]
                            ele2[0]["LOAN_REQUEST_DATE"] = ele1["LOAN_REQUEST_DATE"]
                            ele2[0]["CURRENT_OUTSTANDING"] = ele2[0]["TOTAL_OUTSTANDING"]
                            ele2[0]["LOAN_APPROVAL_DATE"] = ele1["LOAN_APPROVAL_DATE"]
                            ele2[0]["LOAN_REFERENCE_ID2"] = ele1["LOAN_REFERENCE_ID"]
                            ele2[0]["STATUS"] = 'WRITTEN-OFF'
                            Fields["data"].remove(ele1)
                    # print db.pikastr(q.groupby(custcred.CUSTOMER_ID)) if 'priority' in indict else db.pikastr(q)
#                    if ((indict["dueThisWeek"]==1 if "dueThisWeek" in indict.keys() else False) &
#                        (indict["dueAll"]==1 if "dueAll" in indict.keys() else False)):
#                        Fields = q.orderby(loanmaster.LOAN_DISBURSED_DATE, order=Order.desc).limit("%i,%i"%(page["startIndex"], page["size"]))
#                        Fields = db.runQuery(Fields)
#                    else:
#                        #print db.pikastr(q.orderby(loanmaster.ID, order=Order.desc).limit("%i,%i"%(page["startIndex"], page["size"])))
#                        Fields = db.runQuery(q.orderby(loanmaster.ID, order=Order.desc).limit("%i,%i"%(page["startIndex"], page["size"])))
                    for ele in Fields["data"]:
                        if "LIMIT_TYPE" in ele:
                            ele["LIMIT_TYPE"] = mapp[ele["LIMIT_TYPE"]] if ele["LIMIT_TYPE"] in mapp else ele["LIMIT_TYPE"].split(
                                "_LOAN_LIMIT")[0] if len(ele["LIMIT_TYPE"].split("_LOAN_LIMIT")) > 0 else ele["LIMIT_TYPE"]
                        if (indict["repay"] == 1 if "repay" in list(indict.keys()) else False):
                            refIDs = [ele["LOAN_REFERENCE_ID"]] if ("LOAN_REFERENCE_ID2" not in ele) else [
                                ele["LOAN_REFERENCE_ID"], ele["LOAN_REFERENCE_ID2"]]
                            q = Query.from_(repay).select(repay.star).where(
                                repay.LOAN_REF_ID.isin(refIDs))  # ==ele["LOAN_REFERENCE_ID"])
                            ele.update({"REPAY_INFO": db.runQuery(
                                q.orderby(repay.CREATED_DATE, order=Order.desc))["data"]})
                            d = db.runQuery(Query.from_(repay).select(repay.star).where((repay.LOAN_REF_ID.isin(refIDs)) &  # ==ele["LOAN_REFERENCE_ID"]) &
                                                                                        (repay.FINFLUX_TRAN_ID.isnull())))["data"]
                            dates = [(ele2["REPAY_DATETIME"],
                                      ele2["REPAY_AMOUNT"]) for ele2 in d]
                            temp_7 = sum(ele2[1] for ele2 in dates if (ele2[0] if (int(math.log10(
                                ele2[0]))+1) == 10 else ele2[0]/1000) > int((datetime.now() - timedelta(days=10)).strftime("%s")))
                            if temp_7 > 0:
                                ele["TOTAL_OUTSTANDING"] -= temp_7
                                ele["CURRENT_OUTSTANDING"] -= temp_7
                                # print temp_7
                            today = datetime.now().strftime("%Y-%m-%d")
                            q = Query.from_(emis).select(
                                functions.Sum(emis.OVERDUE_AMOUNT).as_("s"))
                            q = db.runQuery(q.where((emis.DUE_DATE < today) & (emis.OVERDUE_AMOUNT > 0) & (
                                emis.LOAN_ACCOUNT_NO == ele["LOAN_ACCOUNT_NO"])))
                            ele.update({"EMI_OVERDUE": ((q["data"][0]["s"]-temp_7) if (q["data"][0]["s"]-temp_7) > 0 else 0) if (temp_7 > 0) and (
                                q["data"][0]["s"]) else q["data"][0]["s"] if q["data"][0]["s"] else 0, "SHOW_AGREEMENT": 1 if ele["LOAN_REQUEST_DATE"] < '2019-09-24' else 0})
                            q = Query.from_(calldata).select(calldata.CREATED_DATE).where(
                                calldata.CUSTOMER_ID == ele["CUSTOMER_ID"])
                            q = db.runQuery(q.orderby(calldata.AUTO_ID, order=Order.desc).limit(1))[
                                "data"]
                            ele.update(
                                {"LAST_CALL_DATE": q[0]["CREATED_DATE"] if q else "1999-09-09"})
                            q = Query.from_(tasks).select(tasks.star).where(
                                tasks.LOAN_REF_ID == ele["LOAN_REFERENCE_ID"])
                            ele.update({"TASK_LIST": db.runQuery(
                                q.orderby(tasks.CREATED_DATE, order=Order.desc))["data"]})
                    if (indict["repay"] == 1 if "repay" in list(indict.keys()) else False):
                        Fields["data"] = sorted(
                            Fields["data"], key=lambda x: x["LAST_CALL_DATE"])
                        # print Fields["data"][0:5]
                    if not Fields["data"]:
                        q = Query.from_(custcred).join(
                            kyc, how=JoinType.left).on_field("CUSTOMER_ID")
                        Data = db.runQuery(q.select(custcred.LOGIN_ID, custcred.CUSTOMER_ID, kyc.NAME).where(
                            custcred.CUSTOMER_ID == (indict["custID"] if "custID" in indict else "")))
                    else:
                        Data = {"data": [{"CUSTOMER_ID": Fields["data"][0]["CUSTOMER_ID"], "LOGIN_ID":Fields["data"][0]["LOGIN_ID"],
                                          "NAME":Fields["data"][0]["NAME"]}], "error": False}
                    logins = Query.from_(users).select(users.LOGIN).where(
                        users.ACCOUNT_STATUS == 'A').orderby(users.CREATED_DATE, order=Order.desc)
                    logins = [ele["LOGIN"]
                              for ele in db.runQuery(logins)["data"]]
                    # print db.pikastr(q.orderby(loanmaster.ID,order=Order.desc).limit("%i,%i"%(page["startIndex"], page["size"])))
                    temp = db.runQuery(q1)["data"]
                    Fields.update(temp[0] if temp else {})
                    if (not Fields["error"]):
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            # if  Fields["data"]!=[]:
                            output_dict["data"].update(
                                {"loanInfo": utils.camelCase(Fields["data"])})
                            output_dict["data"].update({"logins": logins})
                            output_dict["data"].update(
                                {"custData": utils.camelCase(Data["data"]) if Data["data"] else {}})
                            output_dict["data"]["page"] = input_dict["data"]["page"]
                            output_dict["data"]["page"].update(
                                {"count": Fields["count"]})
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                            # else:
                            #    output_dict["msgHeader"]["authToken"] = token["token"]
                            #    output_dict["data"].update({"error":0, "message":"No data found"})
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)
                # print len(Fields["data"])
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
