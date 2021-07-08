from __future__ import absolute_import
import falcon
import json
import requests
import grequests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
from six.moves import range


class CreditEvalInfoResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"custCredentials": {}, "custDetails": {}, "loans": [], "loanLimit": "", "document": [],
                                                                "loanLimitComments": "", "carOwnership": "", "carOwnership2": "", "criteriaMet": {}}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'customerDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='custDetails', request=input_dict):
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
                    custID = input_dict["data"]["customerID"]
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    stagemaster = Table("mw_stage_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    loanlimit = Table("mw_client_loan_limit",
                                      schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    income2 = Table("mw_derived_income_data",
                                    schema="mw_company_3")
                    si = Table("mw_swiggy_income_data", schema="mint_loan")
                    uauth = Table("mw_authorization_dump",
                                  schema="mw_company_3")
                    cprof = Table("mw_profile_info", schema="mw_company_3")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    repayhist = Table(
                        "mw_client_loan_repayment_history_master", schema="mint_loan")
                    mandate = Table(
                        "mw_physical_mandate_status", schema="mint_loan")
                    agree = Table("mw_user_agreement", schema="mint_loan")
                    tran = Table("mw_client_loan_transactions",
                                 schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",
                                 schema="mint_loan")
                    today = datetime.now().strftime("%Y-%m-%d")
                    excelData = db.runQuery(Query.from_(income).select(income.star).where(
                        income.CUSTOMER_ID == custID).orderby(income.WEEK, order=Order.desc).limit(1))["data"]
                    stages = [{"STAGE": ele["STAGE"], "DISABLED":ele["DISABLED"]} for ele in db.runQuery(
                        Query.from_(stagemaster).select("STAGE", "DISABLED"))["data"] if ele["STAGE"]]
                    custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "LAST_LOGIN", "CUSTOMER_ID", "STAGE",
                                                                               "REGISTERED_IP_ADDRESS", "LAST_LOGGED_IN_IP_ADDRESS", "COMMENTS",
                                                                               "DEVICE_ID", "CREATED_DATE", "REJECTED",
                                                                               "REJECTION_REASON").where(custcred.CUSTOMER_ID == custID))
                    custDetails = db.runQuery(Query.from_(profile).select(
                        profile.star).where(profile.CUSTOMER_ID == custID))
                    loanLimit = db.runQuery(Query.from_(loanlimit).select("LOAN_LIMIT", "COMMENTS").where((loanlimit.CUSTOMER_ID == custID) &
                                                                                                          (loanlimit.ARCHIVED == 'N')))
                    q = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    loans = db.runQuery(q.select(loanmaster.star, loandetails.star).where((loanmaster.CUSTOMER_ID == custID) &
                                                                                          (loanmaster.STATUS.isin(["WRITTEN-OFF", "REPAID"]))).orderby(loanmaster.ID, order=Order.asc))
                    loans2 = db.runQuery(q.select(loanmaster.star, loandetails.star).where((loanmaster.CUSTOMER_ID == custID) &
                                                                                           (loanmaster.STATUS == 'ACTIVE') &
                                                                                           (loandetails.EXPECTED_MATURITY_DATE < today)))
                    loans["data"] += loans2["data"]
                    loans["data"] = loans["data"][-5:]
                    for ele in loans["data"]:
                        q = Query.from_(repayhist).select("TRANSACTION_STATUS").where(
                            repayhist.TRANSACTION_MEDIUM.isin(["UBER_DIRECT_DEBIT", "COMPANY_DIRECT_DEBIT"]))
                        tranStat = [X["TRANSACTION_STATUS"] for X in db.runQuery(
                            q.where(repayhist.LOAN_ID == ele["LOAN_ACCOUNT_NO"]))["data"]]
                        L = len(tranStat)/100. if len(tranStat) > 0 else 0.01
                        q = Query.from_(repayhist).select("TRANSACTION_STATUS").where(
                            repayhist.TRANSACTION_MEDIUM == "NACH_DEBIT")
                        #q = q.where(repayhist.TRANSACTION_STATUS=='FAILURE')
                        tranStat2 = [X["TRANSACTION_STATUS"] for X in db.runQuery(
                            q.where(repayhist.LOAN_ID == ele["LOAN_ACCOUNT_NO"]))["data"]]
                        Emis = db.runQuery(Query.from_(emis).select(emis.star).where((emis.LOAN_ACCOUNT_NO == ele["LOAN_ACCOUNT_NO"]) &
                                                                                     (emis.CUSTOMER_ID == custID) &
                                                                                     (emis.DUE_DATE < datetime.now().strftime("%Y-%m-%d 00:00:00"))))
                        try:
                            X = [((datetime.strptime(x["PAID_DATE"], "%Y-%m-%d %H:%M:%S") if ((x["PAID_DATE"] is not None) & (x["PAID_DATE"] != ""))
                                   else datetime.now())-datetime.strptime(x["DUE_DATE"], "%Y-%m-%d %H:%M:%S")).days for x in Emis["data"]]
                            avgDelay = sum(X)/len(X)
                        except:
                            avgDelay = 0
                        trans = db.runQuery(Query.from_(tran).select(tran.star).where((tran.TYPE == 'Repayment') & (tran.CUSTOMER_ID == custID) &
                                                                                      (tran.LOAN_ACCOUNT_NO == ele["LOAN_ACCOUNT_NO"])))
                        matDate = (datetime.strptime(ele["EXPECTED_MATURITY_DATE"], "%Y-%m-%d").date()
                                   if (ele["EXPECTED_MATURITY_DATE"] if "EXPECTED_MATURITY_DATE" in ele else False) else datetime.now().date())
                        s = 0
                        for ele2 in trans["data"]:
                            s += ele2["AMOUNT"]
                        try:
                            overdue = ((datetime.now().date() - matDate).days if ele["STATUS"] == "ACTIVE" else
                                       (max([datetime.strptime(ele2["TRANSACTION_DATE"], "%Y-%m-%d")
                                             for ele2 in trans["data"]]).date()-matDate).days if ele["STATUS"] == "REPAID" else 0)
                        except:
                            overdue = "NA"
                        ele.update({"TRANSACTIONS": trans["data"], "TOTAL_REPAID": s, "OVERDUE_DAYS": overdue, "AVERAGE_DELAY": avgDelay,
                                    "UBER_DEBIT_PART_SUCCESS": "%i" % (tranStat.count("PART_SUCCESS")), "NACH_FAILURE": "%i" % (tranStat2.count("FAILURE")),
                                    "UBER_DEBIT_FAILURE": "%i" % (tranStat.count("FAILURE")), "NACH_SUCCESS": "%i" % (tranStat2.count("SUCCESS"))})
                    mandateData = db.runQuery(Query.from_(mandate).select(
                        mandate.star).where(mandate.CUSTOMER_ID == custID))
                    document = db.runQuery(Query.from_(agree).select(agree.star).where((agree.CUSTOMER_ID == custID) & (
                        agree.TYPE_OF_DOCUMENT.isin(['UBER DIGITAL DEDUCT AGREEMENT', 'UBER DIGITAL INFO AGREEMENT']))))
                    mw = db.runQuery(Query.from_(income2).select(functions.Max(
                        income2.WEEK).as_("mw")).where(income2.NO_OF_TRIPS > 1))["data"][0]["mw"]
                    # print(type(mw))
                    lastWeekDataAvailable = (
                        datetime.now() - datetime.strptime(mw, "%Y-%m-%d")).days in (7, 8, 9, 10)
                    income2 = db.runQuery(Query.from_(income2).select(income2.star).where(
                        income2.CUSTOMER_ID == custID).orderby(income2.WEEK, order=Order.desc))
                    cprof = db.runQuery(Query.from_(cprof).select(cprof.FIRST_TRIP_WEEK).where(
                        cprof.CONFIRMED_CUSTOMER_ID == custID).orderby(cprof.AUTO_ID, order=Order.desc).limit(1))["data"]
                    fweek = cprof[0]["FIRST_TRIP_WEEK"] if cprof else ''
                    wrongData = ((income2["data"][0]["INCOME"] == 0) and (income2["data"][0]["NO_OF_TRIPS"] > 0) and (
                        income2["data"][0]["PARTNER_TYPE"] in ('fleet_dco', 'fleet_ndp', 'single_dco', 'single_ndp'))) if income2["data"] else False
                    if not income2["data"]:
                        mw = db.runQuery(Query.from_(income).select(functions.Max(
                            income.WEEK).as_("mw")).where(income.NO_OF_TRIPS > 1))["data"][0]["mw"]
                        income = db.runQuery(Query.from_(income).select(income.star).where(
                            income.CUSTOMER_ID == custID).orderby(income.WEEK, order=Order.desc))
                        income.update({"api": False})
                    elif wrongData:
                        income1 = db.runQuery(Query.from_(income).select(income.star).where(
                            income.CUSTOMER_ID == custID).orderby(income.WEEK, order=Order.desc))
                        income1.update({"api": False})
                        if (income1["data"][0]["WEEK"] == income2["data"][0]["WEEK"]) if income1["data"] else False:
                            mw = db.runQuery(Query.from_(income).select(functions.Max(
                                income.WEEK).as_("mw")).where(income.NO_OF_TRIPS > 1))["data"][0]["mw"]
                            income = income1
                        else:
                            income = income2
                            income.update({"api": True})
                    else:
                        income = income2
                        income.update({"api": True})
                        atoken = db.runQuery(Query.from_(uauth).select(
                            "ACCESS_TOKEN", "REFRESH_TOKEN", "AUTO_ID").where(uauth.CONFIRMED_CUSTOMER_ID == custID))
                        atoken, rtoken, autoID = (atoken["data"][0]["ACCESS_TOKEN"], atoken["data"][0]
                                                  ["REFRESH_TOKEN"], atoken["data"][0]["AUTO_ID"]) if atoken["data"] else ('', '', '')
                        cust_headers = {
                            "content-type": "application/json", "Authorization": ("Bearer " + atoken)}
                        r11 = requests.get(
                            "https://api.uber.com/v1/partners/me", headers=cust_headers, verify=False)
                        if r11.status_code == 401:
                            r2 = requests.post("https://login.uber.com/oauth/v2/token", data={
                                               "client_id": "U4XCFbyEXwwQ0TF0oLveLcXz-Vo_ddkn", "grant_type": "refresh_token", "refresh_token": rtoken, "client_secret": "JnH50ymytYbXqceXnUMSnY_Qf99rtHTQ6YcZVVJ_"})
                            cust_headers = {"content-type": "application/json", "Authorization": "Bearer " + (
                                r2.json()["access_token"] if "access_token" in r2.json() else '')}
                            db.Update(db="mw_company_3", table="mw_authorization_dump", ACCESS_TOKEN=r2.json()[
                                      "access_token"], conditions={"AUTO_ID=": str(autoID)})
                            r11 = requests.get(
                                "https://api.uber.com/v1/partners/me", headers=cust_headers, verify=False)
                        # if not lastWeekDataAvailable:
                        #    from_time = (datetime.now() - timedelta(days=(8-datetime.now().weekday()))).strftime("%s")
                        #    to_time = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime("%s")
                        #    payUrl = "https://api.uber.com/v1/partners/payments" + "?limit=50&from_time=%s"%from_time + "&to_time=%s"%to_time
                        #    r = requests.get(payUrl, headers=cust_headers, verify=False)
                        #    cust_payments=[]
                        #    count = 0
                        #    offset = 0
                        #    if r.status_code==200:
                        #        cust_payments += r.json()["payments"]
                        #        count = r.json()["count"]
                        #        urls=[payUrl.replace("?", "?offset=%s&"%j) for j in range(50, count, 50)]
                        #        rs = (grequests.get(u, headers=cust_headers) for u in urls)
                        #        resps=grequests.map(rs)
                        #        for resp in resps:
                        #            cust_payments+=(resp.json()["payments"] if (resp.status_code==200 if resp else False) else [])
                        #        lastWeekIncome = sum(ele["amount"] for ele in (filter(lambda x:x["category"] in ('fare', 'promotion', 'other'), cust_payments)))
                        #        x = income["data"][0].copy()
                        #        x["WEEK"] = (datetime.now() - timedelta((days=datetime.now()-timedelta(days=8)).weekday())).strftime("%Y-%m-%d")
                        #        x["INCOME"] = sum(ele["amount"] for ele in (filter(lambda x:x["category"] in ('fare', 'promotion', 'other'), cust_payments)) if ele["amount"])
                        #        x["CASH_COLLECTED"] = sum(ele["cash_collected"] for ele in (filter(lambda x:x["category"] in ('fare', 'promotion', 'other'), cust_payments)) if ele["cash_collected"])
                        #        x["INCOME_OUTGO"]=0
                        #        income["data"] = [x] + income["data"]
                        #        print income
                        income["data"][0].update({"PRESENT_STATUS": ((r11.json()[
                                                 "activation_status"] + " (latest)") if "activation_status" in r11.json() else None)})
                        if "activation_status" in r11.json():
                            income["data"][0].update(
                                {"PRESENT_STATUS": (r11.json()["activation_status"] + "(latest)")})
                            db.Update(db="mw_company_3", table="mw_profile_info", conditions={
                                      "CONFIRMED_CUSTOMER_ID=": str(custID)}, ACTIVATION_STATUS=r11.json()["activation_status"])

                    if income["data"]:
                        cities = {"215": "MUM", "130": "BAN", "342": "PUNE",
                                  "209": "CHENNAI", "197": "DELHI", "473": "DELHI", "474": "DELHI"}
                        make = ''  # income["data"][0]["MAKE"]
                        model = ''  # income["data"][0]["MODEL"]
                        licenseNo = ''  # income["data"][0]["LICENSE_NUMBER"]
                        xliTag = excelData[0]["XLI_TAG"] if excelData else ''
                        uberPlusTier = ((income["data"][0]["TIER"] if income["data"][0]["TIER"] else 'None') + "(api)" + (("/" + (excelData[0]["UBER_PLUS_TIER"] if excelData[0]["UBER_PLUS_TIER"] else 'None') +
                                                                                                                           "(fileUpload)") if excelData else '')) if income["api"] else ((income["data"][0]["UBER_PLUS_TIER"] if income["data"][0]["UBER_PLUS_TIER"] else "None") + "(fileUpload)")
                        #uberPlusTier = ((((income["data"][0]["UBER_PLUS_TIER"] + ("(api)" if income["api"] else "(fileUpload)")) if income["data"][0]["UBER_PLUS_TIER"] else 'None') + "(fileUpload)") if "UBER_PLUS_TIER" in income["data"][0] else ((income["data"][0]["TIER"]  if income["data"][0]["TIER"] else 'None' + "(api)") + (("      "+excelData[0]["UBER_PLUS_TIER"] + "(fileUpload)") if excelData else '')) if "TIER" in income["data"][0] else '')
                        presentStatus = ((income["data"][0]["PRESENT_STATUS"] + ("(api)" if income["api"] else "(fileUpload)")) if income["data"][0]["PRESENT_STATUS"] else 'None') + "/" + (
                            (((excelData[0]["PRESENT_STATUS"] if excelData[0]["PRESENT_STATUS"] else 'None') + "(fileUpload)") if excelData else '') if income["api"] else '')
                        partnerVehicles = income["data"][0]["TOTAL_PARTNER_VEHICLES"] if "TOTAL_PARTNER_VEHICLES" in income[
                            "data"][0] else income["data"][0]["TOTAL_VEHICLES"] if "TOTAL_VEHICLES" in income["data"][0] else ''
                        activeVehicles = income["data"][0]["ACTIVE_VEHICLES"]
                        workingSince = "%.2f" % ((datetime.now() - ((datetime.strptime(income["data"][0]["FIRST_TRIP_WEEK"], "%Y-%m-%d") if income["data"][0]["FIRST_TRIP_WEEK"] else datetime.now(
                        )) if "FIRST_TRIP_WEEK" in income["data"][0] else (datetime.strptime(fweek, "%Y-%m-%d") if fweek else datetime.now()))).days/365.25)
                        city = (cities[income["data"][0]["CITY_ID"]] if income["data"][0]["CITY_ID"]
                                in cities else "Unknown") if "CITY_ID" in income["data"][0] else ''
                    else:
                        make = model = workingSince = licenseNo = city = xliTag = presentStatus = activeVehicles = partnerVehicles = uberPlusTier = ""
                    averageIncome = 0.
                    averageIncome2 = 0.
                    averageIncome3 = 0.
                    averageCash = 0.
                    averageCash2 = 0.
                    averageCash3 = 0.
                    lincome = len(income["data"])
                    if lincome == 0:
                        q = db.runQuery(Query.from_(si).select(
                            "INCOME", "WEEK_NUMBER", "WEEK_YEAR").where(si.CUSTOMER_ID == custID))["data"]
                        income["data"] = [{"INCOME": ele["INCOME"], "WEEK":(datetime(
                            ele["WEEK_YEAR"], 0o1, 0o1) + timedelta(days=(ele["WEEK_NUMBER"]-1)*7-2)).strftime("%Y-%m-%d")} for ele in q]
                        lincome = len(income["data"])
                        income["data"] = sorted(
                            income["data"], key=lambda i: i["WEEK"], reverse=True)
                    # if lincome==0:
                    #    income = db.runQuery(Query.from_(income2).select(income2.star).where(income2.CUSTOMER_ID==custID))
                    #    lincome = len(income["data"])
                    if lincome >= 1:  # calculate average income of 12 weeks of data
                        try:
                            iData = [(datetime.strptime(income["data"][0]["WEEK"].split(" ")[0], "%Y-%m-%d"), income["data"][0]["INCOME"], income["data"][0]["CASH_COLLECTED"]
                                      if "CASH_COLLECTED" in income["data"][0] else None, income["data"][0]["INCOME_OUTGO"] if "INCOME_OUTGO" in income["data"][0] else None)]
                            dFormat = "%Y-%m-%d"
                        except:
                            try:
                                iData = [(datetime.strptime(income["data"][0]["WEEK"].split(" ")[0], "%y-%m-%d"), income["data"][0]["INCOME"], income["data"][0]["CASH_COLLECTED"]
                                          if "CASH_COLLECTED" in income["data"][0] else None, income["data"][0]["INCOME_OUTGO"] if "INCOME_OUTGO" in income["data"][0] else None)]
                                dFormat = "%y-%m-%d"
                            except:
                                iData = [(datetime.now() - timedelta(days=100), income["data"][0]["INCOME"], income["data"][0]["CASH_COLLECTED"]
                                          if "CASH_COLLECTED" in income["data"][0] else None, income["data"][0]["INCOME_OUTGO"] if "INCOME_OUTGO" in income["data"][0] else None)]
                                dFormat = ""
                        if (((datetime.now()-iData[0][0]).days > 14) if (not lastWeekDataAvailable) else ((datetime.now()-iData[0][0]).days >= 14)):
                            iData = [(iData[0][0]+timedelta(days=7*i), 0, 0, 0) for i in range(
                                int((datetime.now()-iData[0][0]).days/7)-1, 0, -1)] + [iData[0]] #introduced int here for python3 version - not sure if the logic still holds
                        for i in range(1, lincome):
                            if i < lincome:
                                try:
                                    d = datetime.strptime(
                                        income["data"][i]["WEEK"].split(" ")[0], dFormat)
                                except:
                                    d = iData[-1][0] - timedelta(days=7)
                                if (iData[-1][0] - d).days > 10:
                                    for j in range(int(((iData[-1][0] - d).days)/7)-1, 0, -1): #introduced int here for python3 version - not sure if the logic still holds
                                        # do not include the element if the date is already present in the list
                                        if d not in list(zip(*iData))[0]:
                                            # append zero for missing week(s) income
                                            iData.append(
                                                (d+timedelta(days=7*j), 0, None, None))
                                # do not include the element if the date is already present in the list
                                if d not in list(zip(*iData))[0]:
                                    iData.append((d, income["data"][i]["INCOME"], income["data"][i]["CASH_COLLECTED"] if "CASH_COLLECTED" in income["data"]
                                                  [i] else None, income["data"][i]["INCOME_OUTGO"] if "INCOME_OUTGO" in income["data"][i] else None))
                        # print iData
                        averageIncome = int(
                            4.25*sum(list(zip(*iData))[1][0:10])/(10.))
                        averageIncome2 = int(4.25*sum(list(zip(*iData))[1][0:3])/3.)
                        averageIncome3 = int(4.25*iData[0][1])
                        averageCash = "%.2f" % (
                            100*(4.25*sum([_f for _f in list(zip(*iData))[2][0:10] if _f])/(10.))/(averageIncome+0.1))
                        averageCash2 = "%.2f" % (
                            100*(4.25*sum([_f for _f in list(zip(*iData))[2][0:3] if _f])/3)/(averageIncome2+0.1))
                        averageCash3 = "%.2f" % (
                            100*((4.25*iData[0][2]) if iData[0][2] else 0)/(averageIncome3+0.1))
                    if income["data"]:
                        ok5 = ('1' if ((averageIncome >= 30000) & (averageIncome2 > 25000) &  # ((income["data"][0]["XLI_TAG"]!=1 if "XLI_TAG" in income["data"][0] else False) if income["data"] else False) &
                                       (False not in {ele["AVERAGE_DELAY"] < 20 for ele in loans["data"][-1:]}) & (len(document["data"]) > 1)) else "0")
                        # ('Y' in {ele["VERIFICATION_STATUS"] for ele in document["data"]})) else "0")
                        ok10 = "1" if ((ok5 == "1") & (5000 <= (max(
                            ele["PRINCIPAL"] for ele in loans["data"]) if loans["data"] else loanLimit["data"][0]["LOAN_LIMIT"] if loanLimit["data"] else 0))) else "0"
                        ok15 = "1" if ((ok10 == "1") & (averageIncome >= 40000) & (averageIncome2 > 30000) &
                                       (10000 <= (max(ele["PRINCIPAL"] for ele in loans["data"]) if loans["data"] else loanLimit["data"][0]["LOAN_LIMIT"] if loanLimit["data"] else 0))) else "0"
                        ok20 = "1" if ((ok15 == "1") & (averageIncome >= 40000) & (averageIncome2 > 40000) &
                                       (15000 <= (max(ele["PRINCIPAL"] for ele in loans["data"]) if loans["data"] else loanLimit["data"][0]["LOAN_LIMIT"] if loanLimit["data"] else 0))) else "0"
                        ok25 = "1" if ((ok20 == "1") & (averageIncome >= 50000) & (averageIncome2 > 50000) &
                                       (20000 <= (max(ele["PRINCIPAL"] for ele in loans["data"]) if loans["data"] else loanLimit["data"][0]["LOAN_LIMIT"] if loanLimit["data"] else 0))) else "0"
                    else:
                        ok5 = ok10 = ok15 = ok20 = ok25 = "0"
                    if (custCredentials["data"]):
                        token = generate(dbw).AuthToken()
                        if token["updated"]:
                            output_dict["data"]["criteriaMet"] = {
                                "5000": ok5, "10000": ok10, "15000": ok15, "20000": ok20, "25000": ok25}
                            output_dict["data"]["mandateData"] = utils.camelCase(
                                mandateData["data"])
                            output_dict["data"]["stages"] = stages
                            output_dict["data"]["averageIncome"] = averageIncome
                            output_dict["data"]["averageIncome2"] = averageIncome2
                            output_dict["data"]["averageIncome3"] = averageIncome3
                            output_dict["data"]["averageCash"] = averageCash
                            output_dict["data"]["averageCash2"] = averageCash2
                            output_dict["data"]["averageCash3"] = averageCash3
                            output_dict["data"]["car"] = (
                                (make + " " + model) if ((make is not None) & (model is not None)) else "")
                            output_dict["data"]["workingSince"] = workingSince
                            output_dict["data"]["licenseNo"] = licenseNo
                            output_dict["data"]["city"] = city
                            output_dict["data"]["presentStatus"] = presentStatus
                            output_dict["data"]["xliTag"] = xliTag
                            output_dict["data"]["uberPlusTier"] = uberPlusTier
                            output_dict["data"]["partnerVehicles"] = partnerVehicles
                            output_dict["data"]["activeVehicles"] = activeVehicles
                            output_dict["data"]["carOwnership"] = (
                                income["data"][0]["DCO"] if "DCO" in income["data"][0] else "") if income["data"] else ""
                            output_dict["data"]["carOwnership2"] = (((((income["data"][0]["PARTNER_TYPE"] if income["data"][0]["PARTNER_TYPE"] else '') + ("(api)" if income["api"] else "(fileUpload)")) if "PARTNER_TYPE" in income["data"][0] else "") + "/" + (
                                (((excelData[0]["PARTNER_TYPE"] if excelData[0]["PARTNER_TYPE"] else 'None') + "(fileUpload)") if excelData else '') if income["api"] else ''))) if (income["data"] != []) and (excelData != []) else ''
                            output_dict["data"]["custCredentials"] = utils.camelCase(
                                custCredentials["data"][0])
                            output_dict["data"]["custDetails"] = utils.camelCase(
                                custDetails["data"][0]) if custDetails["data"] else []
                            output_dict["data"]["document"] = utils.camelCase(
                                document["data"]) if document["data"] else {}
                            if loanLimit["data"]:
                                output_dict["data"]["loanLimit"] = loanLimit["data"][0][
                                    "LOAN_LIMIT"] if loanLimit["data"][0]["LOAN_LIMIT"] else ""
                                output_dict["data"]["loanLimitComments"] = (loanLimit["data"][0]["COMMENTS"] if loanLimit["data"][0]["COMMENTS"]
                                                                            else "")
                            else:
                                output_dict["data"]["loanLimit"], output_dict["data"]["loanLimitComments"] = (
                                    "", "")
                            output_dict["data"]["loans"] = utils.camelCase(
                                loans["data"]) if loans["data"] else []
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)#, encoding='unicode-escape')
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
