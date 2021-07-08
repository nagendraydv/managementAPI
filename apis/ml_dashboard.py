from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, Order, JoinType


class DashboardResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"firstLoans": [], "mintloanCustomers": 0, "finfluxRegistered": 0, "loanTaken": 0, "repaymentsDue": 0, "dueAllTime": 0,
                                "todaysTasks": 0, "pendingTasks": 0, "upcomingTasks": 0, "repayInfo": 0, "totalCash": 0, "mumCash": 0, "banCash": 0,
                                "puneCash": 0, "delhiCash": 0, "chnCash": 0, "hydCash": 0, "selfCash": 0},
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
                #resp.set_header('Access-Control-Allow-Origin', '*')
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    #resp.set_header('Access-Control-Allow-Origin', '*')
                    resp.body = json.dumps(output_dict)
                else:
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    bank = Table("mw_cust_bank_detail", schema="mint_loan")
                    firstLoans = []
                    company = input_dict["data"]["company"] if (
                        "company" in input_dict["data"] if "data" in input_dict else False) else False
                    if ("as_of_date" in input_dict["data"] if "data" in input_dict else False):
                        as_of_date = input_dict["data"]["as_of_date"]
                        disbToday = db.runQuery(Query.from_(log).select(log.CUSTOMER_ID).distinct().where((log.DATA_KEY == 'STAGE') &
                                                                                                          (log.DATA_VALUE == 'LOAN_IN_PROCESS') &
                                                                                                          (log.CREATED_DATE > as_of_date)))["data"]
                        for ele in disbToday:
                            q = Query.from_(loanmaster).select("CUSTOMER_ID").where((loanmaster.STATUS.notin(["ML_REJECTED", "REJECTED"])) &
                                                                                    (loanmaster.CUSTOMER_ID == ele["CUSTOMER_ID"]))
                            loans = db.runQuery(q)["data"]
                            if len(loans) == 1:
                                q = Query.from_(kyc).join(
                                    bank, how=JoinType.left).on_field("CUSTOMER_ID")
                                q = q.select(kyc.CUSTOMER_ID, kyc.NAME, bank.ACCOUNT_NO, bank.IFSC_CODE).where(
                                    kyc.CUSTOMER_ID == ele["CUSTOMER_ID"])
                                data = db.runQuery(
                                    q.where(bank.DELETE_STATUS_FLAG == 0))["data"]
                                firstLoans.append((data[0] if data else
                                                   {"CUSTOMER_ID": ele["CUSTOMER_ID"], "NAME": "", "ACCOUNT_NO": "", "IFSC_CODE": ""}))
                        try:
                            epoch = datetime.strptime(
                                as_of_date, "%Y-%m-%d").strftime("%s")
                            epoch2 = (datetime.strptime(
                                as_of_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%s")
                        except:
                            epoch = datetime.now().date().strftime("%s")
                            epoch2 = (datetime.now() +
                                      timedelta(days=1)).strftime("%s")
                    else:
                        epoch = datetime.now().date().strftime("%s")
                        epoch2 = (datetime.now() +
                                  timedelta(days=1)).strftime("%s")
                    cities = [['Bangalore rural', 'bangalore kasavanalli', 'Bangalore', 'banglore', 'BAN', 'bangaloor', 'Bangalore ', 'Bamgalore'],
                              ['PUNE', 'punr'], ['CHENNAI'], ['DELHI'], ['hyderabad', 'hyd']]
                    ban = Query.from_(prof).select(prof.CUSTOMER_ID).where(
                        prof.CURRENT_CITY.isin(cities[0]))
                    pune = Query.from_(prof).select(prof.CUSTOMER_ID).where(
                        prof.CURRENT_CITY.isin(cities[1]))
                    mum = Query.from_(prof).select(prof.CUSTOMER_ID).where(prof.CURRENT_CITY.notin(cities[0] + cities[1] + cities[2] + cities[3] +
                                                                                                   cities[4]))
                    chn = Query.from_(prof).select(prof.CUSTOMER_ID).where(
                        prof.CURRENT_CITY.isin(cities[2]))
                    delh = Query.from_(prof).select(prof.CUSTOMER_ID).where(
                        prof.CURRENT_CITY.isin(cities[3]))
                    hyd = Query.from_(prof).select(prof.CUSTOMER_ID).where(
                        prof.CURRENT_CITY.isin(cities[4]))
                    q = Query.from_(repay).select(functions.Sum(repay.REPAY_AMOUNT).as_("s")).where((repay.MODE_OF_PAYMENT == "CASH") &
                                                                                                    (repay.CREATED_DATE > epoch) &
                                                                                                    (repay.CREATED_DATE < epoch2))
                    totalCash = db.runQuery(q)["data"][0]["s"]
                    puneCash = db.runQuery(q.where(repay.CUSTOMER_ID.isin(pune)))[
                        "data"][0]["s"]
                    banCash = db.runQuery(q.where(repay.CUSTOMER_ID.isin(ban)))[
                        "data"][0]["s"]
                    mumCash = db.runQuery(q.where(repay.CUSTOMER_ID.isin(mum)))[
                        "data"][0]["s"]
                    chnCash = db.runQuery(q.where(repay.CUSTOMER_ID.isin(chn)))[
                        "data"][0]["s"]
                    delhCash = db.runQuery(q.where(repay.CUSTOMER_ID.isin(delh)))[
                        "data"][0]["s"]
                    hydCash = db.runQuery(q.where(repay.CUSTOMER_ID.isin(hyd)))[
                        "data"][0]["s"]
                    selfCash = db.runQuery(q.where(
                        repay.CREATED_BY == input_dict["msgHeader"]["authLoginID"]))["data"][0]["s"]
                    # mapp = {"Create Customer V3":"createCustomer", "Login":"login", "Get Aadhaar Kyc  Details":"getAadharDetails",
                    #        "Basic Profile Update": "profileUpdate", "Get Bank Details": "getBankDetails",
                    #        "Register Client Fin Flux": "finfluxRegister", "Generate Mandate":"genMandate",
                    #        "Sumbit loan application": "loanSubmit", "Uploading file to S3 AWS.":"fileUpload"}
                    mlCust = Query.from_(custcred).join(prof, how=JoinType.left).on_field("CUSTOMER_ID").select(
                        functions.Count(custcred.CUSTOMER_ID).as_("MINTLOAN_CUSTOMERS")).where(custcred.STAGE != 'REJECTED')
                    mlCust = db.runQuery(mlCust if not company else mlCust.where(
                        prof.COMPANY_NAME == company))
                    mlCust = mlCust["data"][0]["MINTLOAN_CUSTOMERS"] if mlCust["data"] else 0
                    regCust = Query.from_(clientmaster).join(prof, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select(functions.Count(clientmaster.CUSTOMER_ID).as_("REGISTERED_CUSTOMERS"))
                    regCust = db.runQuery(regCust if not company else regCust.where(
                        prof.COMPANY_NAME == company))
                    regCust = regCust["data"][0]["REGISTERED_CUSTOMERS"] if regCust["data"] else 0
                    active = Query.from_(loanmaster).join(prof, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select(functions.Count(loanmaster.CUSTOMER_ID).distinct().as_("ACTIVE"))
                    active = active if not company else active.where(
                        prof.COMPANY_NAME == company)
                    active = db.runQuery(active.where(loanmaster.STATUS == 'ACTIVE'))[
                        "data"][0]["ACTIVE"]
                    date1_prev_month = (
                        datetime.today() + relativedelta(months=-1)).strftime("%Y-%m-%d")
                    date2_prev_month = (datetime.today(
                    ) + relativedelta(months=-1) + relativedelta(days=7)).strftime("%Y-%m-%d")
                    q = Query.from_(loanmaster).select(functions.Count(
                        loanmaster.CUSTOMER_ID).distinct().as_("DUE_THIS_WEEK"))
                    q = q.where((loanmaster.LOAN_DISBURSED_DATE >= date1_prev_month) & (
                        loanmaster.LOAN_DISBURSED_DATE <= date2_prev_month))
                    date2_prev_month = (datetime.today(
                    ) + relativedelta(months=-1) + relativedelta(days=-1)).strftime("%Y-%m-%d")
                    dueThisWeek = db.runQuery(
                        q.where(loanmaster.STATUS == 'ACTIVE'))["data"]
                    dueThisWeek = dueThisWeek[0]["DUE_THIS_WEEK"] if dueThisWeek else 0
                    q = Query.from_(loanmaster).join(loandetails).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    q = q.select(functions.Count(
                        loanmaster.CUSTOMER_ID).distinct().as_("DUE_ALL_TIME"))
                    q = db.runQuery(q.where((loanmaster.LOAN_DISBURSED_DATE <= date2_prev_month) & (
                        loanmaster.STATUS == "ACTIVE")))
                    dueAllTime = q["data"][0]["DUE_ALL_TIME"] if q["data"] else 0
                    q = Query.from_(tasks).select(
                        functions.Count(tasks.star).as_('C'))
                    q = q.where(tasks.LOGIN_ID.isin(([input_dict["msgHeader"]["authLoginID"]] +
                                                     ([] if input_dict["msgHeader"]["authLoginID"] != 'aparajeeta@mintwalk.com' else
                                                      ['disbursementTeam']) +
                                                     ([] if input_dict["msgHeader"]["authLoginID"] not in ['vaibhav.patil@mintwalk.com', '9967299619']
                                                      else ['repaymentTeam']))))
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
                    repayInf = db.runQuery(Query.from_(repay).select(functions.Count(
                        repay.star).as_("C")).orderby(repay.AUTO_ID, order=Order.desc))
                    repayInf = repayInf["data"][0]["C"] if repayInf["data"] else 0
                    # if input_dict["msgHeader"]["authLoginID"]!="dharam@mintwalk.com" else {"updated":True, "token":"scGEx8.gYYwXlGxTGiIMZO2OJ7qdIcuZy0vUA4sPsFc.!rxNMy4BkRT/P6pxWvjP2G6iDwIFukf+o"}
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"mintloanCustomers": mlCust, "finfluxRegistered": regCust, "loanTaken": active,
                                                    "repaymentsDue": dueThisWeek, "dueAllTime": dueAllTime, "todaysTasks": tTasks,
                                                    "pendingTasks": yTasks, "upcomingTasks": uTasks, "repayInfo": repayInf, "firstLoans": firstLoans,
                                                    "selfCash": selfCash if selfCash else 0, "totalCash": totalCash if totalCash else 0,
                                                    "mumCash": mumCash if mumCash else 0, "banCash": banCash if banCash else 0,
                                                    "puneCash": puneCash if puneCash else 0, "chnCash": chnCash if chnCash else 0,
                                                    "delhiCash": delhCash if delhCash else 0, "hydCash": hydCash if hydCash else 0})
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                #resp.set_header('Access-Control-Allow-Origin', '*')
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise


app = falcon.API()
dashboard = DashboardResource()

app.add_route('/dashboard', dashboard)
