from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
from six.moves import range


class CustIncomeDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"custCredentials": {}, "custDetails": {}, "incomeData": [], "averageIncome": 0.,
                                                                "averageIncome2": 0.}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'customerIncome'}
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
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",schema="mint_loan")
                    income2 = Table("mw_derived_income_data",schema="mw_company_3")
                    si = Table("mw_swiggy_income_data", schema="mint_loan")
                    custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "LAST_LOGIN", "CUSTOMER_ID", "STAGE",
                                                                               "REGISTERED_IP_ADDRESS", "LAST_LOGGED_IN_IP_ADDRESS", "COMMENTS",
                                                                               "DEVICE_ID", "CREATED_DATE", "REJECTED",
                                                                               "REJECTION_REASON").where(custcred.CUSTOMER_ID == custID))
                    custDetails = db.runQuery(Query.from_(profile).select(
                        profile.star).where(profile.CUSTOMER_ID == custID))
                    mw = db.runQuery(Query.from_(income2).select(functions.Max(income2.WEEK).as_("mw")).where(income2.NO_OF_TRIPS > 1))["data"][0]["mw"]
                    #print("Value of MW ",str(mw))
                    lastWeekDataAvailable = (
                        datetime.now() - datetime.strptime(mw, "%Y-%m-%d")).days in (7, 8, 9, 10)
                    income2 = db.runQuery(Query.from_(income2).select(income2.star).where(
                        income2.CUSTOMER_ID == custID).orderby(income2.WEEK, order=Order.desc))
                    wrongData = ((income2["data"][0]["INCOME"] == 0) and (income2["data"][0]["NO_OF_TRIPS"] > 0) and (
                        income2["data"][0]["PARTNER_TYPE"] in ('fleet_dco', 'fleet_ndp', 'single_dco', 'single_ndp'))) if income2["data"] else False
                    if not income2["data"]:
                        mw = db.runQuery(Query.from_(income).select(functions.Max(
                            income.WEEK).as_("mw")).where(income.NO_OF_TRIPS > 1))["data"][0]["mw"]
                        income = db.runQuery(Query.from_(income).select(income.star).where(
                            income.CUSTOMER_ID == custID).orderby(income.WEEK, order=Order.desc))
                    elif wrongData:
                        income1 = db.runQuery(Query.from_(income).select(income.star).where(
                            income.CUSTOMER_ID == custID).orderby(income.WEEK, order=Order.desc))
                        if (income1["data"][0]["WEEK"] == income2["data"][0]["WEEK"]) if income1["data"] else False:
                            mw = db.runQuery(Query.from_(income).select(functions.Max(
                                income.WEEK).as_("mw")).where(income.NO_OF_TRIPS > 1))["data"][0]["mw"]
                            income = income1
                        else:
                            income = income2
                    else:
                        income = income2
                    averageIncome = 0.
                    averageIncome2 = 0.
                    averageIncome3 = 0.
                    lincome = len(income["data"])
                    if lincome == 0:
                        q = db.runQuery(Query.from_(si).select(
                            "INCOME", "WEEK_NUMBER", "WEEK_YEAR").where(si.CUSTOMER_ID == custID))["data"]
                        income["data"] = [{"INCOME": ele["INCOME"], "WEEK":(datetime(
                            ele["WEEK_YEAR"], 0o1, 0o1) + timedelta(days=(ele["WEEK_NUMBER"]-1)*7-2)).strftime("%Y-%m-%d")} for ele in q]
                        lincome = len(income["data"])
                        income["data"] = sorted(
                            income["data"], key=lambda i: i["WEEK"], reverse=True)
                    if lincome >= 1:  # calculate average income of 12 weeks of data
                        try:
                            iData = [(datetime.strptime(income["data"][0]["WEEK"].split(
                                " ")[0], "%Y-%m-%d"), income["data"][0]["INCOME"])]
                            dFormat = "%Y-%m-%d"
                        except:
                            try:
                                iData = [(datetime.strptime(income["data"][0]["WEEK"].split(
                                    " ")[0], "%y-%m-%d"), income["data"][0]["INCOME"])]
                                dFormat = "%y-%m-%d"
                            except:
                                iData = [
                                    (datetime.now() - timedelta(days=100), income["data"][0]["INCOME"])]
                                dFormat = ""
                        if (((datetime.now()-iData[0][0]).days > 14) if (not lastWeekDataAvailable) else ((datetime.now()-iData[0][0]).days >= 14)):
                            #print(iData[0][0])
                            iData = [(iData[0][0]+timedelta(days=7*i), 0) for i in range(int((datetime.now()-iData[0][0]).days/7)-1, 0, -1)] + [iData[0]]
                        for i in range(1, lincome):
                            if i < lincome:
                                try:
                                    d = datetime.strptime(
                                        income["data"][i]["WEEK"].split(" ")[0], dFormat)
                                except:
                                    d = iData[-1][0] - timedelta(days=7)
                                if (iData[-1][0] - d).days > 10:
                                    for j in range(int(((iData[-1][0] - d).days)/7)-1, 0, -1): #added int in python3 version, need to check if logically correct
                                        # do not include the element if the date is already present in the list
                                        if d not in list(zip(*iData))[0]:
                                            # append zero for missing week(s) income
                                            iData.append((d+timedelta(days=7*j), 0))
                                if d not in list(zip(*iData))[0]:
                                    iData.append(
                                        (d, income["data"][i]["INCOME"]))
                        # print iData
                        averageIncome = int(
                            4.25*sum(list(zip(*iData))[1][0:10])/(10.))
                        averageIncome2 = int(4.25*sum(list(zip(*iData))[1][0:3])/3.)
                        averageIncome3 = int(4.25*iData[0][1])
                    if (custCredentials["data"]):
                        token = generate(dbw).AuthToken()
                        if token["updated"]:
                            output_dict["data"]["incomeData"] = utils.camelCase(income["data"])
                            output_dict["data"]["averageIncome"] = averageIncome
                            output_dict["data"]["averageIncome2"] = averageIncome2
                            output_dict["data"]["averageIncome3"] = averageIncome3
                            output_dict["data"]["custCredentials"] = utils.camelCase(
                                custCredentials["data"][0])
                            output_dict["data"]["custDetails"] = utils.camelCase(
                                custDetails["data"][0]) if custDetails["data"] else []
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                try:
                    resp.body = json.dumps(
                        output_dict, encoding='unicode-escape')
                except:
                    # encoding='unicode-escape')
                    resp.body = json.dumps(output_dict, ensure_ascii=False)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
