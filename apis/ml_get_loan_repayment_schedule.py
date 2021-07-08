from __future__ import absolute_import
import falcon
import json
import requests
import os
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, functions
from dateutil.relativedelta import relativedelta


class GetLoanRepaymentScheduleResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"customerLoanDetails": ""},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'getLoanRepaymentSchedule'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='loanApplicationRequest', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB()  # input_dict["msgHeader"]["authLoginID"])
                # val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID = input_dict["msgHeader"]["authLoginID"]
                #                                  checkLogin=True)
                if False:  # val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    today = datetime.now().strftime("%d %B %Y")
                    Wednesday = (datetime.now(
                    ) + timedelta(days=(9 - datetime.now().weekday()))).strftime("%d %B %Y")
                    Friday = (datetime.now() + timedelta(days=(11 -
                                                               datetime.now().weekday()))).strftime("%d %B %Y")
                    nextMonth = (datetime.now() + timedelta(days=-datetime.now().day +
                                                            1) + relativedelta(months=1)).strftime("%d %B %Y")
                    custID = input_dict['data']['customerID']
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    emip = Table("mw_finflux_emi_packs_master",
                                 schema="mint_loan")
                    join = Query.from_(custcred).join(clientmaster, how=JoinType.left).on_field(
                        "CUSTOMER_ID").select(clientmaster.CLIENT_ID)
                    lender = ("GETCLARITY" if (input_dict["data"]["lender"] == "GETCLARITY" if "lender" in input_dict["data"] else False)
                              else "CHAITANYA")
                    urlKey = ("MIFOS_URL" if lender ==
                              "GETCLARITY" else "FINFLUX_URL")
                    clientID = db.runQuery(join.where(
                        (custcred.CUSTOMER_ID == custID) & (clientmaster.LENDER == lender)))
                    clientID = clientID if clientID["data"] else {
                        "data": [{"CLIENT_ID": "1"}]}
                    if clientID["data"]:
                        if lender == "GETCLARITY":
                            headers = (
                                utils.finflux_headers[lender] if lender in utils.finflux_headers else {})
                            auth = utils.mifos_auth
                        else:
                            tokenKey = "MintwalkFinfluxAccessToken" if lender == "GETCLARITY" else "FinfluxAccessToken"
                            params = db.runQuery(Query.from_(conf2).select(
                                "CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin([tokenKey])))
                            params = {
                                "FinfluxAccessToken": ele["CONFIG_VALUE"] for ele in params["data"]}
                            headers = utils.mergeDicts((utils.finflux_headers[lender] if lender in utils.finflux_headers else {}),
                                                       {"Authorization": "bearer " + params["FinfluxAccessToken"]})
                        q = Query.from_(loanprod).select("NUMBER_OF_REPAYMENTS", "REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE", "CHARGE_ID",
                                                         "CHARGE_AMOUNT", "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM", "AMORTIZATION_TYPE",
                                                         "INTEREST_CALCULATION_PERIOD_TYPE", "INTEREST_RATE_PER_PERIOD", "INTEREST_TYPE",
                                                         "TRANSACTION_PROCESSING_STRATEGY_ID", "NON_FEE_EMI")
                        prodInfo = db.runQuery(q.where(
                            (loanprod.PRODUCT_ID == input_dict["data"]["loanProductID"]) & (loanprod.LENDER == lender)))
                        baseurl = db.runQuery(Query.from_(conf).select(
                            "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                        if prodInfo["data"] and baseurl["data"]:
                            prodInfo = prodInfo["data"][0]
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                            EMI = db.runQuery(Query.from_(emip).select("EMI", "LOAN_TERM").where(
                                (emip.LOAN_PRODUCT_ID == input_dict["data"]["loanProductID"]) & (emip.LOAN_AMOUNT == input_dict["data"]["loanAmount"])))["data"]
                            emiPackID = ((input_dict["data"]["loanEMIPackId"] if input_dict["data"]["loanEMIPackId"] != "651" else "0")
                                         if "loanEMIPackId" in input_dict["data"] else "0")
                            c = Query.from_(charges).select(charges.star).where(
                                charges.PRODUCT_ID == str(input_dict["data"]["loanProductID"]))
                            if True:  # lender=="GETCLARITY":
                                chargeList = [{"chargeId": ele["CHARGE_ID"], "amount":ele["ACTUAL_AMOUNT"]}
                                              for ele in db.runQuery(c)["data"]]
                            else:
                                chargeList = [{"chargeId": ele["CHARGE_ID"], "amount":ele["ACTUAL_AMOUNT"], "locale":"en",
                                               "dateFormat":"dd MMMM yyyy"} for ele in db.runQuery(c)["data"]]
                            payload = {"submittedOnDate": today, "clientId": clientID["data"][0]["CLIENT_ID"],
                                       "loanProductId": int(input_dict["data"]["loanProductID"]), "repayEvery": prodInfo["REPAY_EVERY"],
                                       "loanAmountRequested": input_dict["data"]["loanAmount"],
                                       "loanPurposeId": ((794 if input_dict["data"]["loanAmount"] == 5500 else 630) if lender == "GETCLARITY" else 319),
                                       # "loanEMIPackId": (emiPackID if emiPackID not in ["0", "1", "2", "3", "6", "14", "651"] else None),
                                       "numberOfRepayments": EMI[0]["LOAN_TERM"] if EMI else prodInfo["NUMBER_OF_REPAYMENTS"], "termFrequency": EMI[0]["LOAN_TERM"] if (EMI != []) and (input_dict["data"]["loanProductID"] not in ("16", 16)) else prodInfo["TERM_FREQUENCY"],
                                       "termPeriodFrequencyEnum": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"], "charges": chargeList,
                                       "repaymentPeriodFrequencyEnum": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"], "accountType": "individual",
                                       "expectedDisbursalPaymentType": 1 if lender == "GETCLARITY" else 751, "dateFormat": "dd MMMM yyyy",
                                       "expectedRepaymentPaymentType": 1 if lender == "GETCLARITY" else 751,  "locale": "en",
                                       "amortizationType": prodInfo["AMORTIZATION_TYPE"], "interestType": prodInfo["INTEREST_TYPE"],
                                       "interestCalculationPeriodType": prodInfo["INTEREST_CALCULATION_PERIOD_TYPE"],
                                       "interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"],
                                       "transactionProcessingStrategyId": prodInfo["TRANSACTION_PROCESSING_STRATEGY_ID"]}
                            if True:  # lender=="GETCLARITY":
                                emi = "470" if payload["loanAmountRequested"] in (
                                    5500, "5500") else "855" if payload["loanAmountRequested"] in (2500, "2500") else None
                                payload["loanType"] = payload.pop(
                                    "accountType")
                                payload["productId"] = payload.pop(
                                    "loanProductId")
                                payload["loanTermFrequency"] = payload.pop(
                                    "termFrequency")
                                payload["loanTermFrequencyType"] = payload.pop(
                                    "termPeriodFrequencyEnum")
                                payload["repaymentFrequencyType"] = payload.pop(
                                    "repaymentPeriodFrequencyEnum")
                                payload["principal"] = payload.pop(
                                    "loanAmountRequested")
                                payload["repaymentEvery"] = payload.pop(
                                    "repayEvery")
                                junk = payload.pop(
                                    "expectedRepaymentPaymentType")
                                junk = payload.pop(
                                    "expectedDisbursalPaymentType")
                                payload.update({"allowPartialPeriodInterestCalcualtion": False, "expectedDisbursementDate": today,
                                                "fixedEmiAmount": (prodInfo["NON_FEE_EMI"] if payload["productId"] not in ("9", 9, "14", 14, 16, "16",
                                                                                                                           "26", 26) else EMI[0]["EMI"] if payload["productId"] in ("14", 14) else EMI[0]["EMI"]-30 if payload["productId"] in ("9", 9) else None) if lender == "GETCLARITY" else None, "isEqualAmortization": False if lender == "GETCLARITY" else None,
                                                "repaymentsStartingFromDate": ((Wednesday if payload["productId"] not in (16, "16") else Friday) if payload["repaymentFrequencyType"] != 2 else
                                                                               nextMonth), "fundId": "1"})
                            payload = {k: v for k,
                                       v in payload.items() if v is not None}
                            utils.logger.info("FINFLUX api URL: " + baseurl + ("loans" if lender == "GETCLARITY"
                                                                               else "loanapplicationreferences"), extra=logInfo)
                            utils.logger.info(
                                "api request: " + json.dumps(payload), extra=logInfo)
                            if lender == "GETCLARITY":
                                r = requests.post(baseurl + "loans?command=calculateLoanSchedule", data=json.dumps(payload), headers=headers,
                                                  auth=auth, verify=False)
                            else:
                                headers["Fineract-Platform-TenantId"] = "chaitanya"
                                r = requests.post(baseurl + "loans?command=calculateLoanSchedule", data=json.dumps(payload), headers=headers,
                                                  verify=False)
                            # print payload, baseurl + "loans?command=calculateLoanSchedule", headers, r.status_code
                            utils.logger.info(
                                "api response: " + json.dumps(r.json()), extra=logInfo)
                            if True:
                                if True:  # inserted:
                                    token = generate(
                                        db).AuthToken(update=False)
                                    if True:  # token["updated"]:
                                        output_dict["data"].update({"customerLoanDetails": utils.mergeDicts(
                                            {"interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"]}, r.json())})
                                        output_dict["data"].update(
                                            {"error": 0 if ("errors" not in r.json()) else 1, "message": success})
                                        output_dict["msgHeader"]["authToken"] = token["token"]
                                    else:
                                        output_dict["data"].update(
                                            {"error": 1, "message": errors["token"]})
                                else:
                                    output_dict["data"].update(
                                        {"error": 1, "message": errors["query"]})
                            else:
                                output_dict["data"].update({"error": 1,
                                                            "message": r.json()['defaultUserMessage'] if 'defaultUserMessage' in r.json()
                                                            else "some error occured"})
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": "Product data not available"})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": "Customer not registered with the lender - %s" % lender})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
