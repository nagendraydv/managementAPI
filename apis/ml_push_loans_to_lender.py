from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
import string
import os
import subprocess
import xlrd
import requests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, JoinType, Order
from dateutil.relativedelta import relativedelta
from pypika import functions as fn

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class PushLoanToLenderResource:

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"data": {"customerClientID": ""},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'pushLoansToLender'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='pushLoansToLender', request=input_dict):
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
                    custmap = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    aadhar = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    cvalues = Table(
                        "mw_finflux_attribute_code_values", schema="mint_loan")
                    panT = Table("mw_pan_status", schema="mint_loan")
                    emip = Table("mw_finflux_emi_packs_master",
                                 schema="mint_loan")
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    report = Table("mw_other_documents", schema="mint_loan")
                    chargeList = [{'amount': '1.7', 'chargeId': 1}, {
                        'amount': '0.306', 'chargeId': 9}]
                    today = datetime.now().strftime("%Y-%m-%d")
                    join = Query.from_(custcred).join(clientmaster, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(aadhar, how=JoinType.left)
                    join = join.on_field("CUSTOMER_ID").join(
                        profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.ACCOUNT_STATUS, custcred.FAIL_ATTEMPT,
                                    custcred.PIN_UPDATED_DATE, custcred.LAST_LOGIN, custcred.REGISTERED_IP_ADDRESS,
                                    custcred.LAST_LOGGED_IN_IP_ADDRESS, custcred.DEVICE_ID, custcred.CREATED_DATE, clientmaster.CLIENT_ID,
                                    clientmaster.FULL_NAME, clientmaster.LENDER, clientmaster.ACCOUNT_NO, clientmaster.FUND, aadhar.NAME,
                                    aadhar.DOB, aadhar.GENDER, aadhar.AADHAR_NO,
                                    fn.Concat(fn.NullIf(aadhar.HOUSE, ''), " ", fn.NullIf(aadhar.STREET, ''), " ", fn.NullIf(aadhar.LM, ''), " ",
                                              fn.NullIf(aadhar.LC, ''), " ", fn.NullIf(
                                                  aadhar.VTC, ''), " ", fn.NullIf(aadhar.DISTRICT, ''), " ",
                                              fn.NullIf(aadhar.POST_OFFICE, ''), " ", fn.NullIf(
                                                  aadhar.STATE, ''), " ",
                                              fn.NullIf(aadhar.PIN_CODE, '')).as_("ADDRESS"), profile.NAME.as_("PROFILE_NAME"))
                    junk = db.dictcursor.execute(db.pikastr(q.where(
                        custcred.CUSTOMER_ID == input_dict["data"]["customerID"])).replace("NULLIF", "IFNULL"))
                    Fields = db.dictcursor.fetchone()
                    fund = input_dict["data"]["fund"]
                    lender = "GETCLARITY"
                    urlKey = ("MIFOS_URL")
                    baseurl = db.runQuery(Query.from_(conf).select(
                        "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                    custID = str(input_dict["data"]["customerID"])
                    today = datetime.now().strftime("%d %B %Y")
                    temp = '26 February 2019'
                    Wednesday = (datetime.now(
                    ) + timedelta(days=(9 - datetime.now().weekday()))).strftime("%d %B %Y")
                    Friday = (datetime.now() + timedelta(days=(11 -
                                                               datetime.now().weekday()))).strftime("%d %B %Y")
                    nextMonth = (datetime.now() + timedelta(days=-datetime.now().day +
                                                            1) + relativedelta(months=1)).strftime("%d %B %Y")
                    prevMonth = (datetime.now() -
                                 relativedelta(months=1)).strftime("%d %B %Y")
                    updated = False
                    if Fields and custID != 0 and baseurl["data"]:
                        data = Fields  # ["data"][0]
                        baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                        headers = (utils.finflux_headers[lender].copy(
                        ) if lender in utils.finflux_headers else {})
                        # 'poonawalla' #'mintwalk'
                        headers['Fineract-Platform-TenantId'] = fund.lower()
                        auth = utils.mifos_auth
                        name = data["NAME"].rstrip(
                            " ") if data["NAME"] else data["PROFILE_NAME"].strip(" ")
                        ldata = db.runQuery(Query.from_(loanmaster).select(loanmaster.star).where((loanmaster.CUSTOMER_ID == custID) &
                                                                                                  (loanmaster.STATUS == "PENDING") &
                                                                                                  (loanmaster.LENDER == "GETCLARITY")))
                        loanDate = datetime.strptime(
                            ldata["data"][0]["LOAN_REQUEST_DATE"], "%Y-%m-%d").strftime("%d %B %Y")
                        if 'clientId' in locals():
                            del clientId
                        cexist = db.runQuery(Query.from_(loanmaster).select(loanmaster.star).where((loanmaster.CUSTOMER_ID == custID) &
                                                                                                   (loanmaster.FUND.like(fund+"%"))))["data"]  # 'MINTWALK'
                        if cexist == []:
                            utils.logger.debug(
                                "creating client for the fund", extra=logInfo)
                            payload = {"officeId": 1, "firstname": " ".join(name.split(" ")[0:-1]) if len(name.split(" ")) > 1 else name,
                                       "lastname": name.split(" ")[-1], "externalId": "000000" + custID, "dateFormat": "dd MMMM yyyy",
                                       "locale": "en", "active": True, "activationDate": loanDate, "submittedOnDate": loanDate}
                            utils.logger.debug("client creation url %s hit" % (
                                baseurl + "clients"), extra=logInfo)
                            utils.logger.debug(
                                "with following payload - " + json.dumps(payload), extra=logInfo)
                            #r = requests.post(
                                #baseurl + "clients", data=json.dumps(payload), headers=headers, auth=auth, verify=False)
                            #utils.logger.debug(
                                #"api response: " + json.dumps(r.json()), extra=logInfo)
                            #if 'clientId' in r.json():
                               # clientId = str(r.json()['clientId'])
                            #else:
                             #   clientId = None
                        else:
                            utils.logger.debug("client exist for the fund with client id: " + str(
                                cexist[0]["FUND_CLIENT_ID"]), extra=logInfo)
                            clientId = str(cexist[0]["FUND_CLIENT_ID"])
                        if (clientId if 'clientId' in locals() else False):
                            if ldata["data"]:
                                ldata = ldata["data"][0]
                                EMI = db.runQuery(Query.from_(emip).select("EMI", "LOAN_TERM", "AUTO_ID").where(
                                    (emip.LOAN_PRODUCT_ID == ldata["LOAN_PRODUCT_ID"]) & (emip.LOAN_AMOUNT == ldata["AMOUNT"])))["data"]
                                c = Query.from_(charges).select(charges.star).where(
                                    charges.PRODUCT_ID == str(ldata["LOAN_PRODUCT_ID"]))
                                c = c.where((charges.EMI_PACK_ID == (EMI[0]["AUTO_ID"] if EMI else 0)) | (
                                    charges.EMI_PACK_ID.isnull()))
                                chargeList = ([{"chargeId": ele2["CHARGE_ID"], "amount":ele2["ACTUAL_AMOUNT"]} for ele2 in db.runQuery(c)["data"]]
                                              if fund != "MINTWALK" else [{"chargeId": 1, "amount": 3}, {"chargeId": 2, "amount": 20}] if ldata["LOAN_PRODUCT_ID"] in ("5", 5) else [{"chargeId": 2, "amount": 90}] if ldata["LOAN_PRODUCT_ID"] in ("11", 11) else [{"chargeId": 2, "amount": 30}] if ldata["LOAN_PRODUCT_ID"] in ("9", 9) else [{"chargeId": 1, "amount": 2}, {"chargeId": 2, "amount": 30}] if ldata["LOAN_PRODUCT_ID"] in ("21", "24", "25", 21, 24, 25) else [{"chargeId": 1, "amount": 3}, {"chargeId": 2, "amount": 30}] if ldata["LOAN_PRODUCT_ID"] in (27, "27") else [{"chargeId": 2, "amount": 50}] if ldata["LOAN_PRODUCT_ID"] in (22, "22") else [{"chargeId": ele2["CHARGE_ID"], "amount":ele2["ACTUAL_AMOUNT"]} for ele2 in db.runQuery(c)["data"]] if ldata["LOAN_PRODUCT_ID"] in ("26", 26) else [{"chargeId": 1, "amount": 2}] if ldata["LOAN_PRODUCT_ID"] not in (16, "16") else [{"chargeId": 1, "amount": 1.25}])
                                if [x for x in chargeList if x["chargeId"] == 14]:
                                    filter(lambda x: x["chargeId"] == 14, chargeList)[
                                        0]["chargeId"]=4
                                if ([x for x in chargeList if x["chargeId"] == 12] != []) & ([x for x in chargeList if x["chargeId"] == 13] != []):
                                    filter(lambda x: x["chargeId"] == 12, chargeList)[0]["amount"]=str(sum(int(
                                        ele["amount"]) for ele in [x for x in chargeList if x["chargeId"] in (12, 13)]))
                                    filter(lambda x: x["chargeId"] == 12, chargeList)[
                                        0]["chargeId"]=3
                                    chargeList.remove(
                                        filter(lambda x: x["chargeId"] == 13, chargeList)[0])
                                q = Query.from_(loanprod).select("NUMBER_OF_REPAYMENTS", "REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE",
                                                                 "CHARGE_ID", "CHARGE_AMOUNT", "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                                 "AMORTIZATION_TYPE", "INTEREST_CALCULATION_PERIOD_TYPE",
                                                                 "INTEREST_RATE_PER_PERIOD", "INTEREST_TYPE",
                                                                 "TRANSACTION_PROCESSING_STRATEGY_ID", "NON_FEE_EMI")
                                prodInfo = db.runQuery(q.where((loanprod.PRODUCT_ID == ldata["LOAN_PRODUCT_ID"]) &
                                                               (loanprod.LENDER == "GETCLARITY")))
                                if prodInfo["data"]:
                                    prodInfo = prodInfo["data"][0]
                                    prodMap = ({"2": "2", 2: 2, "13": "5", 13: 5, "12": "3", 12: 3, "3": "6", 3: 6, "11": "4", 11: 4, "5": "6", 5: 6, "3": 6,
                                                3: 6} if fund != "MINTWALK" else {"2": "2", 2: 2, "12": "1", 12: 1, "5": "3", 5: 3, "16": "4", 16: 4, "11": "5", 11: 5, "9": "6", 9: 6, 22: 8, "22": "8", 21: 7, "21": "7", 27: 10, "27": "10", "26": "11", 26: 11, 24: 12, "24": "12", 25: 13, "25": "13"})
                                    payload = {"submittedOnDate": loanDate, "clientId": clientId, "loanPurposeId": 630,
                                               "productId": int(prodMap[ldata["LOAN_PRODUCT_ID"]]), "principal": ldata["AMOUNT"],
                                               "repaymentEvery": prodInfo["REPAY_EVERY"], "numberOfRepayments": EMI[0]["LOAN_TERM"] if EMI else prodInfo["NUMBER_OF_REPAYMENTS"],
                                               "loanTermFrequency": EMI[0]["LOAN_TERM"] if (EMI != []) and (ldata["LOAN_PRODUCT_ID"] not in ("16", 16)) else prodInfo["TERM_FREQUENCY"],
                                               "loanTermFrequencyType": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"],
                                               "repaymentFrequencyType": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"],
                                               "loanType": "individual", "locale": "en", "dateFormat": "dd MMMM yyyy",
                                               "amortizationType": prodInfo["AMORTIZATION_TYPE"], "interestType": prodInfo["INTEREST_TYPE"],
                                               "interestCalculationPeriodType": prodInfo["INTEREST_CALCULATION_PERIOD_TYPE"],
                                               "interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"],
                                               "transactionProcessingStrategyId": prodInfo["TRANSACTION_PROCESSING_STRATEGY_ID"],
                                               "allowPartialPeriodInterestCalcualtion": False, "expectedDisbursementDate": loanDate,
                                               "fixedEmiAmount": prodInfo["NON_FEE_EMI"] if ldata["LOAN_PRODUCT_ID"] not in ("9", 9, 14, "14", 16, "16", "26", 26) else EMI[0]["EMI"] if ldata["LOAN_PRODUCT_ID"] in ("14", 14) else EMI[0]["EMI"]-30 if ldata["LOAN_PRODUCT_ID"] in ("9", 9) else None, "charges": chargeList,
                                               "isEqualAmortization": False, "repaymentsStartingFromDate": Wednesday if ldata["LOAN_PRODUCT_ID"] not in (16, "16", 26, "26", 11, "11") else Friday, "fundId": "1",
                                               "amortizationType": prodInfo["AMORTIZATION_TYPE"], "interestType": prodInfo["INTEREST_TYPE"],
                                               "interestCalculationPeriodType": prodInfo["INTEREST_CALCULATION_PERIOD_TYPE"],
                                               "transactionProcessingStrategyId": prodInfo["TRANSACTION_PROCESSING_STRATEGY_ID"],
                                               "interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"]}
                                    payload = {
                                        k: v for k, v in payload.items() if v is not None}
                                    print(payload)
                                    utils.logger.debug("loan creation url %s hit" % (
                                        baseurl + "loans"), extra=logInfo)
                                    utils.logger.debug(
                                        "with following payload - " + json.dumps(payload), extra=logInfo)
                                    r = requests.post(
                                        baseurl + "loans", data=json.dumps(payload), headers=headers, auth=auth, verify=False)
                                    utils.logger.debug(
                                        "api response: " + json.dumps(r.json()), extra=logInfo)
                                    if 'resourceId' in r.json():
                                        updated = db.Update(db="mint_loan", table="mw_client_loan_master", checkAll=False, FUND=fund if fund != 'POONAWALLA' else 'POONAWALLA2',
                                                            FUND_CLIENT_ID=clientId, EXTERNAL_LOAN_ID=str(
                                                                r.json()["resourceId"]),
                                                            conditions={"ID=": str(ldata["ID"])})  # , debug=True)
                        if cexist == []:
                            try:
                                dob = datetime.strptime(
                                    Fields["data"][0]["DOB"], "%d-%m-%Y").strftime("%d %B %Y")
                            except:
                                dob = None
                            try:
                                q = Query.from_(cvalues).select(
                                    cvalues.VALUE_ID)
                                gender = db.runQuery(q.where(cvalues.VALUE_NAME == (
                                    "Female" if Fields["data"][0]["GENDER"] == 'F' else "Male")))
                                genderID = int(
                                    gender["data"][0]["VALUE_ID"]) if gender["data"] else None
                                payload = {"locale": "en", "dateFormat": "dd MMMM yyyy", "dateOfBirth": dob,  # "maritalStatusId":912,
                                           "genderId": genderID, "clientTypeId": 760, "clientClassificationId": 836,
                                           "mobileNo": data["LOGIN_ID"][-10:]}
                                r = requests.put(baseurl + "clients/" + clientId, headers=headers, auth=auth, verify=False,
                                                 data=json.dumps({k: v for k, v in payload.items() if v is not None}))
                                pan = Query.from_(panT).select("PAN_NO").where((panT.CUSTOMER_ID == Fields["data"][0]["CUSTOMER_ID"])
                                                                               & (panT.ARCHIVED == 'N')).orderby(panT.CREATED_DATE, order=Order.desc)
                                pan = db.runQuery(pan)
                                if pan["data"]:
                                    payload = {
                                        "documentTypeId": 776, "status": 200, "documentKey": pan["data"][0]["PAN_NO"]}
                                    payload["status"] = "Active"
                                    r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload), auth=auth,
                                                      headers=headers, verify=False)
                                if data["AADHAR_NO"]:
                                    payload = {"documentTypeId": 3, "status": 200,
                                               "documentKey": data["AADHAR_NO"].replace(" ", "")}
                                    payload["status"] = "Active"
                                    r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload), auth=auth,
                                                      headers=headers, verify=False)
                            except:
                                pass
                    if updated:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": "something went wrong"})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    utils.logger.debug(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
            raise
