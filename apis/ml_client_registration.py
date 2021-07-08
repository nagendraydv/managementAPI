from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, functions, Order
import requests
from dateutil.relativedelta import relativedelta


class ClientRegisterResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"customerClientID": ""},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "customer registered successfully"
        logInfo = {'api': 'clientRegistration'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='clientRegister2', request=input_dict):
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
                    pan = Table("mw_pan_status", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    emip = Table("mw_finflux_emi_packs_master",
                                 schema="mint_loan")
                    join = Query.from_(custcred).join(clientmaster, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(aadhar, how=JoinType.left)
                    join = join.on_field("CUSTOMER_ID").join(
                        profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.ACCOUNT_STATUS, custcred.FAIL_ATTEMPT, custcred.PIN_UPDATED_DATE,
                                    custcred.LAST_LOGIN, custcred.REGISTERED_IP_ADDRESS, custcred.LAST_LOGGED_IN_IP_ADDRESS, custcred.DEVICE_ID,
                                    custcred.CREATED_DATE, clientmaster.CLIENT_ID, clientmaster.FULL_NAME, clientmaster.LENDER,
                                    clientmaster.ACCOUNT_NO, clientmaster.ACTIVATION_DATE, aadhar.NAME, aadhar.DOB, aadhar.GENDER, aadhar.AADHAR_NO,
                                    functions.Concat(aadhar.HOUSE, " ", aadhar.STREET, " ", aadhar.LM, " ", aadhar.LC, " ", aadhar.VTC, " ", aadhar.DISTRICT,
                                                     " ", aadhar.POST_OFFICE, " ", aadhar.STATE, " ", aadhar.PIN_CODE).as_("ADDRESS"),
                                    profile.NAME.as_("PROFILE_NAME"))
                    q = q.where(custcred.CUSTOMER_ID ==
                                input_dict['data']['customerID'])
                    Fields = db.runQuery(q)
                    lender = ("GETCLARITY" if (input_dict["data"]["lender"] == "GETCLARITY" if "lender" in input_dict["data"] else False)
                              else "CHAITANYA")
                    urlKey = ("MIFOS_URL" if lender == "GETCLARITY" else "FINFLUX_URL")
                    baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                    custID = input_dict['data']['customerID']
                    today2 = datetime.now().strftime("%d %B %Y")
                    Wednesday = (datetime.now() + timedelta(days=(9 - datetime.now().weekday()))).strftime("%d %B %Y")
                    Friday = (datetime.now() + timedelta(days=(11 - datetime.now().weekday()))).strftime("%d %B %Y")
                    if Fields["data"] and custID != 0 and baseurl["data"]:
                        if (not [x for x in Fields["data"] if x["LENDER"] == input_dict["data"]["lender"]]):
                            today = datetime.strptime(Fields["data"][0]["CREATED_DATE"], "%Y-%m-%d %H:%M:%S").strftime("%d %B %Y") if datetime.strptime(
                                Fields["data"][0]["CREATED_DATE"], "%Y-%m-%d %H:%M:%S") > datetime(2018, 9, 1) else datetime(2018, 9, 1).strftime("%d %B %Y")
                            data = Fields["data"][0]
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                            if lender == "GETCLARITY":
                                #today2 = datetime.now().strftime("%d %B %Y")
                                headers = (utils.finflux_headers[lender].copy(
                                ) if lender in utils.finflux_headers else {})
                                headers['Fineract-Platform-TenantId'] = 'getclarity'
                                headers2 = utils.finflux_headers["PURSHOTTAM"].copy(
                                ) if "PURSHOTTAM" in utils.finflux_headers else {}
                                auth = utils.mifos_auth
                            else:
                                tokenKey = "MintwalkFinfluxAccessToken" if lender == "GETCLARITY" else "FinfluxAccessToken"
                                params = Query.from_(conf2).select("CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin([tokenKey]))
                                params = {"FinfluxAccessToken": ele["CONFIG_VALUE"] for ele in db.runQuery(params)["data"]}
                                headers = utils.mergeDicts((utils.finflux_headers[lender].copy() if lender in utils.finflux_headers else {}),
                                                           {"Authorization": "bearer " + params["FinfluxAccessToken"]})
                                print(data)
                            name = data["NAME"].rstrip(" ").lstrip(" ") if data["NAME"] else data["PROFILE_NAME"]#.lstrip(" ")
                            payload = {"officeId": (1 if lender == "GETCLARITY" else 198),
                                       "firstname": " ".join(name.split(" ")[0:-1]) if len(name.split(" ")) > 1 else name,
                                       "lastname": name.split(" ")[-1], "externalId": "000000" + custID, "dateFormat": "dd MMMM yyyy",
                                       "locale": "en", "active": True, "activationDate": today, "submittedOnDate": today}
                            utils.logger.info(
                                "FINFLUX api POST URL: " + baseurl + "clients", extra=logInfo)
                            utils.logger.info(
                                "api request: " + json.dumps(payload), extra=logInfo)
                            if lender == "GETCLARITY":
                                r = requests.post(
                                    baseurl + "clients", data=json.dumps(payload), headers=headers, auth=auth, verify=False)
                                # requests.post(baseurl + "clients", data=json.dumps(payload), headers=headers2, auth=auth, verify=False)
                                rp = None
                                #utils.logger.info("purshottam api response: " + json.dumps(rp.json()), extra=logInfo)
                            else:
                                r = requests.post(
                                    baseurl + "clients", data=json.dumps(payload), headers=headers, verify=False)
                                rp = None
                            utils.logger.info(
                                "api response: " + json.dumps(r.json()), extra=logInfo)
                            if 'clientId' in r.json():
                                clientId = str(r.json()['clientId'])
                                clientId2 = str(
                                    rp.json()['clientId']) if rp else None
                                inserted = db.Insert(db='mint_loan', table='mw_finflux_client_master', CUSTOMER_ID=custID,
                                                     # FUND=('PURSHOTTAM' if rp else None), FUND_CLIENT_ID=clientId2,
                                                     LENDER=lender, CLIENT_ID=clientId, FULL_NAME=payload[
                                                         'firstname'] + " " + payload['lastname'],
                                                     ACTIVE='1', ACTIVATION_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), CREATED_BY='Admin',
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), date=False, compulsory=False)
                                ldata = db.runQuery(Query.from_(loanmaster).select(loanmaster.star).where((loanmaster.CUSTOMER_ID == custID) &
                                                                                                          (loanmaster.STATUS == "REQUESTED") &
                                                                                                          (loanmaster.LENDER == lender)))
                                if ldata["data"]:
                                    ldata = ldata["data"][0]
                                    q = Query.from_(loanprod).select("NUMBER_OF_REPAYMENTS", "REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE",
                                                                     "CHARGE_ID", "CHARGE_AMOUNT", "TERM_FREQUENCY", "TERM_PERIOD_FREQUENCY_ENUM",
                                                                     "AMORTIZATION_TYPE", "INTEREST_CALCULATION_PERIOD_TYPE",
                                                                     "INTEREST_RATE_PER_PERIOD", "INTEREST_TYPE",
                                                                     "TRANSACTION_PROCESSING_STRATEGY_ID", "NON_FEE_EMI")
                                    prodInfo = db.runQuery(q.where(
                                        (loanprod.PRODUCT_ID == ldata["LOAN_PRODUCT_ID"]) & (loanprod.LENDER == lender)))
                                    if prodInfo["data"]:
                                        prodInfo = prodInfo["data"][0]
                                        EMI = db.runQuery(Query.from_(emip).select("EMI", "LOAN_TERM", "AUTO_ID").where(
                                            (emip.LOAN_PRODUCT_ID == ldata["LOAN_PRODUCT_ID"]) & (emip.LOAN_AMOUNT == ldata["AMOUNT"])))["data"]
                                        c = Query.from_(charges).select(
                                            charges.star).where(charges.LENDER == lender)
                                        c = c.where(
                                            charges.PRODUCT_ID == ldata["LOAN_PRODUCT_ID"])
                                        c = c.where((charges.EMI_PACK_ID == (EMI[0]["AUTO_ID"] if EMI else 0)) | (
                                            charges.EMI_PACK_ID.isnull()))
                                        if lender == "GETCLARITY":
                                            chargeList = [{"chargeId": ele["CHARGE_ID"], "amount":ele["ACTUAL_AMOUNT"]}
                                                          for ele in db.runQuery(c)["data"]]
                                        else:
                                            chargeList = [{"chargeId": ele["CHARGE_ID"], "amount":ele["ACTUAL_AMOUNT"], "locale":"en",
                                                           "dateFormat":"dd MMMM yyyy"} for ele in db.runQuery(c)["data"]]

                                        payload = {"submittedOnDate": today2, "clientId": clientId, "loanProductId": int(ldata["LOAN_PRODUCT_ID"]),
                                                   "loanAmountRequested": ldata["AMOUNT"], "loanPurposeId": 794 if lender == "GETCLARITY" else 319,
                                                   # "loanEMIPackId": ldata["EMI_PACK_ID"] if (ldata["EMI_PACK_ID"] not in
                                                   #                                          ["", "0", "1", "2", "3", "5", "6", "651"]) else None,
                                                   "repayEvery": prodInfo["REPAY_EVERY"], "numberOfRepayments": EMI[0]["LOAN_TERM"] if EMI else prodInfo["NUMBER_OF_REPAYMENTS"],
                                                   "termFrequency": EMI[0]["LOAN_TERM"] if (EMI != []) & (ldata["LOAN_PRODUCT_ID"] not in ("16", 16)) else prodInfo["TERM_FREQUENCY"],
                                                   "termPeriodFrequencyEnum": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"],
                                                   "repaymentPeriodFrequencyEnum": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"],
                                                   "expectedDisbursalPaymentType": 1 if lender == "GETCLARITY" else 751,
                                                   "expectedRepaymentPaymentType": 1 if lender == "GETCLARITY" else 751, "charges": chargeList,
                                                   "accountType": "individual", "locale": "en", "dateFormat": "dd MMMM yyyy"}
                                        if lender == "GETCLARITY":
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
                                            payload.update({"allowPartialPeriodInterestCalcualtion": False, "expectedDisbursementDate": today2,
                                                            "fixedEmiAmount": prodInfo["NON_FEE_EMI"] if payload["productId"] not in ("9", 9, 14, "14", 16, "16", "26", 26) else EMI[0]["EMI"] if payload["productId"] in ("14", 14) else EMI[0]["EMI"]-30 if payload["productId"] in ("9", 9) else None,
                                                            "isEqualAmortization": False, "repaymentsStartingFromDate": Wednesday if payload["productId"] not in (16, "16") else Friday, "fundId": "1" if payload["productId"] in (5, "5", 16, "16") else "4",
                                                            "amortizationType": prodInfo["AMORTIZATION_TYPE"], "interestType": prodInfo["INTEREST_TYPE"],
                                                            "interestCalculationPeriodType": prodInfo["INTEREST_CALCULATION_PERIOD_TYPE"],
                                                            "transactionProcessingStrategyId": prodInfo["TRANSACTION_PROCESSING_STRATEGY_ID"],
                                                            "interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"]})
                                        payload = {
                                            k: v for k, v in payload.items() if v is not None}
                                        utils.logger.info("FINFLUX api POST URL: " + baseurl + ("loans" if lender == "GETCLARITY"
                                                                                                else "loanapplicationreferences"), extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        if lender == "GETCLARITY":
                                            # if payload["productId"] in (5, "5"):
                                            #    Charges = payload.pop("charges")
                                            #    loanPurposeId = payload["loanPurposeId"]
                                            #    payload["loanPurposeId"] = "630"
                                            #    payload["clientId"]=clientId2
                                            #    payload["fundId"] = "1"
                                            #    rp = requests.post(baseurl + "loans", data=json.dumps(payload), headers=headers2, auth=auth,
                                            #                      verify=False)
                                            #    extId = str(rp.json()["resourceId"])
                                            #    utils.logger.info("pushing loan to purshottam", extra=logInfo)
                                            #    utils.logger.info("api request: " + json.dumps(payload), extra=logInfo)
                                            #    utils.logger.info("api response: " + json.dumps(rp.json()), extra=logInfo)
                                            #    payload.update({"charges":Charges, "loanPurposeId":loanPurposeId, "clientId":clientId})
                                            #    payload.update({"fundId":"3", #"externalId":"purshottam0" + extId, "fundId":"3",
                                            #                    "productId":(4 if payload["productId"]==2 else 3 if payload["productId"]==5 else
                                            #                                 payload["productId"])} if "resourceId" in rp.json() else {})
                                            #    utils.logger.info("modified api request: " + json.dumps(payload), extra=logInfo)
                                            # else:
                                            #    rp, extId = (None, None)
                                            r = requests.post(baseurl + "loans", data=json.dumps(payload), headers=headers, auth=auth,
                                                              verify=False)
                                        else:
                                            r = requests.post(baseurl + "loanapplicationreferences", data=json.dumps(payload), headers=headers,
                                                              verify=False)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                        if 'resourceId' in r.json():
                                            if lender == "GETCLARITY":
                                                r2 = requests.get(baseurl + "loans/" + str(r.json()['resourceId']), headers=headers, auth=auth,
                                                                  data=json.dumps({}), verify=False)
                                            else:
                                                r2 = requests.get(baseurl + "loanapplicationreferences/" + str(r.json()['resourceId']),
                                                                  headers=headers, data=json.dumps({}), verify=False)
                                            resp2 = r2.json()
                                            updated = db.Update(db='mint_loan', table='mw_client_loan_master', checkAll=False,
                                                                # FUND=('PURSHOTTAM' if rp else None), EXTERNAL_LOAN_ID=(extId if rp else None),
                                                                LOAN_REFERENCE_ID=str(r.json()['resourceId']), DEBIT_TYPE='DIRECT_DEBIT',
                                                                LOAN_ACCOUNT_NO=None if lender != "GETCLARITY" else str(r.json()[
                                                                                                                        'resourceId']),
                                                                STATUS="PENDING", LOAN_PRODUCT_ID=ldata["LOAN_PRODUCT_ID"],
                                                                LOAN_APPLICATION_NO=(str(resp2["loanApplicationReferenceNo"])
                                                                                     if lender != "GETCLARITY" else None),
                                                                conditions={"ID = ": str(ldata["ID"])})
                                            input_dict = {"LOAN_MASTER_ID": str(ldata["ID"]),
                                                          "PRINCIPAL": (str(resp2["principal"]) if lender == "GETCLARITY"
                                                                        else str(resp2["loanAmountRequested"])),
                                                          "TERM_FREQUENCY": str(resp2["termFrequency"]),
                                                          "STATUS": resp2["status"]["value"], "LOAN_TYPE": "Individual", "CREATED_BY": "CRON",
                                                          "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                                            check = Query.from_(loandetails).select(
                                                "LOAN_MASTER_ID")
                                            check = db.runQuery(check.where(
                                                loandetails.LOAN_MASTER_ID == str(ldata["ID"])))
                                            if not check["data"]:
                                                db.Insert(db="mint_loan", table="mw_client_loan_details", compulsory=False, date=False,
                                                          **input_dict)
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
                                    utils.logger.info(
                                        "FINFLUX api PUT URL: " + baseurl + "clients/" + clientId, extra=logInfo)
                                    utils.logger.info(
                                        "api request: " + json.dumps(payload), extra=logInfo)
                                    if lender == "GETCLARITY":
                                        r = requests.put(baseurl + "clients/" + clientId, headers=headers, auth=auth, verify=False,
                                                         data=json.dumps({k: v for k, v in payload.items() if v is not None}))
                                        r = requests.put(baseurl + "clients/" + clientId2, headers=headers2, auth=auth, verify=False,
                                                         data=json.dumps({k: v for k, v in payload.items() if v is not None}))
                                    else:
                                        r = requests.put(baseurl + "clients/" + clientId, headers=headers, verify=False,
                                                         data=json.dumps({k: v for k, v in payload.items() if v is not None}))
                                    utils.logger.info(
                                        "api response: " + json.dumps(r.json()), extra=logInfo)
                                    pan = Query.from_(pan).select("PAN_NO").where((pan.CUSTOMER_ID == Fields["data"][0]["CUSTOMER_ID"])
                                                                                  & (pan.ARCHIVED == 'N')).orderby(pan.CREATED_DATE, order=Order.desc)
                                    pan = db.runQuery(pan)
                                    if pan["data"]:
                                        payload = {"documentTypeId": 776, "status": 200, "documentKey": pan["data"][0]["PAN_NO"].replace(
                                            " ", "").strip()}
                                        utils.logger.info("FINFLUX api POST URL: " + baseurl + "clients/" + clientId + "/identifiers",
                                                          extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        if lender == "GETCLARITY":
                                            payload["status"] = "Active"
                                            r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload), auth=auth,
                                                              headers=headers, verify=False)
                                            r = requests.post(baseurl + "clients/" + clientId2 + "/identifiers", data=json.dumps(payload), auth=auth,
                                                              headers=headers2, verify=False)
                                        else:
                                            r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload),
                                                              headers=headers, verify=False)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                    if data["AADHAR_NO"]:
                                        payload = {"documentTypeId": 3, "status": 200, "documentKey": data["AADHAR_NO"].replace(
                                            " ", "").strip()}
                                        utils.logger.info(
                                            "FINFLUX api POST URL: " + baseurl + "clients/" + clientId + "/identifiers", extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        if lender == "GETCLARITY":
                                            payload["status"] = "Active"
                                            r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload), auth=auth,
                                                              headers=headers, verify=False)
                                            r = requests.post(baseurl + "clients/" + clientId2 + "/identifiers", data=json.dumps(payload), auth=auth,
                                                              headers=headers2, verify=False)
                                        else:
                                            r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload),
                                                              verify=False, headers=headers)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                    bankDetails = db.runQuery(Query.from_(custbank).select(
                                        custbank.star).where(custbank.CUSTOMER_ID == custID))
                                    if bankDetails["data"] and lender != "GETCLARITY":
                                        bDetails = bankDetails["data"][0]
                                        name = (bDetails["ACCOUNT_HOLDER_NAME"] if bDetails["ACCOUNT_HOLDER_NAME"] else data["NAME"].rstrip(" ")
                                                if data["NAME"] else data["PROFILE_NAME"].strip(" "))
                                        payload = {"name": name, "accountNumber": bDetails["ACCOUNT_NO"], "ifscCode": bDetails["IFSC_CODE"],
                                                   "mobileNumber": data["LOGIN_ID"][-10:], "bankName": bDetails["BANK_NAME"][0:20],
                                                   "bankCity": bDetails["CITY"][0:18], "branchName": bDetails["BRANCH"], "accountTypeId": 1,
                                                   "lastTransactionDate": today, "locale": "en", "dateFormat": "dd MMMM yyyy"}
                                        utils.logger.info("FINFLUX api POST URL: " + baseurl + "clients/" + clientId + "/bankaccountdetail",
                                                          extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        r = requests.post(baseurl + "clients/" + clientId + "/bankaccountdetail", data=json.dumps(payload),
                                                          headers=headers, verify=False)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                except:
                                    pass
                                if inserted:
                                    token = generate(db).AuthToken()
                                    if token["updated"]:
                                        output_dict["data"].update(
                                            {"customerClientID": clientId})
                                        output_dict["data"].update(
                                            {"error": 0, "message": success})
                                        output_dict["msgHeader"]["authToken"] = token["token"]
                                    else:
                                        output_dict["data"].update(
                                            {"error": 1, "message": errors["token"]})
                                else:
                                    output_dict["data"].update({"error": 1, "message": errors["query"]})
                            else:
                                output_dict["data"].update({"error": 1,
                                                            "message": r.json()['defaultUserMessage'] if 'defaultUserMessage' in r.json()
                                                            else "some error occured"})
                        else:
                            output_dict["data"].update({"error": 1, "message": "Customer already registered or Aadhar KYC not done"})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": "No data found for the customer"})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
