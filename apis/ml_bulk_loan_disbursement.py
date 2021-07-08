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
import time
import requests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, Order
from six.moves import range
from six.moves import zip


class BulkLoanDisbursementUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def formatValue(self, y=None, md=None):
        if y.ctype == 2:
            return str(int(y.value))
        elif (y.ctype == 1 and "/" not in y.value):
            return y.value.replace("'", "")
        elif y.ctype == 3:
            return xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d")
        elif "/" in y.value:
            try:
                x = datetime.strptime(y.value, "%d/%m/%Y").strftime("%Y-%m-%d")
                return x
            except:
                return 0

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"processed": []}}
        errors = utils.errors
        success = "Income data imported successfully"
        #logInfo = {'api': 'incomeUpload'}
        try:
            data = {"docType": req.get_param(
                "docType"), "forceUpdate": req.get_param("forceUpdate")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            disbursalData = req.get_param("disbursalData")
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "disbursal_reports_upload/"
        # print data, data["forceUpdate"]=='1'
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (disbursalData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            # if (incomeData.filename.split('.')[-1]!="csv"):
            #    utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = disbursalData.filename.split('.')[-1]
                filename = disbursalData.filename  # self.setFilename() + suffix
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                s3 = session.resource('s3')
                junk = s3.meta.client.head_bucket(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(
                    falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(
                    token=msgHeader["authToken"])
                if val_error:
                    db._DbClose_()
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
                else:
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         UPLOAD_MODE="SmartDash Admin", DOCUMENT_STATUS='N', DOCUMENT_FOLDER=folder[:-1],
                                         CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_other_documents', fields={
                                     "A": ["AUTO_ID"]}, orderBy="AUTO_ID desc", limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(
                        bucket, folder + filename).put(Body=disbursalData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/disbursal." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/disbursal." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    # print d
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
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    emip = Table("mw_finflux_emi_packs_master",
                                 schema="mint_loan")
                    report = Table("mw_other_documents", schema="mint_loan")
                    chargeList = [{'amount': '1.7', 'chargeId': 1}, {
                        'amount': '0.306', 'chargeId': 9}]
                    mapping = {"loan id": "LOAN_ID", "applicant id": "CUSTOMER_ID", "payment benificiary name": "NAME",
                               "gross amount": "GROSS_AMOUNT", "date": "DATE", "remarks": "remarks"}
                    mapping2 = {"POONAWALLA": {"2": "2", 2: 2, "4": "2", 4: 2, "13": "5", 13: 5, "12": "3", 12: 3, "15": "4", 15: 4, "11": 4, 11: 4,
                                               "5": "6", 5: 6, "3": 6, 3: 6}, "MINTWALK": {"2": "2", 2: 2, "4": "2", 4: 2, "12": "1", 12: 1, "5": "3", 5: 3, "16": "4", 16: 4, 15: 5, "15": "5", "11": "5", 11: 5, "9": "6", 9: 6, "20": "6", 20: 6, "3": "3", 3: 3, 22: 8, "22": "8", 21: 7, "21": "7", 27: 10, "27": 10, "26": "11", 26: 11, "24": "12", 24: 12, "25": "13", 25: 13}}
                    ind, h = list(zip(*[(i, mapping[x.value.lower()])
                                        for i, x in enumerate(d[0]) if x.value.lower() in mapping]))
                    processed = []
                    loans = []
                    for i in range(1, len(d)):
                        Data = dict(
                            list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                        # print Data
                        urlKey = ("MIFOS_URL")
                        baseurl = db.runQuery(Query.from_(conf).select(
                            "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                        custID = str(Data["CUSTOMER_ID"])
                        loanID = str(Data["LOAN_ID"])
                        loans.append(
                            {"lender": "GETCLARITY", "loanId": loanID})
                        processed.append(
                            {"loanID": loanID, "modified": False, "approved": False, "disbursed": False})
                        if custID != 0 and baseurl["data"]:
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                            headers = (
                                utils.finflux_headers['GETCLARITY'].copy())
                            # print r, r.json()
                            auth = utils.mifos_auth
                            ldata = db.runQuery(Query.from_(loanmaster).select(loanmaster.star).where((loanmaster.LOAN_REFERENCE_ID == loanID) &
                                                                                                      (loanmaster.STATUS == "PENDING") &
                                                                                                      (loanmaster.LENDER == "GETCLARITY")))
                            EMI = db.runQuery(Query.from_(emip).select("EMI", "LOAN_TERM", "AUTO_ID").where(
                                (emip.LOAN_PRODUCT_ID == ldata["data"][0]["LOAN_PRODUCT_ID"]) & (emip.LOAN_AMOUNT == ldata["data"][0]["AMOUNT"])))["data"] if ldata else None
                            loanDate = datetime.strptime(ldata["data"][0]["LOAN_REQUEST_DATE"], "%Y-%m-%d").strftime(
                                "%d %B %Y") if ldata["data"] else datetime.now()
                            refDate = datetime.strptime(
                                Data["DATE"], "%Y-%m-%d")
                            approveDate = refDate.strftime("%d %B %Y")
                            Wednesday = (
                                refDate + timedelta(days=(9 - refDate.weekday()))).strftime("%d %B %Y")
                            Friday = (datetime.now(
                            ) + timedelta(days=(11 - datetime.now().weekday()))).strftime("%d %B %Y")
                            clientId = db.runQuery(Query.from_(clientmaster).select("CLIENT_ID").where((clientmaster.CUSTOMER_ID == custID) &
                                                                                                       (clientmaster.LENDER == 'GETCLARITY')))["data"]
                            clientId = clientId[0]["CLIENT_ID"] if clientId else None
                            if (clientId is not None) & (ldata["data"] != []):
                                ldata = ldata["data"][0]
                                c = Query.from_(charges).select(charges.star).where(
                                    charges.PRODUCT_ID == str(ldata["LOAN_PRODUCT_ID"]))
                                c = c.where((charges.EMI_PACK_ID == (EMI[0]["AUTO_ID"] if EMI else 0)) | (
                                    charges.EMI_PACK_ID.isnull()))
                                chargeList = [{"chargeId": ele2["CHARGE_ID"],
                                               "amount":ele2["ACTUAL_AMOUNT"]} for ele2 in db.runQuery(c)["data"]]
                                q = Query.from_(loanprod).select("NUMBER_OF_REPAYMENTS", "REPAY_EVERY", "REPAYMENT_PERIOD_FREQUENCY_TYPE",
                                                                 "CHARGE_ID", "CHARGE_AMOUNT", "TERM_FREQUENCY",
                                                                 "TERM_PERIOD_FREQUENCY_ENUM", "AMORTIZATION_TYPE",
                                                                 "INTEREST_CALCULATION_PERIOD_TYPE", "INTEREST_RATE_PER_PERIOD",
                                                                 "INTEREST_TYPE", "TRANSACTION_PROCESSING_STRATEGY_ID", "NON_FEE_EMI")
                                prodInfo = db.runQuery(q.where((loanprod.PRODUCT_ID == ldata["LOAN_PRODUCT_ID"]) &
                                                               (loanprod.LENDER == "GETCLARITY")))
                                mapp2 = {2: 4, 9: 20, 5: 3, 11: 15, 16: 18}
                                if prodInfo["data"]:
                                    prodInfo = prodInfo["data"][0]
                                    payload = {"submittedOnDate": loanDate, "clientId": clientId,  "loanPurposeId": 630,
                                               "productId": (4 if int(ldata["LOAN_PRODUCT_ID"]) == 2 else 20 if int(ldata["LOAN_PRODUCT_ID"]) == 9 else 3 if int(ldata["LOAN_PRODUCT_ID"]) == 5
                                                             else 15 if int(ldata["LOAN_PRODUCT_ID"]) == 11 else 18 if int(ldata["LOAN_PRODUCT_ID"]) == 16 else int(ldata["LOAN_PRODUCT_ID"])),
                                               "repaymentEvery": prodInfo["REPAY_EVERY"], "numberOfRepayments": EMI[0]["LOAN_TERM"] if EMI else prodInfo["NUMBER_OF_REPAYMENTS"],
                                               "loanTermFrequency": EMI[0]["LOAN_TERM"] if (EMI) and (ldata["LOAN_PRODUCT_ID"] not in ("16", 16)) else prodInfo["TERM_FREQUENCY"], "principal": ldata["AMOUNT"],
                                               "externalId": (ldata["FUND"].lower() +
                                                              ldata["EXTERNAL_LOAN_ID"]) if ((ldata["FUND"] is not None)) else None,  # &
                                               # (int(ldata["LOAN_PRODUCT_ID"]) not in (5,3))) else None,
                                               "loanTermFrequencyType": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"],
                                               "repaymentFrequencyType": prodInfo["REPAYMENT_PERIOD_FREQUENCY_TYPE"],
                                               "loanType": "individual", "locale": "en", "dateFormat": "dd MMMM yyyy",
                                               "amortizationType": prodInfo["AMORTIZATION_TYPE"], "interestType": prodInfo["INTEREST_TYPE"],
                                               "interestCalculationPeriodType": prodInfo["INTEREST_CALCULATION_PERIOD_TYPE"],
                                               "interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"],
                                               "transactionProcessingStrategyId": prodInfo["TRANSACTION_PROCESSING_STRATEGY_ID"],
                                               "allowPartialPeriodInterestCalcualtion": False, "expectedDisbursementDate": approveDate,
                                               "fixedEmiAmount": prodInfo["NON_FEE_EMI"] if ldata["LOAN_PRODUCT_ID"] not in ("20", 20, "9", 9, "14", 14, 16, "16", "26", 26) else EMI[0]["EMI"] if ldata["LOAN_PRODUCT_ID"] in ("14", 14) else EMI[0]["EMI"]-30 if ldata["LOAN_PRODUCT_ID"] in ("9", 9) else None, "charges": chargeList,
                                               "isEqualAmortization": False, "repaymentsStartingFromDate": Wednesday if ldata["LOAN_PRODUCT_ID"] not in (16, "16", 26, "26") else Friday,
                                               "fundId": "4" if ldata["FUND"] == 'POONAWALLA' else "5" if ldata["FUND"] == 'MINTWALK' else "1",
                                               "amortizationType": prodInfo["AMORTIZATION_TYPE"], "interestType": prodInfo["INTEREST_TYPE"],
                                               "interestCalculationPeriodType": prodInfo["INTEREST_CALCULATION_PERIOD_TYPE"],
                                               "transactionProcessingStrategyId": prodInfo["TRANSACTION_PROCESSING_STRATEGY_ID"],
                                               "interestRatePerPeriod": prodInfo["INTEREST_RATE_PER_PERIOD"]}
                                    payload = {
                                        k: v for k, v in payload.items() if v is not None}
                                    r = requests.put(baseurl + "loans/" + loanID, data=json.dumps(payload), headers=headers, auth=auth,
                                                     verify=False)
                                    # print r, r.json()
                                    if (ldata["FUND"] in ('POONAWALLA', 'MINTWALK')):
                                        if "externalId" in payload:
                                            payload["clientId"] = ldata["FUND_CLIENT_ID"]
                                            payload["fundId"] = 1
                                            payload["productId"] = mapping2[ldata["FUND"]
                                                                            ][payload["productId"]]
                                            p = payload.pop("externalId")
                                            p = payload.pop("charges")
                                        # 'poonawalla'
                                        headers["Fineract-Platform-TenantId"] = ldata["FUND"].lower()
                                        r = requests.put(baseurl + "loans/" + str(ldata["EXTERNAL_LOAN_ID"]), data=json.dumps(payload),
                                                         headers=headers, auth=auth, verify=False)
                                        # print r, r.json()
                                    headers["Fineract-Platform-TenantId"] = 'getclarity'
                                    if ('resourceId' in r.json() if r.status_code == 200 else False):
                                        processed[-1]["modified"] = True
                                        payload = {"approvedLoanAmount": ldata["AMOUNT"], "approvedOnDate": approveDate, "dateFormat": "dd MMMM yyyy",
                                                   "expectedDisbursementDate": approveDate, "locale": "en"}
                                        r = requests.post(baseurl + "loans/" + loanID + "?command=approve", data=json.dumps(payload),
                                                          headers=headers, auth=auth, verify=False)
                                        time.sleep(0.5)
                                        if ('resourceId' in r.json() if r.status_code == 200 else False):
                                            processed[-1]["approved"] = True
                                            payload = {"actualDisbursementDate": approveDate, "dateFormat": "dd MMMM yyyy", "locale": "en",
                                                       "paymentTypeId": "6" if ldata["FUND"] in ('MINTWALK', 'POONAWALLA') else "2",
                                                       "transactionAmount": ldata["AMOUNT"], "fixedEmiAmount": prodInfo["NON_FEE_EMI"] if ldata["LOAN_PRODUCT_ID"] not in ("20", 20, "9", 9, "14", 14, 16, "16", "26", 26) else EMI[0]["EMI"] if ldata["LOAN_PRODUCT_ID"] in ("14", 14) else EMI[0]["EMI"]-30 if ldata["LOAN_PRODUCT_ID"] in ("9", 9) else None}
                                            r = requests.post(baseurl + "loans/" + loanID + "?command=disburse", data=json.dumps(payload),
                                                              headers=headers, auth=auth, verify=False)
                                            time.sleep(0.5)
                                            if ('resourceId' in r.json() if r.status_code == 200 else False):
                                                processed[-1]["disbursed"] = True
                                                db.Update(db="mint_loan", table="mw_client_loan_master", checkAll=False, STATUS="ACTIVE",
                                                          conditions={"ID=": str(ldata["ID"])})
#                    r = requests.post("http://35.154.125.4:9091/v1/updateLoans", data=json.dumps(loans), headers={"Content-type":"application/json"}, verify=False)
                    db._DbClose_()
                    output_dict["data"]["processed"] = processed
                    output_dict["data"].update(
                        {"error": 0, "message": "successfully updated"})
                    resp.body = json.dumps(output_dict)
                    try:
                        r = requests.post("http://35.154.125.4:9091/v1/updateLoans", data=json.dumps(
                            loans), headers={"Content-type": "application/json"}, verify=False, timeout=1)
                    except:
                        pass
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
