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


class GrandLoanDisburseUploadResource:

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
                x = datetime.strptime(y.value, "%Y/%m/%d").strftime("%Y-%m-%d")
                return x
            except:
                return 0

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"processed": []}}
        errors = utils.errors
        success = "loan disbursed successfully"
        logInfo = {'api': 'GrandLoan'}
        try:
            data = {"docType": req.get_param("docType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            disbursalData = req.get_param("disbursalData")
            utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader}), extra=logInfo)
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "disbursal_reports_upload/"
        if ((not validate.Request(api='disbursalDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (disbursalData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = disbursalData.filename.split('.')[-1]
                filename = disbursalData.filename  # self.setFilename() + suffix
                s3path = s3url + bucket + '/' + folder + filename
                #session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                 #                       aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                #s3 = session.resource('s3')
                #junk = s3.meta.client.head_bucket(Bucket=bucket)
                s3=utils().s3_connect()
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(token=msgHeader["authToken"])
                if val_error:
                    db._DbClose_()
                    output_dict["data"].update({"error": 1, "message": val_error})
                    utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
                elif db.Query(primaryTable="mw_other_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] != 0:
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
                    junk = s3.Object(bucket, folder + filename).put(Body=disbursalData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/disbursal." + suffix)
                    md = xlrd.open_workbook(filename="/tmp/disbursal." + suffix, encoding_override='unicode-escape')
                    #md = xlrd.open_workbook(filename="/home/nagendra/Desktop/book." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    #custmap = Table("mw_customer_login_credentials_map", schema="mint_loan")
                    #clientmaster = Table("mw_finflux_client_master", schema="mint_loan")
                    #profile = Table("mw_client_profile", schema="mint_loan")
                    #aadhar = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    loanmaster = Table("mw_client_loan_master", schema="mint_loan")
                    mapping = {"ID":"ID","loanID": "LOAN_ID", "customerID": "CUSTOMER_ID", "interestRate": "INTEREST_RATE",
                               "amount": "AMOUNT", "expectedMaturityDate": "EXPECTED_MATURITY_DATE", "numberOfRepayments": "NUMBER_OF_REPAYMENT","repaymentEvery":"REPAYMENT_EVERY",
                               "expectedDisbursementDate":"EXPECTED_DISBURSEMENT_DATE","termFrequency":"TERM_FREQUENCY","termFrequencyEnum":"TERM_FREQUENCY_ENUM"}
                    #mapping2 = {"POONAWALLA": {"2": "2", 2: 2, "4": "2", 4: 2, "13": "5", 13: 5, "12": "3", 12: 3, "15": "4", 15: 4, "11": 4, 11: 4,
                                               #"5": "6", 5: 6, "3": 6, 3: 6}, "MINTWALK": {"2": "2", 2: 2, "4": "2", 4: 2, "12": "1", 12: 1, "5": "3", 5: 3, "16": "4", 16: 4, 15: 5, "15": "5", "11": "5", 11: 5, "9": "6", 9: 6, "20": "6", 20: 6, "3": "3", 3: 3, 22: 8, "22": "8", 21: 7, "21": "7", 27: 10, "27": 10, "26": "11", 26: 11, "24": "12", 24: 12, "25": "13", 25: 13}}
                    ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                    #print(ind, h)
                    processed = []
                    loans = []
                    for i in range(1, len(d)):
                        Data = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                        #print(Data)
                        urlKey = ("MIFOS_URL")
                        baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                        custID = str(Data["CUSTOMER_ID"])
                        loanID = str(Data["LOAN_ID"])
                        #print(Data)
                        #loanDate = datetime.strptime(ldata["data"][0]["LOAN_REQUEST_DATE"], "%Y-%m-%d").strftime("%d %B %Y") if ldata["data"] else datetime.now()
                        refDate = datetime.strptime(str(Data["EXPECTED_DISBURSEMENT_DATE"]), "%Y-%m-%d")
                        approveDate = refDate.strftime("%d %B %Y")
                        loans.append({"lender": "GETCLARITY", "loanId": loanID})
                        processed.append({"loanID": loanID, "modified": False, "approved": False, "disbursed": False,"error":0,"errorMessage":""})
                        if custID != 0 and baseurl["data"]:
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                            headers = (utils.finflux_headers['GETCLARITY'].copy())
                            auth = utils.mifos_auth
                            """
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
                            payload = {k: v for k, v in payload.items() if v is not None}
                            r = requests.put(baseurl + "loans/" + loanID, data=json.dumps(payload), headers=headers, auth=auth,verify=False)
                            """
                            headers["Fineract-Platform-TenantId"] = 'getclarity'
                            payload = {"approvedLoanAmount": Data["AMOUNT"], "approvedOnDate": approveDate, "dateFormat": "dd MMMM yyyy",
                                       "expectedDisbursementDate": approveDate, "locale": "en"}
                            #print(payload)
                            r = requests.post(baseurl + "loans/" + loanID + "?command=approve", data=json.dumps(payload),
                                headers=headers, auth=auth, verify=False)
                            utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                            time.sleep(0.5)
                            if ('resourceId' in r.json() if r.status_code == 200 else False):
                                processed[-1]["approved"] = True
                                token = generate(db).AuthToken()
                                payload = {"actualDisbursementDate": approveDate, "dateFormat": "dd MMMM yyyy", "locale": "en",
                                           "paymentTypeId": "2", "transactionAmount": Data["AMOUNT"]}
                                #print(payload)
                                #utils.logger.info("api request: " + json.dumps(payload), extra=logInfo)
                                r = requests.post(baseurl + "loans/" + loanID + "?command=disburse", data=json.dumps(payload),
                                                  headers=headers, auth=auth, verify=False)
                                utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                                time.sleep(0.5)
                                if ('resourceId' in r.json() if r.status_code == 200 else False):
                                    processed[-1]["disbursed"] = True
                                    token = generate(db).AuthToken()
                                    db.Update(db="mint_loan", table="mw_client_loan_master", checkAll=False, STATUS="ACTIVE",conditions={"ID=": str(Data["ID"])})
                                else:
                                    processed[-1]["error"] = 1
                                    processed[-1]["errorMessage"] = r["errors"][0]["defaultUserMessage"]
                                    token = generate(db).AuthToken()
                            else:
                                processed[-1]["error"] = 1
                                processed[-1]["errorMessage"] = r["errors"][0]["defaultUserMessage"]
                                token = generate(db).AuthToken()
                    #r = requests.post("http://35.154.125.4:9091/v1/updateLoans", data=json.dumps(loans), headers={"Content-type":"application/json"}, verify=False)
                    db._DbClose_()
                    output_dict["data"]["processed"] = processed
                    output_dict["data"].update({"error": 0, "message": "successfully updated"})
                    output_dict["msgHeader"]["authToken"] = token["token"]
                    resp.body = json.dumps(output_dict)
                    #try:
                        #r = requests.post("http://35.154.125.4:9091/v1/updateLoans", data=json.dumps(loans), headers={"Content-type": "application/json"}, verify=False, timeout=1)
                    #except:
                        #pass
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
