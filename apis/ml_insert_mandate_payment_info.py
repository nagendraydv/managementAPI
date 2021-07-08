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
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, Order
from six.moves import range
from six.moves import zip


class MandatePaymentsUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Payment data imported successfully"
        #logInfo = {'api': 'incomeUpload'}
        try:
            data = {"docType": req.get_param("docType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            paymentData = req.get_param("paymentData")
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "mandate_payments/"
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data}))):# or (paymentData.filename.split('.')[-1] not in ("xls", "xlsx"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            # if (incomeData.filename.split('.')[-1]!="csv"):
            #    utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = paymentData.filename.split('.')[-1]
                filename = paymentData.filename
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                #s3 = session.resource('s3')
                #junk = s3.meta.client.head_bucket(Bucket=bucket)
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
                    inserted = db.Insert(db="mint_loan", table='mw_payment_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))  # %Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_payment_documents', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    #junk = s3.Object(
                       # bucket, folder + filename).put(Body=paymentData.file.read())
                    #junk = s3.Bucket(bucket).download_file(
                        #folder + filename, "/tmp/payment." + suffix)
                    md = xlrd.open_workbook(
                        filename="//home/nagendra/Downloads/FQJX22H1585058406rbl." + suffix, encoding_override='utf-8')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    cm = Table("mw_finflux_client_master", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    mand = Table("mw_physical_mandate_status",
                                 schema="mint_loan")
                    mapping = ({"STATUS": "STATUS", "ITEM REFERENCE": "LOAN_ACCOUNT_NO", "AMOUNT": "REPAY_AMOUNT", "VALUE DATE": "VALUE_DATE",
                                "REASON CODE": "REPAY_INFO"} if data["docType"] == "yesBankFormat" else
                               {"STATUS": "STATUS", "AMOUNT": "REPAY_AMOUNT", "ECS_DATE": "VALUE_DATE", "REFNO": "REPAY_INFO", "UMRN": "UMRN"})
                    z = [mapping[ele.value] for ele in d[0] if ele.value in mapping]
                    ind = [i for i, ele in enumerate(d[0]) if ele.value in mapping]
                    #print(ind)
                    w = []
                    y = []
                    error = False
                    errorDesc = ""
                    for i, datum in enumerate(d[1:]):
                        #print(datum)
                        #print(i)
                        x = dict(list(zip(z, [ele.value.replace(",", "")for i, ele in enumerate(datum) if i in ind])))
                        #print(x)
                        #print(data["docType"])
                        if data["docType"] != "yesBankFormat":
                            custID = db.runQuery(Query.from_(mand).select(mand.CUSTOMER_ID).where(mand.REF_NO == x["UMRN"]))["data"]
                            custID = str(custID[0]["CUSTOMER_ID"]) if custID else ""
                            if custID != "":
                                q = Query.from_(lm).select(lm.CUSTOMER_ID, lm.LOAN_REFERENCE_ID, lm.LOAN_ACCOUNT_NO, lm.STATUS)
                                loans = q.where((lm.CUSTOMER_ID == custID) & (lm.STATUS.notin(["ML_REJECTED", "REJECTED", "PENDING", "REQUESTED"])))
                                loans = db.runQuery(loans.orderby(lm.ID, order=Order.desc).limit(1))["data"]
                            else:
                                error = True
                                errorDesc += "UMRN %s not in the system," % x["UMRN"]
                        else:
                            q = Query.from_(lm).select(lm.CUSTOMER_ID, lm.LOAN_REFERENCE_ID, lm.LOAN_ACCOUNT_NO)
                            loans = db.runQuery(q.where(lm.LOAN_APPLICATION_NO.like("%" + x["LOAN_ACCOUNT_NO"])))["data"]
                            loans = loans if loans else db.runQuery(q.where(lm.LOAN_ACCOUNT_NO == x["LOAN_ACCOUNT_NO"]))["data"]
                            #print(loans)
                            custID = str(loans[0]["CUSTOMER_ID"]) if loans else ""
                            if custID == "":
                                error = True
                                errorDesc += "LOAN_ACCOUNT_NO %s not in the system," % x["LOAN_ACCOUNT_NO"]
                        if custID != "":
                            clientID = db.runQuery(Query.from_(cm).select(cm.CLIENT_ID).where(cm.CUSTOMER_ID == custID))
                            clientID = clientID["data"][0]["CLIENT_ID"] if clientID["data"] else ""
                            epoch = str(int(datetime.now().strftime("%s")) * 1000)
                            today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            todate = datetime.now().strftime("%Y-%m-%d")
                            epoch2 = datetime.now().strftime("%s")
                            tranStatus = ("SUCCESS" if x["STATUS"] in ("ACCEPTED", "PAID") else "FAILURE")
                            tranDate = datetime.strptime(x["VALUE_DATE"], "%d/%m/%Y").strftime("%Y-%m-%d")
                            w.append({"AMOUNT": "%.2f" % (float(x["REPAY_AMOUNT"].replace(",", ""))), "LOAN_ID": loans[0]["LOAN_ACCOUNT_NO"],
                                      "TRANSACTION_MEDIUM": "NACH_DEBIT", "TRANSACTION_STATUS": tranStatus, "TRANSACTION_DATE": tranDate,
                                      "ACCESSED": "1", "NOTIFICATION_SENT": "0", "CREATED_DATE": today, "CREATED_BY":"CRON",
                                      "TRANSACTION_REF_NO": "-".join([epoch, clientID, loans[0]["LOAN_ACCOUNT_NO"]])})
                            if tranStatus in ("SUCCESS", "PART_SUCCESS"):
                                y.append({"REPAY_AMOUNT": "%.2f" % (float(x["REPAY_AMOUNT"].replace(",", ""))), "CREATED_DATE": epoch2, "CREATED_BY": "CRON",
                                          "MODE_OF_PAYMENT": "NACH_DEBIT", "CUSTOMER_ID": custID, "DEPOSIT_DATETIME": epoch2,
                                          "REPAY_INFO": data["docType"] + " RefNo:" + x["REPAY_INFO"],
                                          "LOAN_REF_ID": loans[0]["LOAN_REFERENCE_ID"] if loans else "", "REPAY_DATETIME": epoch2})
                    #print(w, y)
                    if not error:
                        for i in w:
                            #print(i)
                            inserted = db.Insert(table="mw_client_loan_repayment_history_master", db="mint_loan", date=False,debug=True, compulsory=False, **i)
                        for i in y:
                            #print(i)
                            inserted = db.Insert(table="mw_loan_repayment_data", db="mint_loan", date=False,debug=True, compulsory=False, **i)
                    if inserted and (not error):
                        #print("yes")
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    elif error:
                        output_dict["data"].update({"error": 1, "message": errorDesc})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["query"]})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
