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
import csv
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, Order, JoinType
from six.moves import range
from six.moves import zip


class UberPaymentsUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Income data imported successfully"
        #logInfo = {'api': 'incomeUpload'}
        try:
            data = {"docType": req.get_param("docType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            paymentData = req.get_param("paymentData")
            # print data, msgHeader
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "uber_payments/"
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (paymentData.filename.split('.')[-1] != "csv")):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            resp.body = json.dumps(output_dict)
            # if (incomeData.filename.split('.')[-1]!="csv"):
            #    utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = paymentData.filename.split('.')[-1]
                filename = paymentData.filename  # self.setFilename() + suffix
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
                    resp.body = json.dumps(output_dict)
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
                elif db.Query(primaryTable="mw_payment_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] == 0:
                    inserted = db.Insert(db="mint_loan", table='mw_payment_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))  # %Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_payment_documents', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder + filename).put(Body=paymentData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/payment" + suffix)
                    f = open("/tmp/payment" + suffix, "r")
                    #f = open("/home/nagendra/Downloads/uber_to_mintwalk_payment_report_2018-06-25.csv", "r")
                    reader = csv.reader(f)
                    data = [row for row in reader]
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    cm = Table("mw_finflux_client_master", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    mapping = {"uber_user_uuid": "UUID", "lender_contract_id": "LOAN_ACCOUNT_NO", "amount_to_charge": "amount_to_charge",
                               "amount_charged": "REPAY_AMOUNT", "shortfall": "REPAY_INFO2", "due_date": "due_date", "description": "REPAY_INFO1",
                               "charge_id": "REPAY_INFO4", "payment_item_uuid": "REPAY_INFO3"}
                    if 'transaction_date' in data[0]:
                        mapping.update({"transaction_date":"TransactionDate"})
                    if 'mode' in data[0]:
                        mapping.update({"mode":"Mode"})
                    z = [mapping[ele] for ele in data[0]]
                    for i, datum in enumerate(data[1:]):
                        x = dict(list(zip(z, datum)))
                        #print(x["TransactionDate"])
                        q = Query.from_(lm).select(
                            lm.CUSTOMER_ID, lm.LOAN_REFERENCE_ID, lm.ID,lm.LOAN_ACCOUNT_NO)
                        loans = db.runQuery(
                            q.where(lm.LOAN_ACCOUNT_NO == x["LOAN_ACCOUNT_NO"]))
                        if not loans["data"]:
                            loans = db.runQuery(
                                q.where(lm.LOAN_APPLICATION_NO.like("%" + x["LOAN_ACCOUNT_NO"])))
                        #q = q.where((lm.LOAN_ACCOUNT_NO==x["LOAN_ACCOUNT_NO"]).__or__(lm.LOAN_APPLICATION_NO.like("%" + x["LOAN_ACCOUNT_NO"])))
                        #loans = db.runQuery(q)
                        custID2 = str(
                            loans["data"][0]["CUSTOMER_ID"]) if loans["data"] else "0"
                        custID = db.runQuery(Query.from_(income).select("CUSTOMER_ID").distinct().where(
                            (income.CUSTOMER_ID != 0) & (income.DRIVER_UUID == x["UUID"])))
                        custID = str(
                            custID["data"][0]["CUSTOMER_ID"]) if custID["data"] else None
                        clientID = db.runQuery(Query.from_(cm).select(cm.CLIENT_ID).where(
                            cm.CUSTOMER_ID == (custID2 if custID2 else custID)))
                        clientID = clientID["data"][0]["CLIENT_ID"] if clientID["data"] else ""
                        epoch = str(int(datetime.now().strftime("%s")) * 1000)
                        # if (custID!=custID2) and (custID) and (custID2!='0') and (custID!='0'):
                        #    loans = Query.from_(lm).select(lm.CUSTOMER_ID, lm.LOAN_REFERENCE_ID, lm.LOAN_ACCOUNT_NO)
                        #    loans = loans.where(lm.CUSTOMER_ID==str(custID)).where(lm.STATUS!='ML_REJECTED').orderby(lm.ID, order=Order.desc)
                        #    loans = db.runQuery(loans)
                        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        todate = datetime.now().strftime("%Y-%m-%d")
                        if 'transaction_date' in data[0]:
                            if x['TransactionDate']!='':
                                epoch2=(datetime.strptime(x["TransactionDate"],"%m/%d/%Y")).strftime("%s")
                            else:
                                epoch2=datetime.now().strftime("%s")
                        else:
                            epoch2=datetime.now().strftime("%s")
                        # = datetime.now().strftime("%s")#  
                        #print(epoch2)
                        if 'transaction_date' in data[0]:
                            if x['TransactionDate']!='':
                                transaction_date=(datetime.strptime(x["TransactionDate"],"%m/%d/%Y")).strftime("%Y-%m-%d")
                            else:
                                transaction_date=todate
                        else:
                            transaction_date=todate
                        if 'mode' in data[0]:
                            if x['Mode']!='':
                                mode=x["Mode"]
                            else:
                                mode="UBER_DIRECT_DEBIT"
                        else:
                            mode="UBER_DIRECT_DEBIT"
                        tranStatus = ("SUCCESS" if x["REPAY_INFO2"] == "0" else "PART_SUCCESS"
                                      if (int(float(x["amount_to_charge"])) - int(float(x["REPAY_INFO2"])) > 1) else "FAILURE")
                        w = {"AMOUNT": "%.2f" % (float(x["REPAY_AMOUNT"]) if tranStatus != 'FAILURE' else float(x["amount_to_charge"])), "LOAN_ID": loans["data"][0]["LOAN_ACCOUNT_NO"] if loans["data"] else None,
                             "TRANSACTION_MEDIUM": mode, "TRANSACTION_STATUS": tranStatus, "TRANSACTION_DATE": transaction_date,
                             "ACCESSED": "1", "NOTIFICATION_SENT": "0", "CREATED_DATE": today, "CREATED_BY": "CRON",
                             "TRANSACTION_REF_NO": "-".join([epoch, clientID, loans["data"][0]["LOAN_ACCOUNT_NO"],str(i)])}
                        inserted = db.Insert(
                            table="mw_client_loan_repayment_history_master", db="mint_loan", date=False, compulsory=False, **w)
                        if tranStatus in ("SUCCESS", "PART_SUCCESS"):
                            y = {"REPAY_AMOUNT": "%.2f" % (float(x["REPAY_AMOUNT"])), "CREATED_DATE": epoch2, "CREATED_BY": "CRON",
                                 "MODE_OF_PAYMENT":mode, "CUSTOMER_ID": custID2 if custID2 else custID, "DEPOSIT_DATETIME": epoch2,
                                 "REPAY_INFO": "EMI-" + "-".join([x["REPAY_INFO2"], x["REPAY_INFO3"], x["REPAY_INFO4"]]),
                                 "LOAN_REF_ID": loans["data"][0]["LOAN_REFERENCE_ID"] if loans["data"] else "", "REPAY_DATETIME": epoch2,
                                 "TRANSACTION_REF_NO": "-".join([epoch, clientID, loans["data"][0]["LOAN_ACCOUNT_NO"],str(i)])}
                            inserted = db.Insert(
                                table="mw_loan_repayment_data", db="mint_loan", date=False, compulsory=False, **y)
                    if inserted:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                else:
                    db._DbClose_()
                    output_dict["data"].update(
                        {"error": 0, "message": "duplicate file - not uploaded"})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
