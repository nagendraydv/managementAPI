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
from pypika import Query, Table, functions
import requests
from six.moves import range
from six.moves import zip


class SwiggyWeeklyDeductionUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def formatValue(self, y):
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
                       "data": {"insertFailed": []}}
        errors = utils.errors
        success = "Data imported successfully"
        #logInfo = {'api': 'incomeUpload'}
        try:
            data = {"docType": req.get_param("docType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            transactionData = req.get_param("transactionData")
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "swiggy_reliance_transaction_feed/"
        # print data, data["forceUpdate"]=='1'
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (transactionData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = transactionData.filename.split('.')[-1]
                filename = transactionData.filename  # + suffix
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
                elif db.Query(primaryTable="mw_other_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] == 0:
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
                        bucket, folder + filename).put(Body=transactionData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/swiggy_reliance_transaction_feed." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/swiggy_reliance_transaction_feed." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    mapping = {"de id": "DE_ID", "name": "NAME", "phone no.": "PHONE", "deduction amount": "DEDUCTION_AMOUNT",
                               "supermoney c id": "CUSTOMER_ID", "company name": "COMPANY_NAME", "city": "CITY", "amount deducted": "AMOUNT_DEDUCTED",
                               "deduction status": "DEDUCTION_STATUS", "remarks": "REMARKS", "week": "WEEK", "transaction date": "TRANSACTION_DATE"}
                    ind, h = list(zip(*[(i, mapping[x.value.lower()])
                                        for i, x in enumerate(d[0]) if x.value.lower() in mapping]))
                    insert_dict = []
                    insert_failed = []
                    for i in range(1, len(d)):
                        r = dict(list(zip(h, [(((str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2
                                                else (y.value.encode('latin1', 'ignore').decode() if y.value != 'NA' else None) if y.ctype == 1 else
                                                xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d") if y.ctype == 3 else ""))
                                              # if (not n[j]) else ((str(int(y.value)) if y.value.is_integer() else str(y.value)) if y.ctype==2 else 0))
                                              for j, y in enumerate(d[i]) if j in ind])))
                        if ((not r["AMOUNT_DEDUCTED"].isdigit()) if type(r["AMOUNT_DEDUCTED"]) == str else True) if "AMOUNT_DEDUCTED" in r else True:
                            insert_dict = []
                            insert_failed = [
                                "check the deduction amount. Amount needs to be a positive number."]
                            break
                        if ((not r["CUSTOMER_ID"].isdigit()) if type(r["CUSTOMER_ID"]) == str else True) if "CUSTOMER_ID" in r else True:
                            insert_dict = []
                            insert_failed = [
                                "check supermoney c id. It needs to be a positive number."]
                            break
                        if ((not r["WEEK"]) | (not r["TRANSACTION_DATE"]) | (not r["DEDUCTION_STATUS"]) | (not r["NAME"]) | (not r["COMPANY_NAME"]) | (not r["CITY"]) | (not r["PHONE"])) if (("WEEK" in r) & ("TRANSACTION_DATE" in r) & ("DEDUCTION_STATUS" in r) & ("NAME" in r) & ("COMPANY_NAME" in r) & ("CITY" in r) and ("PHONE" in r)) else True:
                            insert_dict = []
                            insert_failed = ["Some column is blank."]
                            break
                        insert_dict.append(r)
                    for rr in insert_dict:
                        try:
                            inserted = db.Insert(db="gc_reliance", table="mw_swiggy_weekly_deduction", compulsory=False,
                                                 date=False, CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **rr)
                        except:
                            insert_failed.append(rr)
                    if inserted:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0 if insert_failed == [
                            ] else 1, "message": success, "insertFailed": insert_failed})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 1,
                                                        "message": "Error occurred while extracting data from the file. Check file format."})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                else:
                    db._DbClose_()
                    output_dict["data"].update(
                        {"error": 1, "message": "duplicate file - not uploaded", "resp": {}})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
            except Exception as ex:
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
