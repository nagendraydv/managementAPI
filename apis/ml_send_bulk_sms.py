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
import time
from six.moves import range
from six.moves import zip


class SendBulkSmsUploadResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits
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
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"resp": {}}}
        errors = utils.errors
        success = "Income data imported successfully"
        logInfo = {'api': 'bulkSms'}
        try:
            data = {"docType": req.get_param("docType"), "subType": req.get_param("subType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            smsData = req.get_param("smsData")
            utils.logger.debug("Request: " + json.dumps(
                {"data": data, "msgHeader": msgHeader, "incomeData": smsData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "bulk_sms/"
        # print data, data["forceUpdate"]=='1'
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (smsData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = smsData.filename.split('.')[-1]
                filename = smsData.filename  # + suffix
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
                    junk = s3.Object(bucket, folder +
                                     filename).put(Body=smsData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/bulk_sms." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/bulk_sms." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    if data["subType"] != 'overdueAmount':
                        url = "http://13.126.29.47:8080/mintLoan/mintloan/sendSmsToMultipleNumbers"
                    else:
                        # print "http://13.126.28.53:8080/mintLoan/mintloan/sendSmsOverdueAmount"
                        url = "http://13.126.29.47:8080/mintLoan/mintloan/sendSmsOverdueAmount"
                    utils.logger.debug("Request URL: " + url, extra=logInfo)
                    mapping = {"sms_type": "SMS_TYPE", "language": "LANGUAGE",
                               "mob_no": "MOB_NO", "customer_id": "CUSTOMER_ID"}
                    ind, h = list(zip(*[(i, mapping[x.value.lower()])
                                        for i, x in enumerate(d[0]) if x.value.lower() in mapping]))
                    mobInd, cidInd = (ind[h.index("MOB_NO")] if "MOB_NO" in h else None, ind[h.index(
                        "CUSTOMER_ID")] if "CUSTOMER_ID" in h else None)
                    if mobInd and cidInd:
                        phoneNumbers = [{"mobileNumber": str(int((ele[mobInd].value.replace(" ", "") if type(ele[mobInd].value) == str else ele[mobInd].value)))[
                            -10:], "customerId":int(ele[cidInd].value)} for ele in d[1:] if ' ' not in str(int(ele[mobInd].value))[-10:]]
                    else:
                        phoneNumbers = None
                    if phoneNumbers:
                        payload = {"smsType": d[1][ind[h.index("SMS_TYPE")]].value, "language": d[1][ind[h.index("LANGUAGE")]].value,
                                   "sendBy": "Admin", "MobNumbersDetails": phoneNumbers}
                        utils.logger.debug("Request sent to java backend", extra=logInfo)
                        for i in range(0, len(phoneNumbers), 100):
                            payload["MobNumbersDetails"] = phoneNumbers[i:i+100]
                            r = requests.post(url, data=json.dumps(payload),headers={"Content-Type": "application/json"}, verify=False)  # sendSmsToMultipleNumbers
                            time.sleep(1)
                        if r.status_code == 200:
                            utils.logger.debug("Response code 200 returned", extra=logInfo)
                            token = generate(db).AuthToken()
                            if "token" in list(token.keys()):
                                output_dict["data"].update({"error": 0, "message": success, "resp": r.json()})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"], "resp": r.json()})
                        else:
                            utils.logger.debug(
                                "Some error occurred at java end", extra=logInfo)
                            output_dict["data"].update(
                                {"error": 1, "message": errors["query"], "resp": {}})
                    else:
                        utils.logger.debug(
                            "Request not sent to java backend. Some issue with the file.", extra=logInfo)
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 1, "resp": {},
                                                        "message": "Error occurred while extracting data from the file. Check file format."})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"], "resp": {}})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                else:
                    db._DbClose_()
                    output_dict["data"].update(
                        {"error": 1, "message": "duplicate file - not uploaded", "resp": {}})
                    utils.logger.error(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
            except Exception as ex:
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
