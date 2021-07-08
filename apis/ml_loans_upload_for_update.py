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
import redis
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions
import requests
import time
from six.moves import range


class UpdateLoansResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Loans data imported successfully. They will be updated soon."
        logInfo = {'api': 'loansDataUpload'}
        try:
            data = {"docType": req.get_param("docType"), "subType": req.get_param("subType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            loansData = req.get_param("loansData")
            utils.logger.debug("Request: " + json.dumps(
                {"data": data, "msgHeader": msgHeader, "loansData": loansData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "loans_update/"
        # print data, data["forceUpdate"]=='1'
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (loansData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = loansData.filename.split('.')[-1]
                filename = loansData.filename  # + suffix
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
                                     filename).put(Body=loansData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/loans_update." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/loans_update." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [str(int(sheet.row_slice(i)[0].value)) for i in range(
                        1, sheet.nrows) if len(sheet.row_slice(i)) > 0]
                    if d:
                        pool = redis.ConnectionPool(
                            host='164.52.196.180', port=6379, db=0, password="Mint@123")
                        red = redis.Redis(connection_pool=pool)
                        pipe = red.pipeline()
                        for val in d:
                            pipe.lpush('SYNC_TRANS_LOANIDS', val)
                        pipe.execute()
                        try:
                            response = requests.get('http://13.126.29.47:8080/sync-loan/SyncLoan/loanSync', headers={
                                                    'cache-control': 'no-cache'}, timeout=0.1)
                        except:
                            pass
                        utils.logger.debug(
                            "Request sent to redis backend", extra=logInfo)
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        utils.logger.debug(
                            "Request not sent to java backend. Some issue with the file.", extra=logInfo)
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
                        {"error": 1, "message": "duplicate file - not uploaded"})
                    utils.logger.error(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
            except Exception as ex:
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
