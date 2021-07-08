from __future__ import absolute_import
from __future__ import print_function
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
from pypika import Query, Table, functions
import six
from six.moves import range


class RelianceOfflinePurchaseResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Purchase data imported successfully"
        logInfo = {'api': 'purchaseDataUpload'}
        try:
            data = {"docType": req.get_param("docType"), "format": req.get_param("format")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            purchaseData = req.get_param("purchaseData")
            # print data, msgHeader
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "mandateData": mandateData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "reliace_purchase_info/"
        if ((not validate.Request(api='mandateDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (purchaseData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            # if (mandateData.filename.split('.')[-1] not in ("xlsx", "xls")):
            #utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = purchaseData.filename.split('.')[-1]
                filename = self.setFilename() + suffix
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                s3 = session.resource('s3')
                junk = s3.meta.client.head_bucket(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                # falcon.HTTPError(falcon.HTTP_400,'Connection error', 'Could not establish S3 connection')
                raise
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(
                    token=msgHeader["authToken"])
                if val_error:
                    db._DbClose_()
                    output_dict["data"].update({"error": 1, "message": val_error})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
                else:
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_STATUS='N', UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"), DOCUMENT_FOLDER=folder[:-1])
                    docID = db.Query(db="mint_loan", primaryTable='mw_other_documents', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc", limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder + filename).put(Body=purchaseData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/purchase." + suffix)
                    md = xlrd.open_workbook(filename="/tmp/purchase." + suffix)
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    #print(d)
                    header = [(ele.value if type(ele.value) in (
                        str, six.text_type) else '') for ele in d[0]]
                    payload = {"req": [{header[i]:data[i].value for i in range(len(data))} for data in d[1:]]}
                    #print(payload, header)
                    url = "http://13.126.28.53:8080/MintReliance/MintReliance/relianceOfflinePurchase"
                    r = requests.post(url, data=json.dumps(payload), headers={"Content-type": "application/json"})
                    if r.status_code == 200:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success, "response": r.json()})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    # print output_dict
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
