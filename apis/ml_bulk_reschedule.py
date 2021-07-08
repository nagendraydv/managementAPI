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
import inspect
from six.moves import range
from six.moves import zip


class BulkRescheduleResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Request processed"
        logInfo = {'api': 'bulkCustomerCreate'}
        try:
            data = {"docType": req.get_param(
                "docType"), "subType": req.get_param("subType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            customerData = req.get_param("customerData")
            utils.logger.debug("Request: " + json.dumps(
                {"data": data, "msgHeader": msgHeader, "customerData": customerData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "bulk_reschedule/"
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (customerData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = customerData.filename.split('.')[-1]
                filename = customerData.filename  # + suffix
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
                gen = generate(db)
                gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                          logMessage=("Request: " + json.dumps({"data": data, "msgHeader": msgHeader, "customerData": customerData.filename})))
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
                        bucket, folder + filename).put(Body=customerData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/bulk_reschedule." + suffix)
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    emi = Table("mw_client_loan_emi_details",
                                schema="mint_loan")
                    baseurl = db.runQuery(Query.from_(conf).select(
                        "CONFIG_VALUE").where(conf.CONFIG_KEY == "MIFOS_URL"))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    headers = utils.finflux_headers["GETCLARITY"].copy()
                    auth = utils.mifos_auth
                    md = xlrd.open_workbook(
                        filename="/tmp/bulk_reschedule." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    mapping = {"loan_id": "LOAN_ID", "reschedule_from": "RESCHEDULE_FROM",
                               "reschedule_to": "RESCHEDULE_TO"}
                    nreq = ["loan_id", "reschedule_from", "reschedule_to"]
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    ind, h, n = list(zip(*[(i, mapping[x.value.lower()], x.value.lower() in nreq)
                                           for i, x in enumerate(d[0]) if x.value.lower() in mapping]))
                    d2 = []
                    for i in range(1, len(d)):
                        r = dict(list(zip(h, [(str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2
                                              else (y.value.encode('latin1', 'ignore').decode() if y.value != 'NA' else None) if y.ctype == 1 else
                                              xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime(
                            "%Y-%m-%d") if y.ctype == 3 else ""
                            if (not n[j]) else ""
                            for j, y in enumerate(d[i]) if j in ind])))
                        gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                  logMessage=("processing data - "+json.dumps(r)))
                        payload = {"submittedOnDate": datetime.now().strftime("%d %b %Y"), "rescheduleReasonId": 805, "loanId": r["LOAN_ID"],
                                   "rescheduleFromDate": datetime.strptime(r["RESCHEDULE_FROM"], "%Y-%m-%d").strftime("%d %b %Y"),
                                   "adjustedDueDate": datetime.strptime(r["RESCHEDULE_TO"], "%Y-%m-%d").strftime("%d %b %Y"),
                                   "dateFormat": "dd MMMM yyyy", "locale": "en"}
                        gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                  logMessage=("Mifos request url: " + baseurl + "rescheduleloans?command=reschedule"))
                        gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                  logMessage=("Mifos request:"+json.dumps(payload)))
                        request = requests.post(baseurl + "rescheduleloans?command=reschedule", data=json.dumps(payload), headers=headers,
                                                auth=auth, verify=False)
                        if request.status_code == 200:
                            resp1 = request.json()
                            gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                      logMessage=("Mifos response:" + json.dumps(resp1)))
                            if "resourceId" in resp1:
                                payload = {"approvedOnDate": datetime.now().strftime(
                                    "%d %b %Y"), "dateFormat": "dd MMMM yyyy", "locale": "en"}
                                gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                          logMessage=("Mifos request url:" + baseurl + "rescheduleloans/" + str(resp1["resourceId"]) +
                                                      "?command=approve"))
                                gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                          logMessage=("Mifos request:" + json.dumps(payload)))
                                request2 = requests.post(baseurl + "rescheduleloans/" + str(resp1["resourceId"]) + "?command=approve",
                                                         data=json.dumps(payload), headers=headers, auth=auth, verify=False)
                                if request2.status_code == 200:
                                    resp2 = request2.json()
                                    gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                              logMessage=("Mifos response:" + json.dumps(resp2)))
                                else:
                                    try:
                                        resp2 = request2.json()
                                    except:
                                        resp2 = {
                                            "error": "unknown error occurred"}
                                    gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                              logMessage=("Mifos response:" + json.dumps(resp2)))
                        else:
                            try:
                                resp1 = request.json()
                            except:
                                resp1 = {"error": "unknown error occurred"}
                            gen.DBlog(logFrom="rescheduleLoans", lineNo=inspect.currentframe().f_lineno,
                                      logMessage=("Mifos response:" + json.dumps(resp1)))
                    utils.logger.debug("Request processed", extra=logInfo)
                    token = generate(db).AuthToken()
                    if "token" in list(token.keys()):
                        output_dict["data"].update(
                            {"error": 0, "message": success})
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
