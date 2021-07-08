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
import requests
import inspect
import redis
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, Order
from six.moves import range
from six.moves import zip


class RepaymentsBulkUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    @staticmethod
    def multi_push(red, q, vals):
        pipe = red.pipeline()
        for val in vals:
            pipe.lpush(q, val)
        pipe.execute()

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Repayment data inserted successfully"
        logInfo = {'api': 'repayBulkUpload'}
        try:
            data = {"docType": req.get_param("docType"), "lender": req.get_param("lender")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            paymentData = req.get_param("paymentData")
            utils.logger.debug("Request: " + json.dumps({"data": data, "msgHeader": msgHeader, "paymentData": paymentData.filename}), extra=logInfo)
            #generate(db).DBlog(logFrom="repayBulkUpload", lineNo=inspect.currentframe().f_lineno, logMessage="Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "paymentData": paymentData.filename}))
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "finflux_re-payments/"
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (paymentData.filename.split('.')[-1] != "xlsx")):
            output_dict["data"].update({"error": 1, "message": "Invalid file format"})
            resp.body = json.dumps(output_dict)
            if (paymentData.filename.split('.')[-1] != "xlsx"):
                utils.logger.error("ExecutionError: Invalid file format")
            else:
                utils.logger.error("ExecutionError: Invalid request")
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
                pool = redis.ConnectionPool(
                    host='164.52.196.180', port=6379, db=0, password="Mint@123")
                red = redis.Redis(connection_pool=pool)
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(token=msgHeader["authToken"])
                if val_error:
                    db._DbClose_()
                    output_dict["data"].update({"error": 1, "message": val_error})
                    utils.logger.error(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
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
                    junk = s3.Object(
                        bucket, folder + filename).put(Body=paymentData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/payment" + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/payment" + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    errorMsg = "Not processed: "
                    lender = ("GETCLARITY" if (
                        data["lender"] == "GETCLARITY" if "lender" in data else False) else "CHAITANYA")
                    urlKey = ("MIFOS_URL" if lender ==
                              "GETCLARITY" else "FINFLUX_URL")
                    if lender == "GETCLARITY":
                        headers = (
                            utils.finflux_headers[lender] if lender in utils.finflux_headers else {})
                        auth = utils.mifos_auth
                    else:
                        tokenKey = "MintwalkFinfluxAccessToken" if lender == "GETCLARITY" else "FinfluxAccessToken"
                        params = db.runQuery(Query.from_(conf2).select(
                            "CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin([tokenKey])))
                        params = {
                            "FinfluxAccessToken": ele["CONFIG_VALUE"] for ele in params["data"]}
                        headers = utils.mergeDicts((utils.finflux_headers[lender] if lender in utils.finflux_headers else {}),
                                                   {"Authorization": "bearer " + params["FinfluxAccessToken"]})
                    baseurl = db.runQuery(Query.from_(conf).select(
                        "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"] if baseurl["data"] else ""
                    z = [ele.value for ele in d[0]]
                    loans = []
                    gen = generate(db)
                    gen.DBlog(logFrom="repayBulkUpload", lineNo=inspect.currentframe().f_lineno,
                              logMessage="Request: " + json.dumps({"data": data, "msgHeader": msgHeader, "paymentData": paymentData.filename}))
                    for i, datum in enumerate(d[1:]):
                        x = dict(list(zip(z, [(y.value if y.ctype == 2 else y.value.encode('latin1', 'ignore') if y.ctype == 1 else
                                               xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%d %b %Y") if y.ctype == 3 else "")
                                              for y in datum])))
                        payload = {"dateFormat": "dd MMMM yyyy", "locale": "en", "paymentTypeId": 2 if lender == "GETCLARITY" else 751,
                                   "receiptNumber": x["ReceiptNumber"], "routingCode": x["Routing code"],
                                   "transactionAmount": x["Transaction Amount"], "transactionDate": x["Transaction Date"]}
                        postUrl = baseurl + "loans/" + \
                            str(int(x["Account Number"])) + \
                            "/transactions?command=repayment"
                        gen.DBlog(logFrom="repayBulkUpload", lineNo=inspect.currentframe().f_lineno,
                                  logMessage="FINFLUX api URL: " + postUrl)
                        gen.DBlog(logFrom="repayBulkUpload", lineNo=inspect.currentframe().f_lineno,
                                  logMessage="api request: " + json.dumps(payload))
                        utils.logger.info(
                            "FINFLUX api URL: " + postUrl, extra=logInfo)
                        utils.logger.info("api request: " +
                                          json.dumps(payload), extra=logInfo)
                        if lender == "GETCLARITY":
                            r = requests.post(postUrl, data=json.dumps(
                                payload), headers=headers, auth=auth, verify=False)
                        else:
                            r = requests.post(postUrl, data=json.dumps(
                                payload), headers=headers, verify=False)
                        utils.logger.info(
                            "api response: " + json.dumps(r.json()), extra=logInfo)
                        gen.DBlog(logFrom="repayBulkUpload", lineNo=inspect.currentframe().f_lineno,
                                  logMessage="api response: " + json.dumps(r.json()))
                        # {"lender":lender, "loanId":str(int(x["Account Number"]))})
                        loans.append(str(int(x["Account Number"])))
                        if (data["docType"] == "directDebit") & (r.status_code == 200):
                            rid = r.json()["resourceId"]
                            q1 = Query.from_(lm).select("CUSTOMER_ID", "LOAN_REFERENCE_ID").where(
                                lm.LOAN_ACCOUNT_NO == x["Account Number"])
                            q1 = db.runQuery(q1)["data"]
                            if q1:
                                q = Query.from_(repay).select("AUTO_ID").where((repay.CUSTOMER_ID == q1[0]["CUSTOMER_ID"]) &
                                                                               (repay.LOAN_REF_ID == q1[0]["LOAN_REFERENCE_ID"]) &
                                                                               (repay.REPAY_AMOUNT == x["Transaction Amount"]) &
                                                                               (repay.FINFLUX_TRAN_ID.isnull()))
                                q = db.runQuery(q.orderby(repay.REPAY_DATETIME, order=Order.desc).limit(1))[
                                    "data"]
                                repayID = str(q[0]["AUTO_ID"]) if q else ""
                                if repayID != "":
                                    db.Update(db="mint_loan", table="mw_loan_repayment_data", checkAll=False, conditions={"AUTO_ID=": repayID},
                                              FINFLUX_TRAN_ID=(("GC" if lender == "GETCLARITY" else "") + str(rid)), MODIFIED_BY="REPAY_API",
                                              MODIFIED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))
                        elif r.status_code != 200:
                            errorMsg += str(x["Account Number"]) + ","
                        else:
                            errorMsg = success
                    if inserted:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success if errorMsg == "Not processed: " else errorMsg})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                        gen.DBlog(logFrom="repayBulkUpload", lineNo=inspect.currentframe().f_lineno,
                                  logMessage="Response: " + json.dumps(output_dict))
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    utils.logger.debug(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    try:
                        self.multi_push(red, 'SYNC_TRANS_LOANIDS', loans)
                        response = requests.get(
                            'http://13.126.29.47:8080/sync-loan/SyncLoan/loanSync', headers={'cache-control': 'no-cache'})
                        # r = requests.post("http://35.154.125.4:9091/v1/updateLoans", data=json.dumps(loans), timeout=0.1,
                        #                  headers={"Content-type":"application/json"}, verify=False)
                    except:
                        pass
                else:
                    db._DbClose_()
                    output_dict["data"].update(
                        {"error": 1, "message": "duplicate file - not uploaded"})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
            except Exception as ex:
                utils.logger.error("ExecutionError: ",
                                   extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
