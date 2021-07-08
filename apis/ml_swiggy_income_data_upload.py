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
from pypika import Query, Table, functions, Order, JoinType
from six.moves import range
from six.moves import zip


class SwiggyIncomeUploadResource:

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
            data = {"docType": req.get_param(
                "docType"), "usdInr": req.get_param("usdInr")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            incomeData = req.get_param("incomeData")
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "swiggy_income/"
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (incomeData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            # if (incomeData.filename.split('.')[-1]!="csv"):
            #    utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = incomeData.filename.split('.')[-1]
                filename = incomeData.filename  # self.setFilename() + suffix
                s3path = s3url + bucket + '/' + folder + filename
                #session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                #                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                #s3 = session.resource('s3')
                #junk = s3.meta.client.head_bucket(Bucket=bucket)
                s3=utils().s3_connect()
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
                elif db.Query(primaryTable="mw_uber_income_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] == 0:
                    inserted = db.Insert(db="mint_loan", table='mw_uber_income_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))  # %Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_uber_income_documents', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder +
                                     filename).put(Body=incomeData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/income." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/income." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    cred = Table("mw_customer_login_credentials",
                                 schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    custdata = Table("mw_customer_data", schema="mint_loan")
                    panstatus = Table("mw_pan_status", schema="mint_loan")
                    mapping1 = {"reference_no": "CUSTOMER_ID", "latest deid as per swiggy": "LATEST_DE_ID",
                                "avail_card": "AVAIL_CARD", "rating": "RATING", "data_requested_date": "DATA_REQUEST_DATE"}
                    mapping2 = {"reference_no": "CUSTOMER_ID", "latest deid as per swiggy": "DE_ID", "applicant name": "NAME", "phone number": "PHONE_NUMBER", "city": "CITY", "joining date": "JOINING_DATE",
                                "current_address": "ADDRESS", "pincode": "PINCODE", "bank_account no.": "BANK_ACCOUNT_NO", "ifsc code": "IFSC_CODE", "bank name": "BANK_NAME", "account holder name": "ACCOUNT_HOLDER_NAME"}
                    ind, h, n = list(zip(*[(i, mapping1[x.value.lower()], x.value.lower() in list(
                        mapping1.keys())) for i, x in enumerate(d[0]) if x.value.lower() in mapping1]))
                    ind3, h3, n3 = list(zip(*[(i, mapping2[x.value.lower()], x.value.lower() in list(
                        mapping2.keys())) for i, x in enumerate(d[0]) if x.value.lower() in mapping2]))
                    d2 = []
                    ind2, weekNo = list(zip(*[(i, "".join(j for j in d[0][i].value if j.isdigit()))
                                              for i, ele in enumerate(d[0]) if ('week' in ele.value.lower() if ele.value else False)]))
                    currWeek = datetime.now().isocalendar()[1]
                    weekYear = ["2020" if int(
                        i) < currWeek else "2019" for i in weekNo]
                    for i in range(1, len(d)):
                        r = dict(list(zip(h, [(((str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2 else (y.value.encode(
                            'ascii', 'ignore').decode() if y.value != 'NA' else None) if y.ctype == 1 else xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d") if y.ctype == 3 else "")) for j, y in enumerate(d[i]) if j in ind])))
                        r3 = dict(list(zip(h3, [(((str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2 else (y.value.encode(
                            'ascii', 'ignore').decode() if y.value != 'NA' else None) if y.ctype == 1 else xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d") if y.ctype == 3 else "")) for j, y in enumerate(d[i]) if j in ind3])))
                        if ('' not in set(r3.values())) or ('-' not in set(r3.values())):
                            r3 = utils.mergeDicts(r3, {"DOC_ID": docID, "CREATED_BY": msgHeader["authLoginID"], "CREATED_DATE": datetime.now(
                            ).strftime("%Y-%m-%d %H:%M:%S")})
                            db.Insert(db="mint_loan", table="mw_swiggy_loans_profile_data",
                                      compulsory=False, date=False, noIgnor=False, **r3)
                            q = Query.from_(prof).select(
                                prof.CUSTOMER_ID, prof.NUMBER_VERIFIED, prof.NAME_VERIFIED)
                            q = db.runQuery(
                                q.where(prof.CUSTOMER_ID == r["CUSTOMER_ID"]))
                            if ((not (q["data"][0]["NAME_VERIFIED"] or q["data"][0]["NUMBER_VERIFIED"])) if q["data"] else False):
                                junk = db.Update(db="mint_loan", table="mw_client_profile", checkAll=False, NAME_VERIFIED='P', NUMBER_VERIFIED='P',
                                                 MODIFIED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                 MODIFIED_BY=msgHeader["authLoginID"],
                                                 conditions={"CUSTOMER_ID = ": str(q["data"][0]["CUSTOMER_ID"])})
                            for j in range(len(ind2)):
                                r2 = utils.mergeDicts(r, {"DOC_ID": docID, "CREATED_BY": msgHeader["authLoginID"], "WEEK_NUMBER": weekNo[j], "WEEK_YEAR": weekYear[j], "INCOME": (
                                    str(d[i][ind2[j]].value) if d[i][ind2[j]].value.is_integer() else "0")})
                                db.Insert(db="mint_loan", table="mw_swiggy_income_data",
                                          compulsory=False, date=False, noIgnor=False, **r2)
                    if inserted:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update(
                                {"error": 0, "message": success})
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
                        {"error": 1, "message": "duplicate file - not uploaded"})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
