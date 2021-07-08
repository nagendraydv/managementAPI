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


class UdaanIncomeUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."
    def formatValue(self, y, md=None):
        if y.ctype == 2:
            return str(int(y.value) if y.value % 1 == 0 else y.value)
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
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Income data imported successfully"
        #logInfo = {'api': 'incomeUpload'}
        try:
            data = {"docType": req.get_param("docType"), "usdInr": req.get_param("usdInr")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            incomeData = req.get_param("incomeData")
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "udaan_income/"
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
                                        #aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                #s3 = session.resource('s3')
                #junk = s3.meta.client.head_bucket(Bucket=bucket)
                s3=utils().s3_connect()
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(
                    falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(token=msgHeader["authToken"])
                if val_error:
                    db._DbClose_()
                    output_dict["data"].update({"error": 1, "message": val_error})
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
                elif db.Query(primaryTable="mw_uber_income_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] != 0:
                    inserted = db.Insert(db="mint_loan", table='mw_uber_income_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))  # %Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_uber_income_documents', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder + filename).put(Body=incomeData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/income." + suffix)
                    md = xlrd.open_workbook(filename="/tmp/income." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    prof = Table("mw_client_profile", schema="mint_loan")
                    income = Table("mw_driver_income_data_new", schema="mint_loan")
                    mapping1 = {"User Name": "NAME", "Working Since": "WORKING_SINCE"}
                    mapping2 = {"Udaan ID/ Vehicle No.": "VEHICLE_NO"}
                    ind, h, n = list(zip(*[(i, mapping1[x.value], x.value.lower() in list(mapping1.keys())) for i, x in enumerate(d[0]) if x.value in mapping1]))
                    ind3, h3, n3 = list(zip(*[(i, mapping2[x.value], x.value in list(mapping2.keys())) for i, x in enumerate(d[0]) if x.value in mapping2]))
                    d2 = []
                    ind2,weekNo = list(zip(*[(i+3,ele) for i,ele in enumerate(d[0]) if ele]))
                    currWeek = datetime.now().isocalendar()[1]
                    for i in range(1, len(d)):
                        #r = dict(list(zip(h, [(((str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2 else (y.value.encode(
                            #'latin1', 'ignore') if y.value != 'NA' else None) if y.ctype == 1 else xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d") if y.ctype == 3 else "")) for j, y in enumerate(d[i]) if j in ind])))
                        r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                        #r3 = dict(list(zip(h3, [(((str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2 else (y.value.encode(
                            #'latin1', 'ignore') if y.value != 'NA' else None) if y.ctype == 1 else xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d") if y.ctype == 3 else "")) for j, y in enumerate(d[i]) if j in ind3])))
                        r3 = dict(list(zip(h3, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind3])))
                        #print(r3)
                        #if ('' not in set(r3.values())) or ('-' not in set(r3.values())):
                        q = Query.from_(prof).select(prof.CUSTOMER_ID)
                        q = db.runQuery(q.where((prof.VEHICLE_NO == r3["VEHICLE_NO"])& (prof.COMPANY_NAME=='UDAAN')))
                        #print(q)
                        customer_id = str(q["data"][0]["CUSTOMER_ID"]) if q["data"]!=[] else None
                        #print(customer_id)
                        r = utils.mergeDicts({"CREATED_BY": msgHeader["authLoginID"], "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "NAME":r["NAME"], "CUSTOMER_ID":customer_id, "WORKING_SINCE": r["WORKING_SINCE"]})
                        #print(r)
                        db.Insert(db="mint_loan", table="mw_udaan_income_profile_data", compulsory=False,debug = False, date=False, **r)
                        
                        #if ((not (q["data"][0]["NAME_VERIFIED"] or q["data"][0]["NUMBER_VERIFIED"])) if q["data"] else False):
                            #junk = db.Update(db="mint_loan", table="mw_client_profile", checkAll=False, NAME_VERIFIED='P', NUMBER_VERIFIED='P',
                                             #MODIFIED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                             #MODIFIED_BY=msgHeader["authLoginID"],
                                             #conditions={"CUSTOMER_ID = ": str(q["data"][0]["CUSTOMER_ID"])})
                        for j in range(3,len(d[0])):
                            weekNo = self.formatValue(d[0][j],md)
                            #print(weekNo)
                            weekYear = '2021' if int(weekNo)< currWeek else '2020'
                            r3 = utils.mergeDicts(r3, {"DOC_ID": docID, "CREATED_BY": msgHeader["authLoginID"],"CUSTOMER_ID":customer_id,"CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "WEEK_NUMBER": weekNo, "WEEK_YEAR": weekYear, "INCOME": (
                                str(d[i][j].value) if d[i][j].value else "0")})
                            #print(r3)
                            db.Insert(db="mint_loan", table="mw_udaan_income_data",
                                      compulsory=False, date=False,debug = False, **r3)
                    if inserted:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["query"]})
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
