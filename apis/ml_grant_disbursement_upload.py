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
from pypika import functions as fn
import csv

class grantDisbursementUploadResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def formatValue(self, y,md=None):
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
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        try:
            data = {"docType": req.get_param("docType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            grantData = req.get_param("grantData")
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "grant_upload/"
        if ((not validate.Request(api='grantUpload', request={"msgHeader": msgHeader, "data": data})) or
                (grantData.filename.split('.')[-1] not in ("xlsx", "xls","csv",'dbf','txt'))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = grantData.filename.split('.')[-1]
                filename = grantData.filename.split('.')[0] + '.' + suffix
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                s3 = session.resource('s3')
                junk = s3.meta.client.head_bucket(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(token=msgHeader["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                elif db.Query(primaryTable="mw_other_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] == 0:
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         UPLOAD_MODE="SmartDash Admin", DOCUMENT_STATUS='N', DOCUMENT_FOLDER=folder[:-1],
                                         CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_other_documents', fields={
                                     "A": ["DOC_SEQ_ID"]}, orderBy="DOC_SEQ_ID desc", limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["DOC_SEQ_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder + filename).put(Body=grantData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/grant." + suffix)
                    d=[]
                    file =  open('/tmp/grant.'+suffix, 'r')
                    reader = csv.reader(file)
                    for row in reader:
                        d.append(row)
                    grantDis=Table("mw_grant_disbursement",schema="mint_loan")
                    grantDisReq=Table("mw_grant_disbursement_request",schema="mint_loan")
                    disIDLst=[]
                    mapping = {"Payout Reference Id":"DISBURSAL_ID","Fund Account Name":"BENEFICIARY_NAME","Fund Account Number":"BENEFICIERY_BANK_ACCOUNT_NO","Fund Account Ifsc":"BENEFICIERY_IFSC_CODE"}
                    lst1=[]
                    ind, h = list(zip(*[(i, mapping[x])for i, x in enumerate(d[0]) if x in mapping]))
                    for i in range(1,len(d)):
                        r = dict(list(zip(h, [str(y) for j, y in enumerate(d[i]) if j in ind])))
                        disIDLst.append(r["DISBURSAL_ID"])
                        lst1=[]
                    for i in range(len(disIDLst)):
                        lst=db.runQuery(Query.from_(grantDis).select(grantDis.DISBURSAL_ID).where(grantDis.DISBURSAL_ID==r["DISBURSAL_ID"]))
                        if lst["data"]!=[]:
                            lst=lst["data"][0]["DISBURSAL_ID"]
                            lst1.append(lst)
                        else:
                            lst=None
                    if len(lst1)<1:
                        for i in range(1,len(d)):
                            r = dict(list(zip(h, [str(y) for j, y in enumerate(d[i]) if j in ind])))
                            custID=r["DISBURSAL_ID"].split("_")[1]
                            q2=db.runQuery(Query.from_(grantDisReq).select(grantDisReq.AUTO_ID).where(grantDisReq.CUSTOMER_ID==custID))
                            if q2["data"]!=[]:
                                req_id=str(q2["data"][-1]["AUTO_ID"])
                            else:
                                req_id=str(999999999)
                            inserted = db.Insert(db="mint_loan", table='mw_grant_disbursement', compulsory=False, date=False, 
                                                         **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"CUSTOMER_ID":custID,"REQUEST_ID":req_id,
                                                                             "DOC_SEQ_ID":docID,"DISBURSAL_ID":r["DISBURSAL_ID"],"BENEFICIARY_NAME":r["BENEFICIARY_NAME"],
                                                                             "BENEFICIERY_BANK_ACCOUNT_NO":r["BENEFICIERY_BANK_ACCOUNT_NO"],"BENEFICIERY_IFSC_CODE":r["BENEFICIERY_IFSC_CODE"],
                                                                             "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                            junk = db.Update(db="mint_loan", table="mw_customer_login_credentials", checkAll=False, conditions={"CUSTOMER_ID=":custID}, STAGE='GRANT_PROCESSED')
                            junk = db.Insert(db="mint_loan", table="mw_customer_change_log", compulsory=False, date=False,
                                             CUSTOMER_ID=custID, DATA_KEY='STAGE', DATA_VALUE='GRANT_PROCESSED', CREATED_BY=msgHeader["authLoginID"], 
                                             CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                        if inserted:
                            token = generate(db).AuthToken()
                            if "token" in list(token.keys()):
                                output_dict["data"].update({"error": 0, "message": "successfully"})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["query"]})
                    else:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update({"error": 0, "message": "successfully but file data found"})
                else:
                    token = generate(db).AuthToken()
                    if "token" in list(token.keys()):
                        output_dict["data"].update({"error": 1,"message": "duplicate entries exist, try with other file to upload"})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                db._DbClose_()
                resp.body = json.dumps(output_dict)
            except Exception as ex:
                raise

