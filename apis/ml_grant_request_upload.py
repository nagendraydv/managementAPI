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


class grantRequestUploadResource:

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
            #print(data["transactionType"],data["serviceProvider"])
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
                    md = xlrd.open_workbook(filename="/tmp/grant." + suffix, encoding_override='unicode-escape')
                    #md = xlrd.open_workbook(filename="/home/nagendra/Downloads/grant_disbursement_request2." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    data1=[]
                    bulkCustCreate=Table("mw_bulk_customer_create",schema="mint_loan")
                    aadharStatus=Table("mw_aadhar_status",schema="mint_loan")
                    custBankAcc=Table("mw_cust_bank_account_verification",schema="mint_loan")
                    custCred=Table("mw_customer_login_credentials",schema="mint_loan")
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    mapping = {"CUSTOMER_ID":"CUSTOMER_ID","PROGRAM":"PROGRAM","PROGRAM_DETAILS":"PROGRAM_DETAILS","AMOUNT":"AMOUNT"}
                    ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                    disID = db.Query(db="mint_loan", primaryTable='mw_grant_disbursement', fields={
                                     "A": ["DISBURSAL_ID"]}, orderBy="REQUEST_ID desc", limit=1)
                    if disID["data"] != []:
                        disID=str(disID["data"][0]["DISBURSAL_ID"])
                        disID=int(disID.split('_')[-1])
                    else:
                        disID = None
                    for i in range(1,len(d)):
                        r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                        inserted = db.Insert(db="mint_loan", table='mw_grant_disbursement_request', compulsory=False, date=False, 
                                                     **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"CUSTOMER_ID":str(r["CUSTOMER_ID"]), "AMOUNT":r["AMOUNT"],
                                                                         "DOC_SEQ_ID":docID,"PROGRAM":r["PROGRAM"],"PROGRAM_DETAILS":r["PROGRAM_DETAILS"],
                                                                         "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                        ID = db.Query(db="mint_loan", primaryTable='mw_grant_disbursement_request', fields={
                                     "A": ["AUTO_ID"]}, orderBy="AUTO_ID desc", limit=1)
                        if ID["data"] != []:
                            ID = str(ID["data"][0]["AUTO_ID"])
                        else:
                            ID = None
                        disID=disID+1
                        if r["PROGRAM_DETAILS"]!='':
                            sql_select = "SELECT '3434472972820579' AS 'RazorpayX Account Number','' AS 'Payout Amount', 'INR' AS 'Payout Currency', 'IMPS' AS 'Payout Mode', 'payout' AS 'Payout Purpose', 'Kotak Bank Grant' AS 'Payout Narration', CONCAT('DGT_',A.`CUSTOMER_ID`,'_','%s') AS 'Payout Reference Id', '' AS 'Fund Account Id', 'bank_account' AS 'Fund Account Type', A.`NAME` AS 'Fund Account Name', A.`IFSC_CODE` AS 'Fund Account Ifsc', A.BANK_ACCOUNT_NO AS 'Fund Account Number', '' AS 'Fund Account Vpa', 'customer' AS 'Contact Type', A.`NAME` AS 'Contact Name', '' AS 'Contact Email', A.`CONTACT_NUMBER` AS 'Contact Mobile', CONCAT('DGT_',A.`CUSTOMER_ID`,'_','%s') AS 'Contact Reference Id', '' AS 'notes[place]', '' AS 'notes[code]' FROM `mint_loan`.`mw_bulk_customer_create` AS A LEFT OUTER JOIN `mint_loan`.`mw_aadhar_status` AS B ON A.CUSTOMER_ID=B.CUSTOMER_ID LEFT OUTER JOIN `mint_loan`.`mw_cust_bank_account_verification` AS C ON C.CUSTOMER_ID=A.CUSTOMER_ID LEFT OUTER JOIN `mint_loan`.`mw_customer_login_credentials` AS D ON D.CUSTOMER_ID=A.CUSTOMER_ID WHERE A.COMPANY='%s' AND A.INDUSTRY='%s' AND D.STAGE='GRANT_APPROVED' AND B.VERIFIED=1 AND C.VERIFIED=1 AND A.CUSTOMER_ID='%s'"%(disID, disID, r["PROGRAM"], r["PROGRAM_DETAILS"],r["CUSTOMER_ID"])
                            junk = db.dictcursor.execute(sql_select)
                            data = db.dictcursor.fetchall()
                        else:
                            sql_select = "SELECT '3434472972820579' AS 'RazorpayX Account Number', '' AS 'Payout Amount', 'INR' AS 'Payout Currency', 'IMPS' AS 'Payout Mode', 'payout' AS 'Payout Purpose', 'Kotak Bank Grant' AS 'Payout Narration', CONCAT('DGT_',A.`CUSTOMER_ID`,'_','%s') AS 'Payout Reference Id', '' AS 'Fund Account Id', 'bank_account' AS 'Fund Account Type', A.`NAME` AS 'Fund Account Name', A.`IFSC_CODE` AS 'Fund Account Ifsc', A.BANK_ACCOUNT_NO AS 'Fund Account Number', '' AS 'Fund Account Vpa', 'customer' AS 'Contact Type', A.`NAME` AS 'Contact Name', '' AS 'Contact Email', A.`CONTACT_NUMBER` AS 'Contact Mobile', CONCAT('DGT_',A.`CUSTOMER_ID`,'_','%s') AS 'Contact Reference Id', '' AS 'notes[place]', '' AS 'notes[code]' FROM `mint_loan`.`mw_bulk_customer_create` AS A LEFT OUTER JOIN `mint_loan`.`mw_aadhar_status` AS B ON A.CUSTOMER_ID=B.CUSTOMER_ID LEFT OUTER JOIN `mint_loan`.`mw_cust_bank_account_verification` AS C ON C.CUSTOMER_ID=A.CUSTOMER_ID LEFT OUTER JOIN `mint_loan`.`mw_customer_login_credentials` AS D ON D.CUSTOMER_ID=A.CUSTOMER_ID WHERE A.COMPANY='%s' AND D.STAGE='GRANT_APPROVED' AND B.VERIFIED=1 AND C.VERIFIED=1 AND A.CUSTOMER_ID='%s'"%(disID, disID, r["PROGRAM"],r["CUSTOMER_ID"])
                            junk = db.dictcursor.execute(sql_select)
                            data = list(db.dictcursor.fetchall())
                        if data!=():
                            data[0].update({"Payout Amount":r["AMOUNT"]})
                        if data!=():
                             data1.append(data[0])
                    if data!=():
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": "successfully","data":data1})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["query"]})
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

