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
from six.moves import range
from six.moves import zip
from dbfread import DBF

class feedUploadResource:
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
            data = {"docType": req.get_param("docType"), "transactionType": req.get_param("transactionType"),"serviceProvider":req.get_param("serviceProvider")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            feedData = req.get_param("feedData")
            #print(data["transactionType"],data["serviceProvider"])
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "feed_upload/"
        if ((not validate.Request(api='feedUpload', request={"msgHeader": msgHeader, "data": data})) or
                (feedData.filename.split('.')[-1] not in ("xlsx", "xls","csv",'dbf','txt'))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = feedData.filename.split('.')[-1]
                filename = feedData.filename.split('.')[0] + '.' + suffix
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                #s3 = session.resource('s3')
                #junk = s3.meta.client.head_bucket(Bucket=bucket)
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
                                         UPLOAD_MODE="SmartDash Admin",DOCUMENT_TYPE=data["docType"], DOCUMENT_STATUS='N', DOCUMENT_FOLDER=folder[:-1],
                                         CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_other_documents', fields={
                                     "A": ["DOC_SEQ_ID"]}, orderBy="DOC_SEQ_ID desc", limit=1)
                    #print(docID)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["DOC_SEQ_ID"])
                    else:
                        docID = None
                    #junk = s3.Object(bucket, folder + filename).put(Body=feedData.file.read())
                    #junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/feed." + suffix)
                    purchase=Table("mw_physical_purchase_request",schema="mf_investment")
                    redemption=Table("mw_physical_redemption_request",schema="mf_investment")
                    tax_list=[]
                    if suffix=='xlsx':
                        md = xlrd.open_workbook(filename="/tmp/feed." + suffix, encoding_override='unicode-escape')
                        sheet = md.sheet_by_index(0)
                        d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                        mapping = {"USR_TXN_NO":"tax_no"}
                        ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                        for i in range(1,len(d)):
                            r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                            tax_list.append(r["tax_no"])
                    if suffix=='txt':
                        list1 =[]
                        with open('/tmp/feed.'+str(suffix),'r') as f:
                            file_data=f.read()
                            list1 =file_data.split('\n')
                            for i in range(len(list1)):
                                if list1[i]!='':
                                    tax_list.append(list1[i].split("|")[6])
                    if suffix=='dbf':
                        table = DBF('/home/nagendra/Downloads/amcCodeRMF18.'+str(suffix), load=True)
                        for i in range(len(table.records)):
                            tax_list.append(table.records[i]["USR_TXN_NO"])
                            #print(tax_list)
                    for i in range(len(tax_list)):
                        indict = {"PROCESSED":'1',"FILE_DOC_ID":docID}
                        if (data["transactionType"]=='purchase'):
                            updated=db.Update(db="mf_investment", table="mw_physical_purchase_request", checkAll=False,debug =True,
                                                         conditions={"UNIQUE_ID=": tax_list[i]},**indict)
                        if (data["transactionType"]=='redemption'):
                            updated=db.Update(db="mf_investment", table="mw_physical_redemption_request", checkAll=False,debug =True,
                                                         conditions={"UNIQUE_ID=": tax_list[i]},**indict)
                        if updated:
                            token = generate(db).AuthToken()
                            if "token" in list(token.keys()):
                                output_dict["data"].update({"error": 0, "message": "proccessed successfully"})
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
