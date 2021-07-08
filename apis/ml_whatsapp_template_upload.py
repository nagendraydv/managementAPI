from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import requests
#import mimetypes
import string
#import os
#import subprocess
import xlrd
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions
from six.moves import range
from six.moves import zip

class whatsappTemplateUploadResource:
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
        logInfo = {'api': 'whatsappTemplateUpload'}
        try:
            data = {"docType": req.get_param("docType")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            templateData = req.get_param("whatsTemplateData")
            #print(data["transactionType"],data["serviceProvider"])
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "whatstemplateupload/"
        if ((not validate.Request(api='whatsTemplateUpload', request={"msgHeader": msgHeader, "data": data})) or
                (templateData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = templateData.filename.split('.')[-1]
                filename = templateData.filename.split('.')[0] + '.' + suffix
                s3path = s3url + bucket + '/' + folder + filename
                #session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        #aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                #s3 = session.resource('s3')
                #junk = s3.meta.client.head_bucket(Bucket=bucket)
                s3=utils().s3_connect()
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(token=msgHeader["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                elif db.Query(primaryTable="mw_other_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] != 0:
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
                    junk = s3.Object(bucket, folder + filename).put(Body=templateData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/feed." + suffix)
                    lcmap=Table("mw_customer_login_credentials_map",schema="mint_loan")
                    #redemption=Table("mw_physical_redemption_request",schema="mf_investment")
                    #if suffix=='xlsx':
                    customer_id= []
                    mobile_number=[]
                    def chunks(lst, n):
                        for i in range(0, len(lst), n):
                            yield lst[i:i + n]
                    #md = xlrd.open_workbook(filename="/home/nagendra/Documents/whatsappUpload5." + suffix, encoding_override='unicode-escape')
                    md = xlrd.open_workbook(filename="/tmp/feed." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    if str(d[0][0].value) == "customer_id":
                        mapping = {"customer_id":"CUSTOMER_ID"}
                        ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                        for i in range(1,len(d)):
                            r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                            customer_id.append(r["CUSTOMER_ID"])
                        #print(customer_id)
                        customer_id_chunks=(list(chunks(customer_id,2)))
                        baseurl = 'https://dev.mintwalk.com/tomcat/WhatsappService/whatsapp/whatsAppTemplate'
                        headers={'Content-type': 'application/json'}
                        auth = utils.mifos_auth
                        #req_list=[]
                        for i in range(len(customer_id_chunks)):
                            req_list=[]
                            for j in range(len(customer_id_chunks[i])):
                                cust_id = customer_id_chunks[i][j]
                                q = Query.from_(lcmap).select(lcmap.LOGIN_ID).where((lcmap.CUSTOMER_ID==cust_id) & (lcmap.ACTIVE==1))
                                #print(db.pikastr(q))
                                mob = db.runQuery(q)["data"]
                                #print(mob)
                                mobile = mob[0]["LOGIN_ID"][3:] if mob!=[] else ''
                                req_dict = {"mobile":mobile,"templateData":[],"customerId":cust_id,"leadId":""}
                                req_list.append(req_dict)
                                #print(req_list)
                                payload={"req":req_list,"language":"marathi","templateName":"one_family_hindi_marathi_template","flowId":"FLOW1006"}
                            #print(json.dumps(payload))
                            r = requests.post(baseurl , data=json.dumps(payload),headers=headers,auth=auth, verify=False)
                            utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                            res=r.json()
                            #print(res)
                    if str(d[0][0].value)=='phone_number':
                        mapping={"phone_number":"PHONE_NUMBER"}
                        ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                        for i in range(1,len(d)):
                            r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                            mobile_number.append(r["PHONE_NUMBER"])
                        mobile_number_chunks=(list(chunks(mobile_number,50)))
                        baseurl = 'https://dev.mintwalk.com/tomcat/WhatsappService/whatsapp/whatsAppTemplate'
                        headers={'Content-type': 'application/json'}
                        auth = utils.mifos_auth
                        #req_list=[]
                        for i in range(len(mobile_number_chunks)):
                            req_list=[]
                            for j in range(len(mobile_number_chunks[i])):
                                mobile = mobile_number_chunks[i][j]
                                #q = Query.from_(lcmap).select(lcmap.LOGIN_ID).where((lcmap.CUSTOMER_ID==cust_id) & (lcmap.ACTIVE==1))
                                #print(db.pikastr(q))
                                #mob = db.runQuery(q)["data"]
                                #print(mob)
                                #mobile = mob[0]["LOGIN_ID"][3:] if mob!=[] else ''
                                req_dict = {"mobile":mobile,"templateData":[],"customerId":"","leadId":""}
                                req_list.append(req_dict)
                                #print(req_list)
                                payload={"req":req_list,"language":"marathi","templateName":"one_family_hindi_marathi_template","flowId":"FLOW1006"}
                            #print(json.dumps(payload))
                            r = requests.post(baseurl , data=json.dumps(payload),headers=headers,auth=auth, verify=False)
                            utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                            res=r.json()
                            #print(res)
                    if res["status"]:
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
