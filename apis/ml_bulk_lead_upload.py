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
from datetime import time,date
import requests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, JoinType, Order
from dateutil.relativedelta import relativedelta
from pypika import functions as fn
import urllib3
from six.moves import range
from six.moves import zip
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class leadBulkUploadResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits
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
        success = "Lead bulk uploaded successfully"
        logInfo = {'api': 'bulkupload'}
        try:
            data = {"docType": req.get_param("docType"), "name":req.get_param("name"), "product":req.get_param("product"),
                    "preferredModeOfContact":req.get_param("preferredModeOfContact"), "dndModeOfContact":req.get_param("dndModeOfContact"),
                    "company":req.get_param("company"), "city":req.get_param("city"), "campaignID":req.get_param("campaignID"),
                    "campaignName":req.get_param("campaignName"), "createCustomer":req.get_param("createCustomer")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),"timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            bulkLeadData = req.get_param("bulkLeadData")
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "bulk_lead/"
        if ((not validate.Request(api='bulkLeadUpload', request={"msgHeader": msgHeader, "data": data})) or
                (bulkLeadData.filename.split('.')[-1] not in ("xlsx", "csv","xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = bulkLeadData.filename.split('.')[-1]               
                filename = bulkLeadData.filename.split('.')[0] + '.' + suffix
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
                                         UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         DOCUMENT_FOLDER=folder,DOCUMENT_STATUS='N',
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_other_documents', fields={"A": ["DOC_SEQ_ID"]}, orderBy="DOC_SEQ_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["DOC_SEQ_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder + filename).put(Body=bulkLeadData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/lead." + suffix)
                    md = xlrd.open_workbook(filename="/tmp/lead." + suffix, encoding_override='unicode-escape')
                    #md = xlrd.open_workbook(filename="/home/nagendra/Downloads/Bijnis_onboarding_responses_14Jan59." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    campaign_header_mapping = Table("campaign_header_mapping",schema="mw_lead")
                    cm = Table("campaign_master", schema= "mw_lead")
                    cred = Table("mw_customer_login_credentials", schema="mint_loan")
                    bank = Table("mw_bank_ifsc_code", schema="mint_loan")
                    q1 = Query.from_(campaign_header_mapping).select(campaign_header_mapping.file_header,campaign_header_mapping.CORRESPONDING_HEADER)
                    mapp = db.runQuery(q1.where(campaign_header_mapping.CAMPAIGN_NAME==data["campaignName"]))
                    mapping = dict((ele["file_header"],ele["CORRESPONDING_HEADER"]) for ele in mapp["data"])
                    ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                    if data["campaignID"] not in (0, "0"):
                        campaign_id = data["campaignID"]
                    else:
                        inserted = db.Insert(db="mw_lead", table='campaign_master', compulsory=False, date=False, 
                                             **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"NAME":data["name"],
                                                                 "PRODUCT":data["product"], "IS_ACTIVE":str(1),"IS_RECURRING":0,
                                                                 "PREFERRED_MODE_OF_CONTACT":data["preferredModeOfContact"],
                                                                 "DND_MODE_OF_CONTACT":data["dndModeOfContact"],
                                                                 "created_date":(datetime.utcnow() +
                                                                                 timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                        compaign_id = db.Query(db="mw_lead", primaryTable='campaign_master', fields={"A": ["CAMPAIGN_ID"]},
                                               orderBy="CAMPAIGN_ID desc", limit=1)
                        if compaign_id["data"] != []:
                            compaign_id = str(compaign_id["data"][0]["CAMPAIGN_ID"])
                            inserted = db.Insert(db="mw_lead", table='lead_audit_trail', compulsory=False, date=False, 
                                                 **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"LEAD_ID":lead_id,
                                                                     "CAMPAIGN_ID":campaign_id, "DATA_KEY":"CAMPAIGN_CREATED",
                                                                     "DATA_VALUE":data["name"],
                                                                     "created_date":(datetime.utcnow() +
                                                                                     timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                        else:
                            compaign_id = None                    
                    for i in range(1,len(d)):
                        r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                        # populate following keys using a database table query
                        prof_keys = ["PRIMARY_PHONE_NUMBER", "PAN_NUMBER", "AADHAR_NUMBER", "CITY", "UNIQUE_ID", "GSTIN_NUMBER", "FIRST_NAME",
                                     "LAST_NAME", "ADDRESS", "PINCODE", "EMAIL_ID", "BANK_ACCOUNT_NO", "IFSC_CODE", "PENNY_DROP_VERIFICATION",
                                     "ACCOUNT_DETAILS_VERIFIED", "ELIGIBLE_LIMIT"]
                        adv_data = {data_key:data_value for data_key,data_value in r.items() if data_key not in prof_keys}
                        prof_data = {data_key:(data_value if data_value else None) for data_key,data_value in r.items() if (data_key in prof_keys) and (data_value)}
                        if "CITY" not in prof_data:
                            prof_data.update({"CITY":data["CITY"] if "CITY" in data else "Other"})
                        if "FIRST_NAME" not in prof_data:
                            prof_data.update({"FIRST_NAME":r["COMPANY_NAME"].split(" ")[0] if prof_data["COMPANY_NAME"] else None})
                        if "LAST_NAME" not in prof_data:
                            prof_data.update({"LAST_NAME":r["COMPANY_NAME"].split(" ")[-1] if prof_data["COMPANY_NAME"] else None})
                        #prof_data.pop("COMPANY_NAME")
                        inserted = db.Insert(db="mw_lead", table='lead_profile', compulsory=False, date=False, debug=True,
                                             **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"COMPANY":data["company"],
                                                                 "CAMPAIGN_ID":str(campaign_id),
                                                                 "created_date":(datetime.utcnow() +
                                                                                 timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")},
                                                                prof_data))
                        lead_id = db.Query(db="mw_lead", primaryTable='lead_profile', fields={
                                         "A": ["LEAD_ID"]}, orderBy="LEAD_ID desc", limit=1)
                        ut = utils()
                        if lead_id["data"] != []:
                            lead_id = str(lead_id["data"][0]["LEAD_ID"])
                            if data["createCustomer"] in (1, "1"):
                                try:
                                    response = ut.create_customer(userId="+91"+str(prof_data["PRIMARY_PHONE_NUMBER"]))
                                    db._DbClose_()
                                    db = DB(msgHeader["authLoginID"], dictcursor=True)
                                    cid = db.runQuery(Query.from_(cred).select(cred.CUSTOMER_ID).where(cred.LOGIN_ID==response["header"]["loginId"]))
                                    cid = cid["data"][0]["CUSTOMER_ID"] if cid["data"] else "0"
                                except:
                                    cid = "0"
                                try:
                                    response = (ut.update_basic_profile(loginId="+91"+str(prof_data["PRIMARY_PHONE_NUMBER"]),
                                                                        authToken=response["header"]["authToken"], companyName=data["company"],
                                                                        companyID=((prof_data["UNIQUE_ID"] if prof_data["UNIQUE_ID"] else "")
                                                                                   if "UNIQUE_ID" in r else ""),
                                                                        division=r["COMPANY"] if "COMPANY" in r else "",
                                                                        subDevision=r["INDUSTRY"] if "INDUSTRY" in r else "",
                                                                        currentCity=prof_data["CITY"] if "CITY" in prof_data else "OTHER",
                                                                        address=prof_data["ADDRESS"] if "ADDRESS" in prof_data else "",
                                                                        companyNo=str(prof_data["PRIMARY_PHONE_NUMBER"]),
                                                                        dob=r["DOB"] if "DOB" in r else "", experience="",
                                                                        gender=r["GENDER"] if "GENDER" in r else "",
                                                                        monthlyIncome=r["MONTHLY_INCOME"] if "MONTHLY_INCOME" in r else "",
                                                                        education="", lifeStage="",
                                                                        name=(((prof_data["FIRST_NAME"] + " ") if "FIRST_NAME" in prof_data else "") +
                                                                              (prof_data["LAST_NAME"] if "LAST_NAME" in prof_data else "")),
                                                                        email=prof_data["EMAIL_ID"] if "EMAIL_ID" in prof_data else "",
                                                                        placeOfBirth="", userCategory="", alternateMobNo="")
                                                if cid != "0" else {"data": {"successFlag": False}})
                                    #utils.logger.info("api response: " + json.dumps(response, extra=logInfo))
                                    #profileres=response
                                    profileUpdated = "1" if response["data"]["successFlag"] else "0"
                                except:
                                    profileUpdated = "0"
                                if ("IFSC_CODE" in prof_data) and ("BANK_ACCOUNT_NO" in prof_data):
                                    micr = db.runQuery(Query.from_(bank).select("MICR").where(bank.IFSC == prof_data["IFSC_CODE"]))["data"]
                                    try:
                                        res = ut.add_bank(loginId="+91"+str(prof_data["PRIMARY_PHONE_NUMBER"]),
                                                               authToken=response["header"]["authToken"], accNo=prof_data["BANK_ACCOUNT_NO"],
                                                               accType="Saving", defaultAccountFlag=True,
                                                               ifscCode=prof_data["IFSC_CODE"], micrCode=str(micr[0]["MICR"]) if micr else "",
                                                               personalAccountFlag=True)
                                        #utils.logger.info("api response: " + json.dumps(res, extra=logInfo))
                                        #bankres=response
                                        bankAdded = "1" if res["data"]["successFlag"] else "0"
                                    except:
                                        bankAdded = "0"
                                else:
                                    bankAdded = "0"
                            else:
                                profileUpdated, cid, bankAdded = "0","0","0"
                            updated = db.Update(db="mw_lead", table="lead_profile", conditions={"LEAD_ID = ":str(lead_id)}, CUSTOMER_ID=str(cid), PROFILE_CREATED=profileUpdated, BANK_ADDED=bankAdded)
                        else:
                            lead_id = None
                        #print(adv_data)
                        for k,v in adv_data.items():
                            inserted = db.Insert(db="mw_lead", table='lead_additional_details', compulsory=False, date=False, 
                                                 **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"LEAD_ID":str(lead_id), "CAMPAIGN_ID":str(campaign_id),
                                                                     "DATA_KEY":str(k), "DATA_VALUE":str(v),
                                                                     "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                            q2 = Query.from_(cm).select(cm.PRODUCT).where(cm.CAMPAIGN_ID==campaign_id)
                            prod = db.runQuery(q2)["data"]
                            if prod!=[]:
                                prod = prod[0]["PRODUCT"]
                            else:
                                prod = None
                            inserted = db.Insert(db="mw_lead", table='mw_lead_product_mapping', compulsory=False, date=False, debug=False,
                                                     **utils.mergeDicts({"CREATED_BY":msgHeader["authLoginID"],"LEAD_ID":str(lead_id),
                                                                         "CAMPAIGN_ID":str(campaign_id), "PRODUCT_ID": "prodID", "PRODUCT_NAME": prod,
                                                                         "ASSIGNED_TO": "","CREATED_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                    #inserted = True    
                    if inserted:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success,"response":response,"cid":cid,"profileUpdated":profileUpdated})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                else:
                    token = generate(db).AuthToken()
                    if "token" in list(token.keys()):
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update({"error": 1, "message":"Duplicate_file - file already exist"})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)        
                db._DbClose_()
            except Exception as ex:
                raise