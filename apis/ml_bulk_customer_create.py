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
import six
from six.moves import range
from six.moves import zip


class BulkCustomerCreateResource:

    @staticmethod
    def setFilename():
        chars = string.uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Customers created successfully."
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
        folder = "bulk_customer_create/"
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
                        folder + filename, "/tmp/bulk_customer_create." + suffix)
                    citym = Table("mw_city_master", schema="mint_loan")
                    credm = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    bank = Table("mw_bank_ifsc_code", schema="mint_loan")
                    bulkc = Table("mw_bulk_customer_create",
                                  schema="mint_loan")
                    md = xlrd.open_workbook(
                        filename="/tmp/bulk_customer_create." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    mapping = {"city_id": "CITY_ID", "company": "COMPANY", "company_id": "COMPANY_ID", "industry": "INDUSTRY", "name": "NAME", "age": "AGE", "gender": "GENDER",
                               "dob": "DOB", "contact_number": "CONTACT_NUMBER", "pan_no": "PAN_NO", "aadhar_no": "AADHAR_NO",
                               "driving_licence_no": "DRIVING_LICENCE_NO", "monthly_income": "MONTHLY_INCOME", "bank_account_no": "BANK_ACCOUNT_NO",
                               "ifsc_code": "IFSC_CODE", "disbursement_date": "PROPOSED_DATE_OF_DISBURSAL"}
                    nreq = ["city_id", "company", "name", "age", "gender", "dob", "contact_number", "pan_no", "aadhar_no", "driving_licence_no",
                            "monthly_income", "bank_account_no", "ifsc_code", "industry"]
                    # [str(int(sheet.row_slice(i)[0].value)) for i in range(1,sheet.nrows) if len(sheet.row_slice(i))>0]
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
                        # print r
                        ut = utils()
                        try:
                            response = ut.create_customer(userId="+91"+str(r["CONTACT_NUMBER"])) if ("getclarity" in (
                                r["COMPANY"].lower()) if r["COMPANY"] else False) else {"data": {"successFlag": False}}
                            r.update(
                                {"CUSTOMER_CREATED": "1" if response["data"]["successFlag"] else "0"})
                        except:
                            response = {"message": "failed", "authToken": ""}
                            r.update({"CUSTOMER_CREATED": "0"})
                        try:
                            q = db.runQuery(Query.from_(citym).select("CITY").where(
                                citym.CITY_ID == str(r["CITY_ID"])))["data"]
                            response = ut.update_basic_profile(loginId="+91"+str(r["CONTACT_NUMBER"]), authToken=response["header"]["authToken"], companyName="WEBGRANT1", companyID=(r["COMPANY_ID"] if r["COMPANY_ID"] else None) if "COMPANY_ID" in r else None, division=r["COMPANY"] if r["COMPANY"] else None, subDevision=r["INDUSTRY"] if r["INDUSTRY"] else None, currentCity=q[0]["CITY"] if q else "OTHER", address=None, companyNo=str(
                                r["CONTACT_NUMBER"]), dob=r["DOB"] if r["DOB"] else None, experience=None, gender=r["GENDER"] if r["GENDER"] else None, monthlyIncome=r["MONTHLY_INCOME"], education=None, lifeStage=None, name=r["NAME"], email=None, placeOfBirth=None, userCategory=None, alternateMobNo=None) if (r["CUSTOMER_CREATED"] == "1") & ("uber" not in (r["COMPANY"].lower()) if r["COMPANY"] else False) else {"data": {"successFlag": False}}
                            r.update(
                                {"PROFILE_CREATED": "1" if response["data"]["successFlag"] else "0"})
                        except:
                            response = {"message": "failed", "authToken": ""}
                            r.update({"PROFILE_CREATED": "0"})
                        q2 = db.runQuery(Query.from_(bank).select(
                            "MICR").where(bank.IFSC == r["IFSC_CODE"]))["data"]
                        try:
                            response = ut.add_bank(loginId="+91"+str(r["CONTACT_NUMBER"]), authToken=response["header"]["authToken"], accNo=r["BANK_ACCOUNT_NO"], accType="Saving", defaultAccountFlag=True,
                                                   ifscCode=r["IFSC_CODE"], micrCode=str(q2[0]["MICR"]) if q2 else "", personalAccountFlag=True) if r["PROFILE_CREATED"] == "1" else {"data": {"successFlag": False}}
                            r.update(
                                {"BANK_DETAILS_CREATED": "1" if response["data"]["successFlag"] else "0"})
                        except:
                            response = {"message": "failed", "authToken": ""}
                            r.update({"BANK_DETAILS_CREATED": "0"})
                        db._DbClose_()
                        db = DB(msgHeader["authLoginID"], dictcursor=True)
                        q1 = Query.from_(credm).select("CUSTOMER_ID").where(
                            credm.LOGIN_ID == "+91"+str(r["CONTACT_NUMBER"])).where(credm.ACTIVE == "1")
                        q1 = db.runQuery(q1.where(credm.CUSTOMER_ID.notin(
                            Query.from_(bulkc).select(bulkc.CUSTOMER_ID).distinct())))["data"]
                        junk = db.Insert(db="mint_loan", table="mw_bulk_customer_create", compulsory=False, date=False, **utils.mergeDicts({k: v for k, v in six.iteritems(r) if v}, {"CREATED_BY": msgHeader["authLoginID"], "CREATED_DATE": (datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"), "DONATION_STAGE": (
                            "VERIFIED" if (("getclarity" not in (r["COMPANY"].lower()) if r["COMPANY"] else False) | (r["BANK_DETAILS_CREATED"] == "1")) else "AWAITING_BANK_DETAILS" if r["PROFILE_CREATED"] == "1" else "AWAITING_KYC"), "CUSTOMER_ID": (str(q1[0]["CUSTOMER_ID"]) if q1 else "0"), "SOURCE": "BULK_UPLOAD"}))
                        utils.logger.debug(
                            "Request sent to java backend", extra=logInfo)
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
