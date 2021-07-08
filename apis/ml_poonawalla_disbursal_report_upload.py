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
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, JoinType, Order
from dateutil.relativedelta import relativedelta
from pypika import functions as fn

import urllib3
from six.moves import range
from six.moves import zip
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class PoonawallaDisbursalUploadResource:

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
        success = "Report uploaded successfully"
        #logInfo = {'api': 'incomeUpload'}
        try:
            data = {"docType": req.get_param(
                "docType"), "forceUpdate": req.get_param("forceUpdate")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            disbursalData = req.get_param("disbursalData")
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "incomeData": incomeData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "disbursal_reports/"
        # print data, data["forceUpdate"]=='1'
        if ((not validate.Request(api='incomeDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (disbursalData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            # if (incomeData.filename.split('.')[-1]!="csv"):
            #    utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = disbursalData.filename.split('.')[-1]
                filename = self.setFilename() + suffix
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
                    #utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                    resp.body = json.dumps(output_dict)
                else:
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
                        bucket, folder + filename).put(Body=disbursalData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/disbursal." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/disbursal." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    custmap = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    aadhar = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    cvalues = Table(
                        "mw_finflux_attribute_code_values", schema="mint_loan")
                    panT = Table("mw_pan_status", schema="mint_loan")
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    report = Table("mw_other_documents", schema="mint_loan")
                    disb = Table("mw_mifos_disbursements", schema="mint_loan")
                    chargeList = [{'amount': '1.7', 'chargeId': 1}, {
                        'amount': '0.306', 'chargeId': 9}]
                    mapping = {"loan id": "LOAN_ID", "applicant id": "APPLICANT_ID", "payment benificiary name": "NAME", "current_city": "CURRENT_CITY",
                               "gross amount": "GROSS_AMOUNT", "pf": "FEE", "net disbursal amount": "NET_AMOUNT", "date": "DISBURSAL_DATE",
                               "credit account number": "ACCOUNT_NO", "ifsc code": "IFSC_CODE"}
                    ind, h = list(zip(*[(i, mapping[x.value.lower()])
                                        for i, x in enumerate(d[0]) if x.value.lower() in mapping]))
                    today = datetime.now().strftime("%Y-%m-%d")
                    loanIDs = [int(ele[ind[0]].value)
                               for ele in d[1:] if ele[ind[0]]]
                    exist = db.runQuery(Query.from_(disb).select(
                        "LOAN_ID").where(disb.LOAN_ID.isin(loanIDs)))["data"]
                    if ((not exist) or ((data["forceUpdate"] in (1, '1')) if "forceUpdate" in data else False)):
                        # cc = Query.from_(loanmaster).select("CUSTOMER_ID").where((loanmaster.LOAN_REFERENCE_ID.isin(loanIDs)) &
                        #                                                         (loanmaster.EXTERNAL_LOAN_ID.notnull()))
                        # Data=db.runQuery(cc)["data"]
                        for i in range(1, len(d)):
                            r = dict(
                                list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                            inserted = db.Insert(db="mint_loan", table="mw_mifos_disbursements", compulsory=False, date=False,
                                                 **utils.mergeDicts(r, {"CREATED_BY": msgHeader["authLoginID"],
                                                                        "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))
                            custID = str(r["APPLICANT_ID"])
                            db.Update(db="mint_loan", table="mw_customer_login_credentials", checkAll=False,
                                      STAGE='LOAN_IN_PROCESS', conditions={"CUSTOMER_ID=": custID})  # , debug=True)
                            db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False,
                                      CUSTOMER_ID=custID, DATA_KEY="STAGE", DATA_VALUE="LOAN_IN_PROCESS", CREATED_BY="CRON",
                                      CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        if True:  # inserted:
                            token = generate(db).AuthToken()
                            if "token" in list(token.keys()):
                                output_dict["data"].update(
                                    {"error": 0, "message": success, "exist": []})
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
                            output_dict["data"].update({"error": 1, "exist": exist,
                                                        "message": "duplicate entries exist, try forceful update if you still wish to upload"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
