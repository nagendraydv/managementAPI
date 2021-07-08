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


class MandateDataUploadResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Mandate data imported successfully"
        logInfo = {'api': 'mandateDataUpload'}
        try:
            data = {"docType": req.get_param("docType"), "format": req.get_param("format")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            mandateData = req.get_param("mandateData")
            # print data, msgHeader
            #utils.logger.debug("Request: " + json.dumps({"data":data, "msgHeader": msgHeader, "mandateData": mandateData.filename}), extra=logInfo)
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "mandate_info/"
        if ((not validate.Request(api='mandateDataUpload', request={"msgHeader": msgHeader, "data": data})) or
                (mandateData.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            # if (mandateData.filename.split('.')[-1] not in ("xlsx", "xls")):
            #utils.logger.error("ExecutionError: Invalid file format")
            # else:
            #    utils.logger.error("ExecutionError: Invalid request")
        else:
            try:
                suffix = mandateData.filename.split('.')[-1]
                filename = self.setFilename() + suffix
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                s3 = session.resource('s3')
                junk = s3.meta.client.head_bucket(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                # falcon.HTTPError(falcon.HTTP_400,'Connection error', 'Could not establish S3 connection')
                raise
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
                    inserted = db.Insert(db="mint_loan", table='mw_mandate_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"), MANDATE_FORMAT=data["format"])
                    docID = db.Query(db="mint_loan", primaryTable='mw_mandate_documents', fields={
                                     "A": ["AUTO_ID"]}, orderBy="AUTO_ID desc", limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(
                        bucket, folder + filename).put(Body=mandateData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/mandate." + suffix)
                    md = xlrd.open_workbook(filename="/tmp/mandate." + suffix)
                    sheet = md.sheet_by_index(0)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    bank = Table("mw_cust_bank_detail", schema="mint_loan")
                    mapp = Table("mw_mandate_header_mapping",
                                 schema="mint_loan")
                    mandate = Table(
                        "mw_physical_mandate_status", schema="mint_loan")
                    q = Query.from_(mapp).select("MW_KEY", "MANDATE_KEY").where(
                        mapp.MANDATE_FORMAT == data["format"])
                    mapping = {}
                    for x in db.runQuery(q)["data"]:
                        mapping.update({x["MANDATE_KEY"]: x["MW_KEY"]})
                    d2 = []
                    ind, h = list(
                        zip(*[(i, mapping[x.value]) for i, x in enumerate(d[0]) if x.value in mapping]))
                    for i in range(1, len(d)):
                        d2.append(dict(list(zip(h, [(str(y.value) if y.ctype != 3 else
                                                     xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d"))
                                                    for j, y in enumerate(d[i]) if j in ind]))))
                    for ele in d2:
                        q = Query.from_(bank).select("CUSTOMER_ID").where(
                            bank.ACCOUNT_NO == ele["ACCOUNT_NUMBER"])
                        custID = db.runQuery(
                            q.where(bank.IFSC_CODE == ele["IFSC_CODE"]))["data"]
                        custID = str(custID[0]["CUSTOMER_ID"]
                                     ) if custID else "0"
                        q = Query.from_(mandate).select(mandate.MANDATE_STATUS).where(
                            mandate.IFSC_CODE == ele["IFSC_CODE"])
                        ex = db.runQuery(q.where((mandate.ACCOUNT_NUMBER == ele["ACCOUNT_NUMBER"]) & (
                            mandate.AMOUNT == ele["AMOUNT"])))["data"]
                        # print ele, ex, custID
                        if ex:
                            #print (ex[0]["MANDATE_STATUS"].lower() != 'active'), (ele["MANDATE_STATUS"]==ex[0]["MANDATE_STATUS"])
                            if (ex[0]["MANDATE_STATUS"].lower() != 'active') or (ele["MANDATE_STATUS"] == ex[0]["MANDATE_STATUS"]):
                                inserted = db.Update(db="mint_loan", table="mw_physical_mandate_status", checkAll=False,  # debug=True,
                                                     conditions={"ACCOUNT_NUMBER=": ele["ACCOUNT_NUMBER"], "AMOUNT=": ele["AMOUNT"],
                                                                 "IFSC_CODE=": ele["IFSC_CODE"]},
                                                     **utils.mergeDicts(ele, {"CUSTOMER_ID": custID, "MODIFIED_BY": "ADMIN", "DOC_ID": docID,
                                                                              "MODIFIED_DATE": datetime.now().strftime("%Y-%m-%d")}))
                            else:
                                inserted = True
                        else:
                            inserted = db.Insert(db="mint_loan", table="mw_physical_mandate_status", compulsory=False, date=False,
                                                 **utils.mergeDicts(ele, {"CUSTOMER_ID": custID, "CREATED_BY": "ADMIN", "DOC_ID": docID,
                                                                          "CREATED_DATE": datetime.now().strftime("%Y-%m-%d")}))
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
                    # print output_dict
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise
