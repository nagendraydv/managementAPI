#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 16:45:51 2020

@author: nagendra
"""

from __future__ import absolute_import
from __future__ import print_function
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
from pypika import Query, Table, functions, JoinType, Order
from six.moves import range


class repaymentDataResource:
    def on_get(self, req, resp):
        pass

    def on_post(self, req, resp):
        '''Handles post request'''
        output_dict = {"msgHeader": {"authToken": {}}, "data": {}}
        errors = utils.errors
        success = "file uploaded successfully"
        fields = {"data": {}}
        lrddata = {"data": {}}
        list = []
        try:
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            data = {"docType": req.get_param(
                "docType"), "usdInr": req.get_param("usdInr")}
            incomedata = req.get_param("incomedata")
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"
        folder = "test/"
        if ((not validate.Request(api="fileUploadTest", request={"msgHeader": msgHeader, "data": data})) or
                (incomedata.filename.split('.')[-1] not in ("xlsx", "xls"))):
            output_dict["data"].update(
                {"errors": 1, "message": errors["json"]})
            resp.body = json.dumps(output_dict)
        else:
            try:
                suffix = incomedata.filename.split('.')[-1]
                filename = incomedata.filename
                s3path = s3url+bucket+folder+filename+'/'+suffix
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                s3 = session.resource('s3')
                junk = s3.meta.client.head_bucket(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(
                    falcon.HTTP_400, "connection error", "could not connected")
            try:
                db = DB(msgHeader["authLoginID"], dictcursor="True")
                val_error = validate(db).basicChecks(
                    token=msgHeader["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 0, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    lrd = Table("mw_loan_repayment_data", schema="mint_loan")
                    ld = Table("mw_client_loan_details", schema="mint_loan")
                    inserted = db.Insert(db="mint_loan", table="mw_uber_income_documents", compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))
                    docId = db.Query(db="mint_loan", primaryTable="mw_uber_income_documents", fields={"A": ["AUTO_ID"]},
                                     orderBy="AUTO_IDdesc", limit=1)
                    if docId["data"] != []:
                        docID = str(docId["data"][0]["AUTO_ID"])
                    else:
                        fields["data"].update({"REPAY_AMOUNT1": "", "REPAY_DATETIME1": "", "MODE_OF_PAYMENT1": "", "REPAY_AMOUNT2": "", "REPAY_DATETIME2": "",
                                               "MODE_OF_PAYMENT2": "", "REPAY_AMOUNT3": "", "REPAY_DATETIME3": "", "MODE_OF_PAYMENT3": "", "CURRENT_OUTSTANDING": "", "TOTAL_OUTSTANDING": "",
                                               "TEMP_ALL": ""})
                        docId = None
                        junk = s3.Object(bucket, folder + filename).put(Body=incomedata.file.read())
                        junk = s3.Bucket(bucket).download_file(folder + filename, '/home/nagendra/Documents/loginfile.' + suffix)
                        md = xlrd.open_workbook(filename='/home/nagendra/Documents/loginfile.' + suffix, encoding_override='unicode-escape')
                        sheet = md.sheet_by_index(0)
                        d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                        for ele in d[1:]:
                            # print ele
                            q1 = Query.from_(lm).select(lm.LENDER, lm.STATUS, lm.FUND, lm.LOAN_REFERENCE_ID).where(
                                lm.LOAN_REFERENCE_ID == str(int(ele[0].value)))
                            q2 = Query.from_(lrd).select(lrd.REPAY_AMOUNT, lrd.REPAY_DATETIME, lrd.MODE_OF_PAYMENT).where(
                                (lrd.LOAN_REF_ID == str(int(ele[0].value))) & (lrd.FINFLUX_TRAN_ID.isnull()))
                            q3 = Query.from_(ld).select(ld.CURRENT_OUTSTANDING, ld.TOTAL_OUTSTANDING, ld.PAID_IN_ADVANCE).where(
                                ld.LOAN_MASTER_ID == str(int(ele[0].value)))
                            amt = db.runQuery(Query.from_(lrd).select(lrd.REPAY_AMOUNT).where(
                                (lrd.LOAN_REF_ID == str(int(ele[0].value))) & (lrd.FINFLUX_TRAN_ID.isnull())))
                            fields = utils.camelCase(db.runQuery(q1))
                            # print d
                            lrddata = (db.runQuery(q2))
                            lddata = (db.runQuery(q3))
                            fields["data"][0]["CURRENT_OUTSTANDING"] = lddata["data"][0]["CURRENT_OUTSTANDING"]
                            fields["data"][0]["TOTAL_OUTSTANDING"] = lddata["data"][0]["TOTAL_OUTSTANDING"]
                            # fields["data"][0]["TEMP_ALL"]=lddata["data"][0]["TEMP_ALL"]
                            for i, ele2 in enumerate(lrddata["data"]):
                                fields["data"][0]["REPAY_AMOUNT" +
                                                  str(i+1)] = ele2["REPAY_AMOUNT"]
                                fields["data"][0]["MODE_OF_PAYMENT" +
                                                  str(i+1)] = ele2["MODE_OF_PAYMENT"]
                                #fields["data"][0]["REPAY_DATETIME"+str(i+1)] = ele2["REPAY_DATETIME"]
                                fields["data"][0]["REPAY_DATETIME" + str(i+1)] = (
                                    datetime.fromtimestamp(ele2["REPAY_DATETIME"]).strftime("%y-%m-%d-%H:%M:%S"))
                                amount = [(datum["REPAY_AMOUNT"])
                                          for datum in amt["data"]]
                                TEMP_ALL = sum((value) for value in amount)
                                fields["data"][0]["TEMP_ALL"] = TEMP_ALL
                                # fields["data"][0].update(fields["data"][0])
                            list.append(fields["data"][0])
                            # print fields
                            # data["data"]+=fields["data"].update(fields["data"][0])
                            # data.update({"data":fields["data"]})
                            # fields["data"][0].update({"REPAY_AMOUNT1":"","REPAY_DATETIME1":"","MODE_OF_PAYMENT1":"","REPAY_AMOUNT2":"","REPAY_DATETIME2":"","MODE_OF_PAYMENT2":"","REPAY_AMOUNT3":"","REPAY_DATETIME3":"","MODE_OF_PAYMENT3":""})
                            # fields["data"].extend(lrddata["data"])
                        # print(db.pikastr(q1))
                        # print(db.pikastr(q2))
                            # print fields
                        print(list)
                        if (fields):
                            token = generate(db).AuthToken()
                            #if "token" in list(token.keys()):
                            if token:
                                output_dict.update({"data": list})
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
            except:
                raise
