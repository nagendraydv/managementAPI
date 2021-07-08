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


class UberIncomeUploadResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

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
        folder = "uber_income/"
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
                elif db.Query(primaryTable="mw_uber_income_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] == 0:
                    inserted = db.Insert(db="mint_loan", table='mw_uber_income_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         DOCUMENT_TYPE=data["docType"], UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"))  # %Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_uber_income_documents', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["AUTO_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder +
                                     filename).put(Body=incomeData.file.read())
                    junk = s3.Bucket(bucket).download_file(
                        folder + filename, "/tmp/income." + suffix)
                    md = xlrd.open_workbook(
                        filename="/tmp/income." + suffix, encoding_override='unicode-escape')
                    sheet = md.sheet_by_index(1)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    cred = Table("mw_customer_login_credentials",
                                 schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    custdata = Table("mw_customer_data", schema="mint_loan")
                    panstatus = Table("mw_pan_status", schema="mint_loan")
                    mapping = {"city_id": "CITY_ID", "week": "WEEK", "partner_uuid": "DRIVER_UUID", "contact_number": "CONTACT_NUMBER",
                               "firstname": "FIRST_NAME", "lastname": "LAST_NAME", "total_partner_vehicles": "TOTAL_PARTNER_VEHICLES",
                               "active_vehicles": "ACTIVE_VEHICLES", "num_drivers": "NUM_DRIVERS", "first_trip_week": "FIRST_TRIP_WEEK",
                               "rating": "RATING", "partner_type": "PARTNER_TYPE", "xli_tag": "XLI_TAG", "present_status": "PRESENT_STATUS",
                               "dco": "DCO", "license_num": "LICENSE_NUMBER", "pan_num": "PAN_NUMBER", "uber_plus_tier": "UBER_PLUS_TIER",
                               "address": "ADDRESS", "make": "MAKE", "model": "MODEL", "total_trips_completed": "NO_OF_TRIPS",
                               "total_earnings": "TOTAL_EARNINGS", "gb_usd": "GB", "total_earnings": "INCOME", "edi_usd": "EDI_USD",
                               "edi_inr": "EDI_INR", "organic_earnings_inr": "DRIVER_ORGANIC_EARNINGS", "incentives_inr": "INCENTIVES_INR"}
                    nreq = ["city_id", "contact_number", "total_partner_vehicles", "active_vehicles", "rating",  # "uber_plus_tier",#"num_drivers",
                            "xli_tag", "total_trips_completed", "total_earnings", "gb_inr", "organic_earnings_inr", "incentives_inr"]
                    # nreq is number required
                    ind, h, n = list(zip(*[(i, mapping[x.value.lower()], x.value.lower() in nreq)
                                           for i, x in enumerate(d[0]) if x.value.lower() in mapping]))
                    d2 = []
                    dco = {"Driver Cum Owner": "1", "Driver Under Partner": "0",
                           "Non Driving partner/ Fleet Owner": "2"}
                    Q = Query.from_(cred).select("CUSTOMER_ID").where(
                        cred.STAGE.isin(['REJECTED']))
                    for i in range(1, len(d)):
                        r = dict(list(zip(h, [(((str(int(y.value)) if y.value.is_integer() else str(y.value) if y.value != 'NA' else None) if y.ctype == 2
                                                else (y.value.encode('latin1', 'ignore') if y.value != 'NA' else None) if y.ctype == 1 else
                                                xlrd.xldate.xldate_as_datetime(y.value, md.datemode).strftime("%Y-%m-%d") if y.ctype == 3 else "")
                                               if (not n[j]) else ((str(int(y.value)) if y.value.is_integer() else str(y.value)) if y.ctype == 2 else 0))
                                              for j, y in enumerate(d[i]) if j in ind])))  # +
                        # [("INCOME", "%.2f"%(d[i][ind[h.index("INCOME_USD")]].value*float(data["usdInr"])))])
                        if "DCO" in r:
                            r["DCO"] = (dco[r["DCO"]] if r["DCO"]
                                        in dco else "9")
                        q = Query.from_(income).select("AUTO_ID").where(
                            income.DRIVER_UUID == r["DRIVER_UUID"])
                        exists = db.runQuery(
                            q.where(income.WEEK == r["WEEK"]))["data"]
                        if (not exists):
                            custID = db.runQuery(Query.from_(income).select("CUSTOMER_ID").where((income.DRIVER_UUID == r["DRIVER_UUID"]) &
                                                                                                 (income.CUSTOMER_ID.notin([0, "0"]))))
                            custID = str(
                                custID["data"][0]["CUSTOMER_ID"]) if custID["data"] else ""
                            if (custID == '') and ((str(r["CONTACT_NUMBER"]) != "0") or (r["CONTACT_NUMBER"] is not None)):
                                q = Query.from_(custdata).select("CUSTOMER_ID").where((custdata.DATA_VALUE.like(
                                    "%"+str(r["CONTACT_NUMBER"]))) & (custdata.DATA_KEY == 'COMPANY_NUMBER'))
                                custID = db.runQuery(q.where((custdata.CUSTOMER_ID.notin(Q)) &
                                                             (custdata.ARCHIVED == 'N')).orderby(custdata.ID, order=Order.desc))["data"]
                                custID = (str(custID[0]["CUSTOMER_ID"]) if custID else
                                          db.runQuery(q.where(custdata.CUSTOMER_ID.notin(Q)).orderby(custdata.ID, order=Order.desc))["data"])
                                custID = custID if type(custID) == str else (
                                    str(custID[0]["CUSTOMER_ID"]) if custID else "")
                            # if not custID:
                            #    custID = db.runQuery(Query.from_(cred).select(cred.CUSTOMER_ID).where((cred.LOGIN_ID.like("%"+r["CONTACT_NUMBER"]))
                            #                                                                          & (cred.CUSTOMER_ID.notin(Q))))
                            #    custID = str(custID["data"][0]["CUSTOMER_ID"]) if custID["data"] else ""
                            # if not custID:
                            #    q = Query.from_(panstatus).select("CUSTOMER_ID").where((panstatus.PAN_NO.like("%"+r["PAN_NUMBER"])) &
                            #                                                                (panstatus.CUSTOMER_ID.notin(Q)))
                            #    custID = db.runQuery(q.where(panstatus.ARCHIVED=='N'))["data"]
                            #    custID = str(custID[0]["CUSTOMER_ID"]) if custID else db.runQuery(q)["data"]
                            #    custID = custID if type(custID)==str else (str(custID[0]["CUSTOMER_ID"]) if custID else "0")
                            q1 = db.runQuery(Query.from_(income).select(
                                income.DRIVER_UUID).distinct().where(income.CUSTOMER_ID == custID))["data"]
                            if (r["DRIVER_UUID"] not in [ele["DRIVER_UUID"] for ele in q1]) & (q1 != []):
                                custID = "0"
                            d2.append(utils.mergeDicts(r, {"CUSTOMER_ID": custID, "CREATED_BY": msgHeader["authLoginID"], "SOURCE": "UBER",
                                                           "DOC_ID": docID, "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))
                            # print r, custID
                        # else:
                        #    q = Query.from_(custdata).select("CUSTOMER_ID").where(custdata.DATA_VALUE.like("%"+str(r["CONTACT_NUMBER"])))
                        #    custID = db.runQuery(q.where((custdata.CUSTOMER_ID.notin(Q)) &
                        #                                      (custdata.ARCHIVED=='N')).orderby(custdata.ID, order=Order.desc))["data"]
                        #    custID = (str(custID[0]["CUSTOMER_ID"]) if custID else
                        #              db.runQuery(q.where(custdata.CUSTOMER_ID.notin(Q)).orderby(custdata.ID, order=Order.desc))["data"])
                        #    custID = custID if type(custID)==str else (str(custID[0]["CUSTOMER_ID"]) if custID else "")
                        #    if not custID:
                        #        custID = db.runQuery(Query.from_(cred).select(cred.CUSTOMER_ID).where((cred.LOGIN_ID.like("%"+r["CONTACT_NUMBER"]))
                        #                                                                              & (cred.CUSTOMER_ID.notin(Q))))
                        #        custID = str(custID["data"][0]["CUSTOMER_ID"]) if custID["data"] else ""
                        #    if not custID:
                        #        q = Query.from_(panstatus).select("CUSTOMER_ID").where((panstatus.PAN_NO.like("%"+r["PAN_NUMBER"])) &
                        #                                                                    (panstatus.CUSTOMER_ID.notin(Q)))
                        #        custID = db.runQuery(q.where(panstatus.ARCHIVED=='N'))["data"]
                        #        custID = str(custID[0]["CUSTOMER_ID"]) if custID else db.runQuery(q)["data"]
                        #        custID = custID if type(custID)==str else (str(custID[0]["CUSTOMER_ID"]) if custID else "0")
                        #    q1 = db.runQuery(Query.from_(income).select(income.DRIVER_UUID).distinct().where(income.CUSTOMER_ID==custID))["data"]
                        #    if (r["DRIVER_UUID"] not in [ele["DRIVER_UUID"] for ele in q1]) & (q1!=[]):
                        #        custID = "0"
                        #    X = utils.mergeDicts(r, {"CUSTOMER_ID":custID, "MODIFIED_BY":msgHeader["authLoginID"], "SOURCE":"UBER",
                        #                             "DOC_ID":docID, "MODIFIED_DATE":datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                        #    db.Update(db="mint_loan", table="mw_driver_income_data_new", conditions={"AUTO_ID = ":str(exists[0]["AUTO_ID"])},
                        #              checkAll=False, replace=True, **X)
                    custIDs = {ele["CUSTOMER_ID"]: set()
                               for ele in d2 if ele["CUSTOMER_ID"] not in (0, "0")}
                    for ele in d2:
                        if ele["CUSTOMER_ID"] not in (0, "0"):
                            custIDs[ele["CUSTOMER_ID"]].add(ele["DRIVER_UUID"])
                    duplicates = [x for x in custIDs if len(custIDs[x]) > 1]
                    for e in duplicates:
                        #pan = db.runQuery(Query.from_(panstatus).select(panstatus.PAN_NO).distinct().where(panstatus.CUSTOMER_ID==e))["data"]
                        for i, j in enumerate(d2):
                            if (j["CUSTOMER_ID"] == e) & (j["PARTNER_TYPE"] == "Driver Under Partner"):
                                d2[i]["CUSTOMER_ID"] = "0"
                    for e in custIDs:
                        q = Query.from_(prof).select(
                            prof.CUSTOMER_ID, prof.NUMBER_VERIFIED, prof.NAME_VERIFIED)
                        q = db.runQuery(q.where(prof.CUSTOMER_ID == e))
                        if ((not (q["data"][0]["NAME_VERIFIED"] or q["data"][0]["NUMBER_VERIFIED"])) if q["data"] else False):
                            junk = db.Update(db="mint_loan", table="mw_client_profile", checkAll=False, NAME_VERIFIED='P', NUMBER_VERIFIED='P',
                                             MODIFIED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                             MODIFIED_BY=msgHeader["authLoginID"],
                                             conditions={"CUSTOMER_ID = ": str(q["data"][0]["CUSTOMER_ID"])})
                    for ele in d2:
                        inserted = db.Insert(db="mint_loan", table="mw_driver_income_data_new", compulsory=False, date=False, **ele)
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
