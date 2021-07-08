from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType, Order


class CustReportResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"fileID": ""}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='searchUser', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    s3url = "https://s3-ap-southeast-1.amazonaws.com/"
                    bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
                    folder = "ml_cust_csv/"
                    try:
                        filename = (
                            datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + ".csv"
                        s3path = s3url + bucket + '/' + folder + filename
                        session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                                aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                        s3 = session.resource('s3')
                        junk = s3.meta.client.head_bucket(Bucket=bucket)
                    except botocore.exceptions.ClientError as e:
                        raise falcon.HTTPError(
                            falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
                    page = input_dict["data"]['page']
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    docs = Table("mw_cust_kyc_documents", schema="mint_loan")
                    docs2 = Table("mw_other_documents", schema="mint_loan")
                    loanmaster = Table("mw_client_loan_master", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",schema="mint_loan")
                    income2 = Table("mw_derived_income_data",schema="mw_company_3")
                    custdata = Table("mw_customer_data", schema="mint_loan")
                    uagg = Table("mw_user_agreement", schema="mint_loan")
                    uuidm = Table("mw_driver_uuid_master", schema="mint_loan")
                    join = Query.from_(custcred).join(
                        kyc, how=JoinType.left).on_field("CUSTOMER_ID")
                    join = join.join(profile, how=JoinType.left).on_field(
                        "CUSTOMER_ID")  # .join(custdata, how=JoinType.left)
#		    join = join.on_field("CUSTOMER_ID")
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, kyc.NAME, kyc.CREATED_BY, profile.name.as_("PROFILE_NAME"),
                                    profile.COMPANY_NAME, profile.COMPANY_NUMBER)  # .where((custdata.DATA_KEY=="COMPANY_NUMBER") | (custdata.CUSTOMER_ID.isnull()))
                    indict = input_dict['data']
                    if "priority" in list(indict.keys()):
                        if indict["priority"] == "firstPriority":
                            q3 = Query.from_(income).select(income.CUSTOMER_ID).distinct().where(
                                income.PARTNER_TYPE != 'Driver Under Partner')
                            q4 = Query.from_(income2).select(
                                income2.CUSTOMER_ID).distinct().where(income2.INCOME > 0)
                            qqq = Query.from_(custcred).select(custcred.CUSTOMER_ID).distinct().where((custcred.CUSTOMER_ID.isin(
                                q4)) | (custcred.CREATED_DATE > (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d 00:00:00")))
                            q2 = Query.from_(docs).join(q3, how=JoinType.left).on_field("CUSTOMER_ID").select(docs.CUSTOMER_ID).distinct().where((docs.DOCUMENT_TYPE_ID.isin(['113', '127', '128'])) &
                                                                                                                                                 (q3.CUSTOMER_ID.isnull()) & (docs.CUSTOMER_ID.isin(qqq)) &
                                                                                                                                                 (docs.VERIFICATION_STATUS == 'Y'))
                            q3 = Query.from_(docs).select(docs.CUSTOMER_ID).distinct().where(
                                docs.DOCUMENT_TYPE_ID.isin(['114', '115']))
                            q2 = Query.from_(uagg).join(uuidm, how=JoinType.left).on_field("CUSTOMER_ID").select(uagg.CUSTOMER_ID).distinct().where(
                                (uagg.TYPE_OF_DOCUMENT == 'UBER DIGITAL INFO AGREEMENT') & (uagg.CUSTOMER_ID.isin(q3)) & (uuidm.CUSTOMER_ID.isnull()))
                        elif indict["priority"] == "secondPriority":
                            q3 = Query.from_(income).select(income.CUSTOMER_ID, functions.Max(
                                income.WEEK).as_("maxdate")).groupby(income.CUSTOMER_ID)
                            q2 = Query.from_(income).join(q3).on_field(
                                "CUSTOMER_ID").select(income.CUSTOMER_ID).distinct()
                            q2 = q2.where(q3.maxdate < (
                                datetime.now() - timedelta(days=13)).strftime("%Y-%m-%d 00:00:00"))
                        q4 = Query.from_(income2).select(
                            income2.CUSTOMER_ID).distinct().where(income2.INCOME > 0)
                        q = q.where(custcred.CUSTOMER_ID.isin(q2)).where((custcred.CUSTOMER_ID.isin(q4)) | (
                            custcred.CREATED_DATE > (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d 00:00:00")))
                    if indict["searchBy"] == "listCustomers" and indict["days"] > 0:
                        q = q.where(custcred.CREATED_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    elif indict["searchBy"] in ["name", "loginID", "email", "pan", "aadhar", "customerID", "stage"]:
                        if indict["searchBy"] == "stage":
                            q = q.where(custcred.STAGE == indict["searchText"])
                        if indict["searchBy"] == 'loginID':
                            q = q.where(custcred.LOGIN_ID.like(
                                "%" + indict["searchText"] + "%"))
                        if indict["searchBy"] == 'customerID':
                            q = q.where(custcred.CUSTOMER_ID ==
                                        indict["searchText"])
                        if indict["searchBy"] == 'name':
                            q = q.where(profile.NAME.like(
                                "%" + indict["searchText"] + "%"))
                    Fields = db.runQuery(
                        q.orderby(custcred.CUSTOMER_ID, order=Order.desc))
                    with open("temp.csv", "w") as f:
                        f.write(
                            "id,CompanyNumber,CompanyNumber2,CompanyNumber3,CompanyName,Name,Phone,Agreement\n")
                        for datum in Fields["data"]:
                            #custPan = Query.from_(pan).select("PAN_NO").where(pan.CUSTOMER_ID==datum["CUSTOMER_ID"])
                            #panNo = db.runQuery(custPan.orderby(pan.CREATED_DATE, order=Order.desc).limit(1))
                            #datum.update({"PAN_NO": panNo["data"][0]["PAN_NO"] if panNo["data"] else ""})
                            #custDocs = Query.from_(docs).select("DOCUMENT_TYPE_ID", "VERIFICATION_STATUS", "CREATED_DATE")
                            # custDocs = db.runQuery(custDocs.where((docs.CUSTOMER_ID==datum["CUSTOMER_ID"]) &
                            #                                      ((docs.VERIFICATION_STATUS=='Y') | (docs.VERIFICATION_STATUS.isnull()))))
                            #lastDocUpload = max([ele["CREATED_DATE"] for ele in custDocs["data"]]) if custDocs["data"] else ""
                            #aadharDoc = [ele["VERIFICATION_STATUS"] for ele in filter(lambda x:x["DOCUMENT_TYPE_ID"]==108, custDocs["data"])]
                            #aadharDoc = 'VERIFIED' if 'Y' in aadharDoc else 'SUBMITTED' if len(aadharDoc)>0 else 'NOT_AVAILABLE'
                            #agreementDoc = [ele["VERIFICATION_STATUS"] for ele in filter(lambda x:x["DOCUMENT_TYPE_ID"]==113, custDocs["data"])]
                            #agreementDoc = 'VERIFIED' if 'Y' in agreementDoc else 'SUBMITTED' if len(agreementDoc)>0 else 'NOT_AVAILABLE'
                            compNo = Query.from_(custdata).select("DATA_VALUE").where((custdata.DATA_KEY == "COMPANY_NUMBER") & (
                                custdata.CUSTOMER_ID == datum["CUSTOMER_ID"])).orderby(custdata.ID, order=Order.desc)
                            compNo = [ele["DATA_VALUE"]
                                      for ele in db.runQuery(compNo)["data"]]
                            #compNo.remove(datum["COMPANY_NUMBER"]) if (datum["COMPANY_NUMBER"] in compNo) else None
                            compNo1 = compNo[0] if len(compNo) > 0 else ""
                            compNo2 = compNo[1] if len(compNo) > 1 else ""
                            compNo3 = compNo[2] if len(compNo) > 2 else ""
                            compNo = Query.from_(income).select(income.CONTACT_NUMBER).distinct().where(
                                income.CUSTOMER_ID == datum["CUSTOMER_ID"])
                            compNo = [ele["CONTACT_NUMBER"]
                                      for ele in db.runQuery(compNo)["data"]]
                            #compNo.remove(datum["COMPANY_NUMBER"]) if (datum["COMPANY_NUMBER"] in compNo) else None
                            compNo.remove(compNo1) if (
                                compNo1 in compNo) else None
                            compNo.remove(compNo2) if (
                                compNo2 in compNo) else None
                            compNo.remove(compNo3) if (
                                compNo3 in compNo) else None
                            compNo3 = compNo[0] if (
                                (len(compNo) > 0) and (compNo3 == "")) else compNo3
                            compNo2 = compNo[1] if (
                                (len(compNo) > 1) and (compNo2 == "")) else compNo2
                            #compNo1 = compNo[2] if ((len(compNo)>2) and (compNo1=="")) else compNo1
                            #bankDoc = [ele["VERIFICATION_STATUS"] for ele in filter(lambda x:x["DOCUMENT_TYPE_ID"] in (102,106), custDocs["data"])]
                            #bankDoc = 'VERIFIED' if 'Y' in bankDoc else 'SUBMITTED' if len(bankDoc)>0 else 'NOT_AVAILABLE'
                            #incomeDoc = [ele["VERIFICATION_STATUS"] for ele in filter(lambda x:x["DOCUMENT_TYPE_ID"]==110, custDocs["data"])]
                            #incomeDoc = 'VERIFIED' if 'Y' in incomeDoc else 'SUBMITTED' if len(incomeDoc)>0 else 'NOT_AVAILABLE'
                            #carDoc = [ele["VERIFICATION_STATUS"] for ele in filter(lambda x:x["DOCUMENT_TYPE_ID"]==111, custDocs["data"])]
                            #carDoc = 'VERIFIED' if 'Y' in carDoc else 'SUBMITTED' if len(carDoc)>0 else 'NOT_AVAILABLE'
                            #panDoc =  [ele["VERIFICATION_STATUS"] for ele in filter(lambda x:x["DOCUMENT_TYPE_ID"]==100, custDocs["data"])]
                            #panDoc = 'VERIFIED' if 'Y' in panDoc else 'SUBMITTED' if len(panDoc)>0 else 'NOT_AVAILABLE'
                            #q = Query.from_(loanmaster).select("STATUS", "CREATED_DATE").where(loanmaster.CUSTOMER_ID==datum["CUSTOMER_ID"])
                            #loanStatus = db.runQuery(q.orderby(loanmaster.CREATED_DATE, order=Order.desc).limit(1))["data"]
                            #loanStatus, loanCreated = (loanStatus[0]["STATUS"], loanStatus[0]["CREATED_DATE"]) if loanStatus else ("", "")
                            #readiness = "YES" if set([bankDoc, incomeDoc, panDoc, carDoc]) == set(["VERIFIED"]) else "NO"
                            #readiness = "YES" if (readiness=="YES" and (datum["CREATED_BY"]=="Admin" or aadharDoc)) else "NO"
                            f.write(("%s," % (datum["CUSTOMER_ID"]) +  # , datum["COMPANY_NUMBER"].replace("\n","") if datum["COMPANY_NUMBER"] else "") +
                                     "%s,%s,%s,%s," % (compNo1, compNo2, compNo3, datum["COMPANY_NAME"].replace("\n", "") if datum["COMPANY_NAME"] else "") +
                                     "%s," % (datum["NAME"].replace("\n", "") if datum["NAME"] else datum["PROFILE_NAME"].replace("\n", "")
                                              if datum["PROFILE_NAME"] else "") +
                                     "%s,%s\n" % (datum["LOGIN_ID"], '1')))  # agreementDoc)))
                            # "'%s','%s',"%(lastDocUpload, loanCreated) +
                            # "'%s','%s','%s','%s','%s','%s',,,\n"%(agreementDoc, bankDoc, panDoc, incomeDoc, carDoc, loanStatus)))
                    junk = s3.Object(bucket, folder + filename).put(Body=open("temp.csv", 'rb'))
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=filename,
                                         DOCUMENT_FOLDER=folder.strip("/"), UPLOAD_MODE="SmartDash Admin", DOCUMENT_STATUS="N",
                                         CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docSeqID = db.runQuery(Query.from_(docs2).select("DOC_SEQ_ID").orderby(
                        docs2.DOC_SEQ_ID, order=Order.desc).limit(1))["data"]
                    #token = generate(db).AuthToken()
                    if True:  # token["updated"]:
                        output_dict["data"].update(
                            {"fileID": docSeqID[0]["DOC_SEQ_ID"] if docSeqID else 0})
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
