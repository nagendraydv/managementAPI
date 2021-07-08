from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, Order, functions


class PaymentDisbursalReportResource:

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
        success = "generated report successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='paymentDisbursalReport', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # setting an instance of DB class
                db = DB(id=input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    s3url = "https://s3-ap-southeast-1.amazonaws.com/"
                    bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
                    folder = "ml_payment_reports/"
                    try:
                        filename = (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + ".csv"
                        s3path = s3url + bucket + '/' + folder + filename
                        session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                                aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                        s3 = session.resource('s3')
                        junk = s3.meta.client.head_bucket(Bucket=bucket)
                    except botocore.exceptions.ClientError as e:
                        raise falcon.HTTPError(
                            falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    bankdetails = Table(
                        "mw_cust_bank_detail", schema="mint_loan")
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    docs = Table("mw_other_documents", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    income = Table("mw_derived_income_data",
                                   schema="mw_company_3")
                    cprof = Table("mw_profile_info", schema="mw_company_3")
                    income2 = Table("mw_driver_income_data_new",
                                    schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    uagg = Table("mw_user_agreement", schema="mint_loan")
                    disb = Table("mw_disbursal_report", schema="mint_loan")
                    citym = Table("mw_city_master", schema="mint_loan")
                    uuidm = Table("mw_driver_uuid_master", schema="mint_loan")
                    emip = Table("mw_finflux_emi_packs_master",
                                 schema="mint_loan")
                    lender = (
                        input_dict["data"]["lender"] if "lender" in input_dict["data"] else "CHAITANYA")
                    qagg = Query.from_(uagg).select(uagg.CUSTOMER_ID).where(
                        uagg.TYPE_OF_DOCUMENT == 'UBER DIGITAL DEDUCT AGREEMENT')
                    q0 = Query.from_(profile).select("CUSTOMER_ID").where(
                        profile.CUSTOMER_ID.isin(qagg))  # .where(profile.CURRENT_CITY!='CHENNAI')
                    q01 = Query.from_(profile).select("CUSTOMER_ID").where(
                        profile.COMPANY_NAME == 'Swiggy')
                    q00 = Query.from_(income).select(
                        "PRESENT_STATUS", "PARTNER_TYPE")
                    q000 = Query.from_(income2).select(
                        "PRESENT_STATUS", "PARTNER_TYPE")
                    qcprof = Query.from_(cprof).select("ACTIVATION_STATUS")
                    q1 = Query.from_(uuidm).select(
                        uuidm.CUSTOMER_ID).distinct().where(uuidm.CUSTOMER_ID.isin(q0))
                    q2 = Query.from_(kycdocs).select(kycdocs.CUSTOMER_ID).where((kycdocs.DOCUMENT_TYPE_ID.isin(['114', '115'])) &
                                                                                (kycdocs.CUSTOMER_ID.isin(q1) | kycdocs.CUSTOMER_ID.isin(q01)))
                    #q2 = Query.from_(uagg).select(uagg.CUSTOMER_ID).where(uagg.TYPE_OF_DOCUMENT=='UBER DIGITAL DEDUCT AGREEMENT')
                    non_disbursal_products = [
                        "MOBILE_LOAN_LIMIT", "TYRE_LOAN_LIMIT", "EDUCATION_LOAN_LIMIT", "INSURANCE_LOAN_LIMIT"]
                    lp_no_disburse = db.runQuery(Query.from_(loanprod).select("PRODUCT_ID").where(
                        loanprod.LIMIT_TYPE.isin(non_disbursal_products)))["data"]
                    lp_no_disburse = [str(ele["PRODUCT_ID"])
                                      for ele in lp_no_disburse]
                    q = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    join = q.join(bankdetails, how=JoinType.left).on(
                        loanmaster.CUSTOMER_ID == bankdetails.CUSTOMER_ID)
                    join = join.join(kyc, how=JoinType.left).on_field("CUSTOMER_ID").join(
                        profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    join = join.join(loanprod, how=JoinType.left).on(
                        loanprod.PRODUCT_ID == loanmaster.LOAN_PRODUCT_ID)
                    # join = join.join(charges, how=JoinType.left).on(loanmaster.LOAN_PRODUCT_ID=charges.PRODUCT_ID)
                    if lender not in ('POONAWALLA', 'GETCLARITY', 'MINTWALK', 'POONAWALLA2'):
                        #q = join.join(custcred, how=JoinType.left).on(loanmaster.CUSTOMER_ID==custcred.CUSTOMER_ID)
                        q = join.select(loandetails.APPROVED_PRINCIPAL, loandetails.PRINCIPAL, loanmaster.LOAN_APPLICATION_NO.as_("LOAN_ID"),
                                        loanmaster.AMOUNT, loanmaster.LOAN_ACCOUNT_NO, kyc.NAME, bankdetails.ACCOUNT_HOLDER_NAME,
                                        loanmaster.CUSTOMER_ID, bankdetails.IFSC_CODE, bankdetails.ACCOUNT_NO, profile.NAME.as_(
                                            "PROFILE_NAME"),
                                        loanmaster.LOAN_PRODUCT_ID, profile.COMPANY_NAME)
                        data = db.runQuery(q.where((loanmaster.LENDER == lender) & (loanmaster.LOAN_PRODUCT_ID.isin(["3", "5"])) &
                                                   (loanmaster.LOAN_REFERENCE_ID.isin(input_dict["data"]["loanRefIDs"])) &
                                                   (loanmaster.LOAN_REQUEST_DATE > (datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d")) &
                                                   (bankdetails.DELETE_STATUS_FLAG == 0) & (bankdetails.DEFAULT_STATUS_FLAG == 1)))  # &
                        # (loanmaster.CUSTOMER_ID.isin(q2))))
                        data["data"] += db.runQuery(q.where((loanmaster.LENDER == lender) & (loanmaster.LOAN_PRODUCT_ID.notin(["3", "5"])) &
                                                            (loanmaster.LOAN_REFERENCE_ID.isin(input_dict["data"]["loanRefIDs"])) &
                                                            (loanmaster.LOAN_REQUEST_DATE > (datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d")) &
                                                            (bankdetails.DELETE_STATUS_FLAG == 0) & (bankdetails.DEFAULT_STATUS_FLAG == 1)))["data"]
                    else:
                        q = join.join(custcred, how=JoinType.left).on(
                            loanmaster.CUSTOMER_ID == custcred.CUSTOMER_ID)
                        q = q.select(loanmaster.LOAN_ACCOUNT_NO, loanmaster.CUSTOMER_ID, loanmaster.LOAN_REQUEST_DATE, bankdetails.account_holder_name, kyc.NAME,
                                     profile.name.as_(
                                         "PROFILE_NAME"), profile.current_city, loanmaster.amount, bankdetails.account_no,
                                     bankdetails.ifsc_code, profile.email_id, custcred.LOGIN_ID, profile.COMPANY_NAME, loanmaster.LOAN_PRODUCT_ID)
                        data = db.runQuery(q.where((loanmaster.LENDER == "GETCLARITY") & (custcred.STAGE == 'LOAN_APPROVED') &
                                                   ((loanmaster.FUND == 'POONAWALLA2') if lender == "POONAWALLA" else (loanmaster.FUND == 'MINTWALK') if lender == 'MINTWALK' else (loanmaster.LOAN_PRODUCT_ID.notin(["2", "12", "13"]))) & (loanmaster.STATUS == 'PENDING') &
                                                   (bankdetails.DELETE_STATUS_FLAG == 0) & (bankdetails.DEFAULT_STATUS_FLAG == 1)))  # &
                        # (loanmaster.CUSTOMER_ID.isin(q2))))
                    # print db.pikastr(q.where((loanmaster.LENDER==lender) & (loanmaster.LOAN_REFERENCE_ID.isin(input_dict["data"]["loanRefIDs"])) & (bankdetails.DELETE_STATUS_FLAG==0) & (bankdetails.DEFAULT_STATUS_FLAG==1) & (loanmaster.CUSTOMER_ID.isin(q2)))), data
                    for ele in data["data"]:
                        exist = db.runQuery(Query.from_(uagg).select(functions.Count("CUSTOMER_ID").as_("c")).where(
                            (uagg.TYPE_OF_DOCUMENT.like('%DIGITAL DEDUCT AGREEMENT')) & (uagg.CUSTOMER_ID == ele["CUSTOMER_ID"])))["data"]
                        exist = (db.runQuery(Query.from_(kycdocs).select(functions.Count("CUSTOMER_ID").as_("c")).where((kycdocs.DOCUMENT_TYPE_ID.isin(['114', '115'])) & (
                            kycdocs.CUSTOMER_ID == ele["CUSTOMER_ID"])))["data"] if exist[0]["c"] > 0 else [{"c": 0}]) if (ele["COMPANY_NAME"].lower() not in ('swiggy', 'shuttle')) else [{"c": 1}]
                        exist = db.runQuery(Query.from_(income2).select(functions.Count("CUSTOMER_ID").as_("c")).where(
                            income2.CUSTOMER_ID == ele["CUSTOMER_ID"]))["data"] if exist[0]["c"] > 0 else [{"c": 0}]
                        XX = db.runQuery(q00.where(income.CUSTOMER_ID == str(ele["CUSTOMER_ID"])).orderby(
                            income.WEEK, order=Order.desc).limit(1))["data"]
                        XX = db.runQuery(q000.where(income2.CUSTOMER_ID == str(ele["CUSTOMER_ID"])).orderby(
                            income2.WEEK, order=Order.desc).limit(1))["data"] if not XX else XX
                        status = (XX[0] if XX else {"PRESENT_STATUS": "waitlisted", "PARTNER_TYPE": "Driver Under Partner"}) if (ele["COMPANY_NAME"].lower(
                        ) not in ('swiggy', 'uber auto') if ele["COMPANY_NAME"] else False) else {"PRESENT_STATUS": "active", "PARTNER_TYPE": "fleet_dco"}
                        pstatus = db.runQuery(qcprof.where(
                            cprof.CONFIRMED_CUSTOMER_ID == str(ele["CUSTOMER_ID"])))["data"]
                        status["PRESENT_STATUS"] = pstatus[0]["ACTIVATION_STATUS"] if pstatus else status["PRESENT_STATUS"]
                        if (ele["LOAN_PRODUCT_ID"] not in ("11", "26", "16", "9")) & (ele["COMPANY_NAME"] != 'UDAAN') & ((not (exist[0]["c"] > 0)) | ((status["PARTNER_TYPE"] not in ("fleet_ndp", "fleet_dco", "single_dco", "single_ndp")) or ((("auto-reactivation" not in status["PRESENT_STATUS"].lower()) and (status["PRESENT_STATUS"].lower() != "active") if status["PRESENT_STATUS"] is not None else True) and status["PARTNER_TYPE"] not in ("fleet_dco", "fleet_ndp", "single_ndp")))):
                            # if partner_type is not fleet_ndp, fleet_dco or single_dco or single_ndp or present status is not active or 'auto_reactivation is not in present_status or present_status is None (do not check this for single_ndp, fleet_dco and fleet_ndp)
                            data["data"].remove(ele)
                        # if status["PARTNER_TYPE"] not in ("fleet_ndp", "fleet_dco", "single_dco"):
                        #    data["data"].remove(ele)
                    x = [(ele["LOAN_ID"] if lender == 'CHAITANYA' else ele["LOAN_ACCOUNT_NO"])
                         for ele in data["data"]]
                    y = [ele for ele in set(x) if x.count(ele) > 1]
                    d = [ele for ele in data["data"] if (
                        ele["LOAN_ID"] if lender == 'CHAITANYA' else ele["LOAN_ACCOUNT_NO"]) not in y]
                    today = datetime.now().strftime("%d/%m/%Y")
                    with open("temp.csv", 'w') as f:
                        # if lender=="GETCLARITY":
                        #    f.write("CustomerID,Beneficiary Name,Beneficiary A/c No.,IFS Code,Amount,Remarks,Company Name\n")
                        if lender in ("POONAWALLA", "GETCLARITY", 'MINTWALK', 'POONAWALLA2'):
                            f.write("Company,Payment mode,Loan id,Applicant id,Payment benificiary name,current_city,GROSS AMOUNT,PF,NET DISBURSAL Amount,Date,Debit account number,Credit account Number,IFSC Code,Email id,Mobile Number,Remarks\n")
                        else:
                            f.write("Record_Identifier,Critical_issue,Duplicate_count,Payment_Value_Date,Payment_Amount,Debit_Account_No," +
                                    "Customer_Reference_No,Customer_Instrument_No,Payment_Product_Code,Beneficiary_Code,Beneficiary_Name," +
                                    "Beneficiary_Address1,Beneficiary_Address2,Beneficiary_Address3,Beneficiary_Address4,Payable_Loc_Code," +
                                    "Print_Branch_Code,Dispatch_Address1,Dispatch_Address2,Dispatch_Mode,Dispatch_To,Payment_Remarks," +
                                    "ReasonForPayment,Credit_Account_No,IFSC_Code,Notification_Emails,Enrichment1,Enrichment2,Enrichment3," +
                                    "Enrichment4,Product Name\n")
                        acc_no = []
                        for i, ele in enumerate(d):
                            EMI = db.runQuery(Query.from_(emip).select("EMI", "LOAN_TERM", "AUTO_ID").where((emip.LOAN_PRODUCT_ID == ele["LOAN_PRODUCT_ID"]) & (
                                emip.LOAN_AMOUNT == ele[("amount" if lender in ('GETCLARITY', 'POONAWALLA', 'MINTWALK', 'POONAWALLA2') else "PRINCIPAL")])))["data"]
                            c = Query.from_(charges).select(charges.star).where(charges.PRODUCT_ID == str(
                                ele["LOAN_PRODUCT_ID"])).where(charges.CHARGE_TIME_TYPE == 'DISBURSEMENT')
                            c = c.where((charges.EMI_PACK_ID == (EMI[0]["AUTO_ID"] if EMI else 0)) | (
                                charges.EMI_PACK_ID.isnull()))
                            q = db.runQuery(c)["data"]
                            # q = db.runQuery(Query.from_(charges).select(charges.star).where((charges.PRODUCT_ID==ele["LOAN_PRODUCT_ID"]) &
                            #                                                                (charges.CHARGE_TIME_TYPE=='DISBURSEMENT')))["data"]
                            if lender == "CHAITANYA":
                                q0 = Query.from_(disb).select("CUSTOMER_REFERENCE_NO").where(
                                    disb.CUSTOMER_REFERENCE_NO == ele["LOAN_ID"])  # .lstrip("0"))
                                dup = db.runQuery(q0)["data"]
                                dup2 = '0' if ele["ACCOUNT_NO"] not in acc_no else 'DUPLICATE_LOAN'
                            else:
                                dup2 = '0'
                            pval = db.runQuery(Query.from_(disb).select(disb.RECORD_IDENTIFIER.as_("m")).orderby(disb.AUTO_ID,
                                                                                                                 order=Order.desc).limit(1))["data"]
                            char = sum([ele2["CHARGE_AMOUNT"] for ele2 in q])
                            if dup2 == '0' and lender not in ('GETCLARITY', 'POONAWALLA', 'MINTWALK', 'POONAWALLA2'):
                                f.write("P-%i,%s,%i,'%s," % (i+1+(int(pval[0]["m"][2:]) if pval else 0), dup2, len(dup), today) +  # int(pval["m"][2:])
                                        "%.2f,'409000484416," % ((ele["APPROVED_PRINCIPAL"]-char) if ele["APPROVED_PRINCIPAL"]
                                                                 else (ele["PRINCIPAL"]-char)) +
                                        "%s,,NEFT_INDIVIDUAL,,%s,,,,,,,,,,,," % (ele["LOAN_ID"], (ele["ACCOUNT_HOLDER_NAME"]  # .lstrip("0") zeros req now
                                                                                                  if ele["ACCOUNT_HOLDER_NAME"] else
                                                                                                  (ele["NAME"] if ele["NAME"] else ""))) +
                                        "LOAN DISBURSAL,'%s,%s,,,,,," % (ele["ACCOUNT_NO"], ele["IFSC_CODE"]) +
                                        "MINTWALK %s WEEKLY\n" % ("8" if (ele["APPROVED_PRINCIPAL"] == 10000 or ele["PRINCIPAL"] == 10000)
                                                                  else "4" if (ele["APPROVED_PRINCIPAL"] == 5000 or ele["PRINCIPAL"] == 5000) else "10"))
                            # elif lender=='GETCLARITY':
                            #    f.write(str(ele["CUSTOMER_ID"]) + ",%s,"%(ele["ACCOUNT_HOLDER_NAME"] if ele["ACCOUNT_HOLDER_NAME"] else (ele["NAME"] if ele["NAME"] else "")) +
                            #            "%s,%s,"%(ele["ACCOUNT_NO"], ele["IFSC_CODE"]) +
                            #            "%.2f,SUPERMONEY LOAN %s,%s\n"%((ele["AMOUNT"]-char),ele["LOAN_ACCOUNT_NO"],ele["COMPANY_NAME"]))
                            elif lender in ('POONAWALLA', 'GETCLARITY', 'MINTWALK', 'POONAWALLA2'):
                                state = db.runQuery(Query.from_(citym).select("STATE").where(
                                    functions.Lower(citym.CITY) == ele["current_city"].lower()))["data"]
                                f.write(ele["COMPANY_NAME"] + ',N,' + ele["LOAN_ACCOUNT_NO"] + "," + str(ele["CUSTOMER_ID"]) +
                                        ",%s," % (ele["account_holder_name"] if ele["account_holder_name"] else (ele["NAME"] if ele["NAME"] else "")) +
                                        "%s,%.2f,%.2f,%.2f," % (state[0]["STATE"] if state else ele["current_city"], ele["amount"], (0 if ele["LOAN_PRODUCT_ID"] in lp_no_disburse else 0.0125 if ele["LOAN_PRODUCT_ID"] == "16" else (0.02006 if lender == 'POONAWALLA2' else 0.02) if ele["LOAN_PRODUCT_ID"] != "5" else 0.03)*ele["amount"] if ele["LOAN_PRODUCT_ID"] != "26" else (char), (1 if ele["LOAN_PRODUCT_ID"] in lp_no_disburse else 0.9875 if ele["LOAN_PRODUCT_ID"] == "16" else (0.97994 if lender == 'POONAWALLA2' else 0.98) if ele["LOAN_PRODUCT_ID"] != "5" else 0.97)*ele["amount"] if ele["LOAN_PRODUCT_ID"] != "26" else (ele["amount"]-char)) +
                                        "%s,000505026575,%s,%s," % ((datetime.now().strftime("%Y-%m-%d") if ele["LOAN_PRODUCT_ID"] not in lp_no_disburse else ele["LOAN_REQUEST_DATE"]), (ele["account_no"] if ele["LOAN_PRODUCT_ID"] not in lp_no_disburse else ''), (ele["ifsc_code"] if ele["LOAN_PRODUCT_ID"] not in lp_no_disburse else '')) +
                                        "%s,%s,\n" % (ele["email_id"], ele["LOGIN_ID"]))
                    junk = s3.Object(bucket, folder + filename).put(Body=open("temp.csv",'rb'))
                    #junk = s3.Object(bucket, folder + filename).put(Body=open("temp.csv","r",encoding='utf-8'))
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=filename,
                                         DOCUMENT_FOLDER=folder.strip("/"), UPLOAD_MODE="SmartDash Admin", DOCUMENT_STATUS="N",
                                         CREATED_BY=input_dict["msgHeader"]["authLoginID"], OPTIONAL_ARG1=lender,
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docSeqID = db.runQuery(Query.from_(docs).select("DOC_SEQ_ID").orderby(
                        docs.DOC_SEQ_ID, order=Order.desc).limit(1))["data"]
                    #token = generate(db).AuthToken(exp=0)
                    if True:  # token["updated"]:
                        output_dict["data"].update({"fileID": docSeqID[0]["DOC_SEQ_ID"] if docSeqID else 0})
                        output_dict["data"].update({"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
#                resp.content_type = mimetypes.guess_type(filename)[0]
#                resp.stream = s3.Object(bucket,folder + filename).get()['Body']
#                resp.stream_len = s3.Object(bucket,folder + filename).content_length
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
