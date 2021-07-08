from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, Order
import six


class MandatePaymentOrderResource:

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
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
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
                    folder = "ml_mandate_payment_orders/"
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
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    chargemaster = Table(
                        "mw_charges_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    bankdetails = Table(
                        "mw_cust_bank_detail", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    mandate = Table(
                        "mw_physical_mandate_status", schema="mint_loan")
                    mantype = Table("mw_mandate_documents", schema="mint_loan")
                    docs = Table("mw_other_documents", schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    hist = Table(
                        "mw_client_loan_repayment_history_master", schema="mint_loan")
                    emi = Table("mw_finflux_emi_packs_master",
                                schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",
                                 schema="mint_loan")
                    days_left = 2-datetime.today().weekday()
                    # (7-datetime.today().weekday()) + 2
                    days_left += 7 if days_left < 0 else 0
                    wed = (datetime.today() + timedelta(days=days_left)
                           ).strftime("%Y-%m-%d")
                    q = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    join = q.join(emi, how=JoinType.left).on(
                        loanmaster.LOAN_PRODUCT_ID == emi.LOAN_PRODUCT_ID)
                    join = join.join(bankdetails, how=JoinType.left).on(
                        loanmaster.CUSTOMER_ID == bankdetails.CUSTOMER_ID)
                    join = join.join(kyc, how=JoinType.left).on_field("CUSTOMER_ID").join(
                        profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    join = join.join(mandate, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(mantype, how=JoinType.left)
                    join = join.on(mandate.DOC_ID == mantype.AUTO_ID)
                    q = join.select(loandetails.APPROVED_PRINCIPAL, loandetails.PRINCIPAL, loanmaster.LOAN_ACCOUNT_NO.as_("LOAN_ID"), kyc.NAME,
                                    loanmaster.LOAN_PRODUCT_ID, bankdetails.ACCOUNT_HOLDER_NAME, mandate.IFSC_CODE, mandate.ACCOUNT_NUMBER,
                                    profile.NAME.as_(
                                        "PROFILE_NAME"), mandate.REF_NO, mandate.UTILITY_CODE, mantype.MANDATE_FORMAT,
                                    loandetails.EXPECTED_MATURITY_DATE, loandetails.TOTAL_OUTSTANDING, loanmaster.CUSTOMER_ID,
                                    loanmaster.LOAN_REFERENCE_ID, emi.EMI, loanmaster.LOAN_DISBURSED_DATE, profile.COMPANY_NAME)
                    if input_dict["data"]["loanRefIDs"] == '' or input_dict["data"]["loanRefIDs"] == []:
                        data = db.runQuery(q.where((loanmaster.STATUS.isin(['ACTIVE', 'WRITTEN-OFF'])) & (
                            mandate.MANDATE_STATUS == 'Active') & (mandate.REF_NO.notnull())).groupby(loanmaster.CUSTOMER_ID))
                    else:
                        data = db.runQuery(q.where((loanmaster.LOAN_ACCOUNT_NO.isin(input_dict["data"]["loanRefIDs"])) &
                                                   (mandate.MANDATE_STATUS == 'Active') &
                                                   (loanmaster.STATUS.isin(['ACTIVE', 'WRITTEN-OFF'])) & (bankdetails.DELETE_STATUS_FLAG == 0)))
                    # len(data["data"])
                    for datum in data["data"]:
                        d = db.runQuery(Query.from_(repay).select(repay.star).where((repay.LOAN_REF_ID == datum["LOAN_REFERENCE_ID"]) &
                                                                                    (repay.FINFLUX_TRAN_ID.isnull())))["data"]
                        H = Query.from_(hist).select(
                            "LOAN_ID", "TRANSACTION_DATE", "TRANSACTION_STATUS", "AMOUNT")
                        h = db.runQuery(H.where((hist.TRANSACTION_MEDIUM == 'NACH_DEBIT') & (hist.TRANSACTION_STATUS == 'FAILURE') &
                                                (hist.LOAN_ID == datum["LOAN_ID"])).orderby(hist.TRANSACTION_DATE, order=Order.desc))["data"]
                        fails = len(h)
                        if fails >= 3:
                            h3 = ",".join(str(v) for k, v in six.iteritems(
                                h[2]) if k != 'LOAN_ID')
                            h2 = ",".join(str(v) for k, v in six.iteritems(
                                h[1]) if k != 'LOAN_ID')
                            h1 = ",".join(str(v) for k, v in six.iteritems(
                                h[0]) if k != 'LOAN_ID')
                        elif fails == 2:
                            h3 = ",,"
                            h2 = ",".join(str(v) for k, v in six.iteritems(
                                h[1]) if k != 'LOAN_ID')
                            h1 = ",".join(str(v) for k, v in six.iteritems(
                                h[0]) if k != 'LOAN_ID')
                        elif fails == 1:
                            h3, h2 = ",,", ",,"
                            h1 = ",".join(str(v) for k, v in six.iteritems(
                                h[0]) if k != 'LOAN_ID')
                        else:
                            h3, h2, h1 = ",,", ",,", ",,"
                        h = db.runQuery(H.where((hist.TRANSACTION_MEDIUM == 'NACH_DEBIT') & (hist.TRANSACTION_STATUS == 'SUCCESS') &
                                                (hist.LOAN_ID == datum["LOAN_ID"])).orderby(hist.TRANSACTION_DATE, order=Order.desc))["data"]
                        succ = len(h)
                        if succ >= 3:
                            h6 = ",".join(str(v) for k, v in six.iteritems(
                                h[2]) if k != 'LOAN_ID')
                            h5 = ",".join(str(v) for k, v in six.iteritems(
                                h[1]) if k != 'LOAN_ID')
                            h4 = ",".join(str(v) for k, v in six.iteritems(
                                h[0]) if k != 'LOAN_ID')
                        elif succ == 2:
                            h6 = ",,"
                            h5 = ",".join(str(v) for k, v in six.iteritems(
                                h[1]) if k != 'LOAN_ID')
                            h4 = ",".join(str(v) for k, v in six.iteritems(
                                h[0]) if k != 'LOAN_ID')
                        elif succ == 1:
                            h6, h5 = ",,", ",,"
                            h4 = ",".join(str(v) for k, v in six.iteritems(
                                h[0]) if k != 'LOAN_ID')
                        else:
                            h6, h5, h4 = ",,", ",,", ",,"
                        # print h3, h2, h1
                        e = Query.from_(emis).select(
                            "DUE_DATE", "TOTAL_DUE_FOR_PERIOD")
                        e = db.runQuery(e.where((emis.CUSTOMER_ID == datum["CUSTOMER_ID"]) &
                                                (emis.LOAN_ACCOUNT_NO == datum["LOAN_ID"])))["data"]
                        due_date, datum["EMI"] = (
                            (e[0]["DUE_DATE"] if e else ""), (e[0]["TOTAL_DUE_FOR_PERIOD"] if e else datum["EMI"]))
                        dates = [(ele["REPAY_DATETIME"], ele["REPAY_AMOUNT"])
                                 for ele in d]
                        temp_7 = sum(ele[1] for ele in dates if ele[0] > int(
                            (datetime.now() - timedelta(days=7)).strftime("%s")))
                        datum.update({"TEMP_7": temp_7, "TEMP_ALL": sum(ele[1] for ele in dates), "DUE_DATE": due_date, "h": [
                                     h1, h2, h3, h4, h5, h6], "EMIS": e, "FAILURES": fails, "SUCCESSES": succ})
                    today = datetime.now().strftime("%d/%m/%Y")
                    with open("temp.csv", 'w',encoding='utf-8') as f:
                        f.write("COMPANY,UTILITYCODE,TRANSACTIONTYPE,SETTLEMENTDATE,BENEFICIARYACHOLDERNAME,AMOUNT,DESTINATIONBANKCODE," +
                                "BENEFICIARYACNO,TRANSACTIONREFERENCE,LOAN_REF_ID,UMRN,CUSTOMER_ID,MANDATE_TYPE,WEEKLY,MATURITY_DATE,OUTSTANDING," +
                                "TEMP_7,TEMP_ALL,first_emi_date,emi_passed,OriginalOs,Exp_repayment,ExpOs,no_of_emi,ActOs-ExpOs,ActOs-ExpOs-temp," +
                                "amount,date,status,amount,date,status,amount,date,status,no_of_failures,"
                                "amount,date,status,amount,date,status,amount,date,status,no_of_success\n")
                        for i, ele in enumerate(data["data"]):
                            # print ele
                            # (5120 if ((ele["LOAN_PRODUCT_ID"]=='44') & (ele["LOAN_DISBURSED_DATE"]<'2018-07-31'))
                            original_outstanding = sum(
                                e2["TOTAL_DUE_FOR_PERIOD"] for e2 in ele["EMIS"])
                            # else 5200 if (ele["LOAN_PRODUCT_ID"]=="44") else 10400 if (ele["LOAN_PRODUCT_ID"]=='45')
                            # else 15800 if (ele["LOAN_PRODUCT_ID"]=="52") else 5125)
                            # (4 if (ele["LOAN_PRODUCT_ID"]=='44') else 8 if (ele["LOAN_PRODUCT_ID"]=='45') else 10
                            emi_no = len(ele["EMIS"])
                            # if (ele["LOAN_PRODUCT_ID"]=="52") else 1)
                            emi_passed = ((datetime.strptime(wed, "%Y-%m-%d")-datetime.strptime(ele["DUE_DATE"], "%Y-%m-%d %H:%M:%S")).days/7 + 1
                                          if ele["DUE_DATE"] else 0)
                            emi_passed = (4 if ((ele["LOAN_PRODUCT_ID"] == '44') & (emi_passed > 4)) else 8
                                          if ((ele["LOAN_PRODUCT_ID"] == "45") & (emi_passed > 8)) else 10
                                          if ((ele["LOAN_PRODUCT_ID"] == '52') & (emi_passed > 10)) else 12
                                          if ((ele["LOAN_PRODUCT_ID"] in ['2', '1']) & (emi_passed > 12)) else emi_passed if emi_no != 1 else 1)
                            f.write("%s,%s,ACH DR,%s," % (ele["COMPANY_NAME"], ele["UTILITY_CODE"] if ele["UTILITY_CODE"] else "NACH00000000004122", today) +
                                    "%s,'',%s,%s," % ((ele["ACCOUNT_HOLDER_NAME"] if ele["ACCOUNT_HOLDER_NAME"] else
                                                       (ele["NAME"] if ele["NAME"] else "")), ele["IFSC_CODE"], ele["ACCOUNT_NUMBER"]) +
                                    "%s,%s,%s,%s,%s," % (ele["LOAN_ID"].lstrip("0"), ele["LOAN_REFERENCE_ID"], ele["REF_NO"], ele["CUSTOMER_ID"], ele["MANDATE_FORMAT"]) +
                                    "%s,%s," % ("1" if ele["LOAN_PRODUCT_ID"] != "43" else "0", ele["EXPECTED_MATURITY_DATE"]) +
                                    "%s,%s,%s," % (ele["TOTAL_OUTSTANDING"], ele["TEMP_7"], ele["TEMP_ALL"]) +
                                    "%s,%s,%s,%s," % (ele["DUE_DATE"], emi_passed, original_outstanding, emi_passed*ele["EMI"]) +
                                    "%s,%s," % (original_outstanding-(emi_passed*ele["EMI"]), emi_no) +
                                    "%s," % (ele["TOTAL_OUTSTANDING"]-(original_outstanding-emi_passed*ele["EMI"])) +
                                    "%s," % (ele["TOTAL_OUTSTANDING"]-(original_outstanding-emi_passed*ele["EMI"])-ele["TEMP_7"]) +
                                    "%s,%s,%s,%s,%s,%s,%s,%s\n" % (ele["h"][0], ele["h"][1], ele["h"][2], ele["FAILURES"], ele["h"][3], ele["h"][4], ele["h"][5], ele["SUCCESSES"]))
                    junk = s3.Object(bucket, folder + filename).put(Body=open("temp.csv", 'rb'))
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=filename,
                                         DOCUMENT_FOLDER=folder.strip("/"), UPLOAD_MODE="SmartDash Admin", DOCUMENT_STATUS="N",
                                         CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docSeqID = db.runQuery(Query.from_(docs).select("DOC_SEQ_ID").orderby(
                        docs.DOC_SEQ_ID, order=Order.desc).limit(1))["data"]
                    #token = generate(db).AuthToken(exp=0)
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
