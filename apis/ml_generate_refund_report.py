from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, Order, functions


class RefundReportResource:

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
                    folder = "ml_refund_reports/"
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
                    loanprod = Table(
                        "mw_finflux_loan_product_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    bankdetails = Table(
                        "mw_cust_bank_detail", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    docs = Table("mw_other_documents", schema="mint_loan")
                    charges = Table("mw_charges_master", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    disb = Table("mw_disbursal_report", schema="mint_loan")
                    lender = (
                        input_dict["data"]["lender"] if "lender" in input_dict["data"] else "CHAITANYA")
                    q = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    join = q.join(bankdetails, how=JoinType.left).on(
                        loanmaster.CUSTOMER_ID == bankdetails.CUSTOMER_ID)
                    join = join.join(kyc, how=JoinType.left).on_field("CUSTOMER_ID").join(
                        profile, how=JoinType.left).on_field("CUSTOMER_ID")
                    join = join.join(loanprod, how=JoinType.left).on(
                        loanprod.PRODUCT_ID == loanmaster.LOAN_PRODUCT_ID)
                    q = join.select(loandetails.APPROVED_PRINCIPAL, loandetails.PRINCIPAL, loanmaster.LOAN_APPLICATION_NO.as_("LOAN_ID"),
                                    loanmaster.AMOUNT, loanmaster.LOAN_ACCOUNT_NO, kyc.NAME, bankdetails.ACCOUNT_HOLDER_NAME, loanmaster.CUSTOMER_ID,
                                    bankdetails.IFSC_CODE, bankdetails.ACCOUNT_NO, profile.NAME.as_("PROFILE_NAME"), loanmaster.LOAN_PRODUCT_ID, profile.COMPANY_NAME)
                    data = db.runQuery(q.where((loanmaster.LENDER == lender) & (loanmaster.LOAN_REFERENCE_ID.isin(input_dict["data"]["loanRefIDs"])) &
                                               (bankdetails.DELETE_STATUS_FLAG == 0) & (bankdetails.DEFAULT_STATUS_FLAG == 1)))["data"]
                    # print data
                    today = datetime.now().strftime("%d/%m/%Y")
                    with open("temp.csv", 'w',encoding='utf-8') as f:
                        f.write(
                            "Beneficiary Name,Beneficiary A/c No.,IFS Code,Amount,Remarks\n")
                        acc_no = []
                        for i, ele in enumerate(data):
                            f.write("%s," % (ele["ACCOUNT_HOLDER_NAME"] if ele["ACCOUNT_HOLDER_NAME"] else (ele["NAME"] if ele["NAME"] else "")) +
                                    "'%s,%s,100,REFUND%s\n" % (ele["ACCOUNT_NO"], ele["IFSC_CODE"], ele["CUSTOMER_ID"]))
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
#                resp.content_type = mimetypes.guess_type(filename)[0]
#                resp.stream = s3.Object(bucket,folder + filename).get()['Body']
#                resp.stream_len = s3.Object(bucket,folder + filename).content_length
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
