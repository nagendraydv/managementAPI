from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, Order


class UberPaymentOrderResource:

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
                    folder = "ml_uber_payment_orders/"
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
                    ldetails = Table("mw_client_loan_details",
                                     schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    uuidm = Table("mw_driver_uuid_master", schema="mint_loan")
                    emi = Table("mw_finflux_emi_packs_master",
                                schema="mint_loan")
                    docs = Table("mw_other_documents", schema="mint_loan")
                    repay = Table("mw_loan_repayment_data", schema="mint_loan")
                    emis = Table("mw_client_loan_emi_details",
                                 schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    days_left = 2-datetime.today().weekday()
                    days_left += 7 if days_left < 0 else 0
                    wed = (datetime.today() + timedelta(days=days_left)
                           ).strftime("%Y-%m-%d")
                    # .join(income, how=JoinType.left).on(loanmaster.CUSTOMER_ID==income.CUSTOMER_ID)
                    q = Query.from_(loanmaster).join(prof, how=JoinType.left).on(
                        loanmaster.CUSTOMER_ID == prof.CUSTOMER_ID)
                    join = q.join(emi, how=JoinType.left).on_field(
                        "LOAN_PRODUCT_ID").join(ldetails, how=JoinType.left)
                    join = join.on(loanmaster.ID == ldetails.LOAN_MASTER_ID).select(
                        ldetails.TOTAL_OUTSTANDING, ldetails.EXPECTED_MATURITY_DATE, ldetails.CURRENT_OUTSTANDING)
                    q = join.select(loanmaster.LOAN_ACCOUNT_NO, emi.EMI, loanmaster.LOAN_PRODUCT_ID, loanmaster.LOAN_DISBURSED_DATE,
                                    loanmaster.LOAN_REFERENCE_ID, loanmaster.CUSTOMER_ID, loanmaster.STATUS, prof.COMPANY_NAME)
                    # print db.pikastr(q.where(loanmaster.LOAN_REFERENCE_ID.isin(input_dict["data"]["loanRefIDs"])).groupby(loanmaster.CUSTOMER_ID))
                    if input_dict["data"]["loanRefIDs"] == '' or input_dict["data"]["loanRefIDs"] == []:
                        data = db.runQuery(q.where((loanmaster.STATUS.isin(['ACTIVE', 'WRITTEN-OFF'])) & (
                            ldetails.LOAN_MASTER_ID.notnull())).groupby(loanmaster.LOAN_ACCOUNT_NO))
                    else:
                        data = db.runQuery(q.where(loanmaster.LOAN_ACCOUNT_NO.isin(
                            input_dict["data"]["loanRefIDs"])).groupby(loanmaster.LOAN_ACCOUNT_NO))
                    with open("uber.csv", 'w',encoding='utf-8') as f:
                        f.write(("company,uber_user_uuid,financier_name,lender_contract_id,amount,emi,description,customer_id,outstanding,maturity_date,") +
                                ("TEMP_7,TEMP_all,first_emi_date,emi_passed,OriginalOs,Exp_repayment,ExpOs,no_of_emi,ActOs-ExpOs,ActOs-ExpOs-temp,product_id,overdue_minus_temp,overdue_plus_emi_minus_temp\n"))
                        for i, ele in enumerate(data["data"]):
                            #u = db.runQuery(Query.from_(income).select(income.DRIVER_UUID).where(income.CUSTOMER_ID==ele["CUSTOMER_ID"]).orderby(income.WEEK, order=Order.desc).limit(1))["data"]
                            u = db.runQuery(Query.from_(uuidm).select(uuidm.DRIVER_UUID).where(
                                uuidm.CUSTOMER_ID == ele["CUSTOMER_ID"]))["data"]
                            d = db.runQuery(Query.from_(repay).select(repay.star).where((repay.LOAN_REF_ID == ele["LOAN_REFERENCE_ID"]) &
                                                                                        (repay.FINFLUX_TRAN_ID.isnull())))["data"]
                            e = Query.from_(emis).select(
                                "DUE_DATE", "OVERDUE_AMOUNT", "TOTAL_DUE_FOR_PERIOD", "TOTAL_PAID_FOR_PERIOD")
                            e = db.runQuery(e.where((emis.CUSTOMER_ID == ele["CUSTOMER_ID"]) & (emis.TOTAL_DUE_FOR_PERIOD > 0) &
                                                    (emis.LOAN_ACCOUNT_NO == ele["LOAN_ACCOUNT_NO"])))["data"]
                            overdue = sum(X["OVERDUE_AMOUNT"] for X in e if X["OVERDUE_AMOUNT"]
                                          ) if ele["STATUS"] == 'ACTIVE' else ele["CURRENT_OUTSTANDING"]
                            due_date, ele["EMI"] = (
                                (e[0]["DUE_DATE"] if e else ""), (e[0]["TOTAL_DUE_FOR_PERIOD"] if e else ele["EMI"]))
                            dates = [(datum["REPAY_DATETIME"],
                                      datum["REPAY_AMOUNT"]) for datum in d]
                            temp_7 = sum(datum[1] for datum in dates if datum[0] > int(
                                (datetime.now() - timedelta(days=7)).strftime("%s")))
                            ele.update({"TEMP_7": temp_7, "TEMP_ALL": sum(
                                datum[1] for datum in dates), "DUE_DATE": due_date, "EMIS": e, "DRIVER_UUID": u[0]["DRIVER_UUID"] if u else None, "OVERDUE_AMOUNT": overdue if overdue else 0})
                            original_outstanding = sum(
                                e2["TOTAL_DUE_FOR_PERIOD"] for e2 in ele["EMIS"]) if ele["EMIS"] else ele["TOTAL_OUTSTANDING"]
                            emi_no = len(ele["EMIS"])
                            emi_passed_1 = ((datetime.strptime(wed, "%Y-%m-%d").date()-datetime.strptime(ele["DUE_DATE"], "%Y-%m-%d %H:%M:%S").date()).days/7 +
                                            1) if ele["DUE_DATE"] else 0
                            emi_passed = (
                                emi_passed_1 if emi_passed_1 < emi_no else emi_no)
                            first_emi_due = (datetime.strptime(
                                due_date, "%Y-%m-%d %H:%M:%S")-datetime.strptime(wed, "%Y-%m-%d")).days <= 0 if due_date else False
                            # ele["EMI"] if overdue<(ele["TOTAL_OUTSTANDING"]-ele["EMI"]) else 0 if sum(X["TOTAL_PAID_FOR_PERIOD"] for X in e if X["TOTAL_PAID_FOR_PERIOD"])>(ele["EMI"]*emi_no) or else ele["EMI"]
                            nextEmi = 0
                            overdue_plus_emi = (ele["OVERDUE_AMOUNT"]+(nextEmi if (
                                (emi_passed_1 <= emi_no) & (first_emi_due)) else 0)-(temp_7 if temp_7 else 0))
                            actos_expos_tmp = (ele["TOTAL_OUTSTANDING"] if ele["STATUS"] == 'ACTIVE' else ele["CURRENT_OUTSTANDING"] if ("TOTAL_OUTSTANDING" in ele) and (
                                "CURRENT_OUTSTANDING" in ele) and ("STATUS" in ele) else 0)-(original_outstanding-emi_passed*ele["EMI"])-ele["TEMP_ALL"]
                            # emi_passed = (4 if ((ele["LOAN_PRODUCT_ID"]=='44') & (emi_passed>4)) else 8
                            #              if ((ele["LOAN_PRODUCT_ID"]=="45") & (emi_passed>8)) else 10
                            #              if ((ele["LOAN_PRODUCT_ID"]=='52') & (emi_passed>10)) else 12
                            #              if ((ele["LOAN_PRODUCT_ID"] in ['2','1']) & (emi_passed>12)) else emi_passed if emi_no!=1 else 1)
                            f.write(("%s,%s,mintwalk,%s,%s,%s,%s,%s," % (ele["COMPANY_NAME"], ele["DRIVER_UUID"], ele["LOAN_ACCOUNT_NO"],
                                                                         (ele["EMI"] if (ele["TOTAL_OUTSTANDING"]-ele["EMI"]) > 10
                                                                          else ele["TOTAL_OUTSTANDING"]), ele["EMI"],
                                                                         wed, ele["CUSTOMER_ID"]) +
                                     "%s,%s,%s,%s," % (ele["TOTAL_OUTSTANDING"], ele["EXPECTED_MATURITY_DATE"], ele["TEMP_7"], ele["TEMP_ALL"]) +
                                     "%s,%s,%s,%s," % (ele["DUE_DATE"], emi_passed, original_outstanding, emi_passed*ele["EMI"]) +
                                     "%s,%s," % (original_outstanding-(emi_passed*ele["EMI"]), emi_no) +
                                     "%s," % ((ele["TOTAL_OUTSTANDING"] if ele["TOTAL_OUTSTANDING"] else 0)-(original_outstanding-emi_passed*ele["EMI"])) +
                                     "%s,%s," % (actos_expos_tmp, ele["LOAN_PRODUCT_ID"]) +
                                     "%s,%s\n" % (ele["OVERDUE_AMOUNT"]-(temp_7 if temp_7 else 0), max(min(ele["EMI"], (ele["TOTAL_OUTSTANDING"] if ele["STATUS"] == 'ACTIVE' else ele["CURRENT_OUTSTANDING"] if ("TOTAL_OUTSTANDING" in ele) and ("CURRENT_OUTSTANDING" in ele) and ("STATUS" in ele) else 0)-ele["TEMP_ALL"]), actos_expos_tmp))))
                    junk = s3.Object(bucket, folder + filename).put(Body=open("uber.csv", 'rb'))
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
