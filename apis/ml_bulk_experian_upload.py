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
from datetime import time,date
import requests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, JoinType, Order
from dateutil.relativedelta import relativedelta
from pypika import functions as fn
import urllib3
from six.moves import range
from six.moves import zip
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BulkExperianUploadResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits
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
        success = "Experian bulk uploaded successfully"
        try:
            data = {"docType": req.get_param("docType")}#, "forceUpdate": req.get_param("forceUpdate")}
            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),"timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            bulkExperianData = req.get_param("bulkExperianData")
        except:
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "datafeed.mintwalk.com"  # "uat.secureasset.mintwalk.com"
        folder = "bulk_experian/"
        if ((not validate.Request(api='bulkExperianUpload', request={"msgHeader": msgHeader, "data": data})) or
                (bulkExperianData.filename.split('.')[-1] not in ("xlsx", "csv","xls"))):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
        else:
            try:
                suffix = bulkExperianData.filename.split('.')[-1]                
                filename = bulkExperianData.filename.split('.')[0] + '.' + suffix
                s3path = s3url + bucket + '/' + folder + filename
                session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",
                                        aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")
                s3 = session.resource('s3')
                junk = s3.meta.client.head_bucket(Bucket=bucket)
            except botocore.exceptions.ClientError as e:
                raise falcon.HTTPError(falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
            try:
                db = DB(msgHeader["authLoginID"], dictcursor=True)
                val_error = validate(db).basicChecks(token=msgHeader["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                elif db.Query(primaryTable="mw_other_documents", fields={"A": ["*"]}, conditions={"DOCUMENT_URL =": s3path}, Data=False)["count"] == 0:
                    inserted = db.Insert(db="mint_loan", table='mw_other_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         UPLOAD_MODE="SmartDash Admin", CREATED_BY=msgHeader["authLoginID"],
                                         DOCUMENT_FOLDER=folder,DOCUMENT_STATUS='N',
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    docID = db.Query(db="mint_loan", primaryTable='mw_other_documents', fields={"A": ["DOC_SEQ_ID"]}, orderBy="DOC_SEQ_ID desc",
                                     limit=1)
                    if docID["data"] != []:
                        docID = str(docID["data"][0]["DOC_SEQ_ID"])
                    else:
                        docID = None
                    junk = s3.Object(bucket, folder + filename).put(Body=bulkExperianData.file.read())
                    junk = s3.Bucket(bucket).download_file(folder + filename, "/tmp/experian." + suffix)
                    md = xlrd.open_workbook(filename="/tmp/experian." + suffix, encoding_override='unicode-escape')
                    #print('true')
                    sheet = md.sheet_by_index(1)
                    d = [sheet.row_slice(i) for i in range(sheet.nrows)]
                    sheet1 = md.sheet_by_index(0)
                    d_score = [sheet1.row_slice(i) for i in range(sheet1.nrows)]
                    #print(d_score)
                    report_number = str(int(datetime.timestamp(datetime.strptime(str(date.today()),"%Y-%m-%d")))+int(str(docID)[-3:]))
                    customer_log=[]
                    #print(report_number)
                    cais_acc_summary=Table('cais_account_summary',schema="experian_credit_details")
                    trans_req_log = Table('transaction_request_log',schema = "experian_credit_details")
                    cais_acc_detail = Table('cais_account_details',schema = "experian_credit_details")
                    acc_type = Table('account_type',schema="experian_credit_details")
                    acc_holder_type = Table('account_holder_type',schema = "experian_credit_details")
                    suit_filed_code = Table('suit_filed_wilful_default',schema="experian_credit_details")
                    score_mapping ={"CUSTOMERID":"customer_id","score":"bureau_score"}
                    cais_acc_hist_mapp = {'DAYS_PAST_DUE_01': 'days_past_due_1','DAYS_PAST_DUE_02': 'days_past_due_2','DAYS_PAST_DUE_03': 'days_past_due_3','DAYS_PAST_DUE_04': 'days_past_due_4','DAYS_PAST_DUE_05': 'days_past_due_5','DAYS_PAST_DUE_06': 'days_past_due_6','DAYS_PAST_DUE_07': 'days_past_due_7','DAYS_PAST_DUE_08': 'days_past_due_8',
                                          'DAYS_PAST_DUE_09': 'days_past_due_9','DAYS_PAST_DUE_10': 'days_past_due_10','DAYS_PAST_DUE_11': 'days_past_due_11','DAYS_PAST_DUE_12': 'days_past_due_12','DAYS_PAST_DUE_13': 'days_past_due_13','DAYS_PAST_DUE_14': 'days_past_due_14',
                                          'DAYS_PAST_DUE_15': 'days_past_due_15','DAYS_PAST_DUE_16': 'days_past_due_16','DAYS_PAST_DUE_17': 'days_past_due_17','DAYS_PAST_DUE_18': 'days_past_due_18','DAYS_PAST_DUE_19': 'days_past_due_19','DAYS_PAST_DUE_20': 'days_past_due_20',
                                          'DAYS_PAST_DUE_21': 'days_past_due_21','DAYS_PAST_DUE_22': 'days_past_due_22','DAYS_PAST_DUE_23': 'days_past_due_23','DAYS_PAST_DUE_24': 'days_past_due_24'}
                    adv_acc_hist_balance_amt = {'BALANCE_AM_01': 'current_bal_1','BALANCE_AM_02': 'current_bal_2','BALANCE_AM_03': 'current_bal_3','BALANCE_AM_04': 'current_bal_4','BALANCE_AM_05': 'current_bal_5',
                                         'BALANCE_AM_06': 'current_bal_6','BALANCE_AM_07': 'current_bal_7','BALANCE_AM_08': 'current_bal_8','BALANCE_AM_09': 'current_bal_9','BALANCE_AM_10': 'current_bal_10',
                                         'BALANCE_AM_11': 'current_bal_11','BALANCE_AM_12': 'current_bal_12','BALANCE_AM_13': 'current_bal_13','BALANCE_AM_14': 'current_bal_14','BALANCE_AM_15': 'current_bal_15',
                                         'BALANCE_AM_16': 'current_bal_16','BALANCE_AM_17': 'current_bal_17','BALANCE_AM_18': 'current_bal_18','BALANCE_AM_19': 'current_bal_19','BALANCE_AM_20': 'current_bal_20',
                                         'BALANCE_AM_21': 'current_bal_21','BALANCE_AM_22': 'current_bal_22','BALANCE_AM_23': 'current_bal_23','BALANCE_AM_24': 'current_bal_24'}
                    adv_acc_hist_actual_pay ={'ACTUAL_PAYMENT_AM_01': 'Actual_payment_amount_1','ACTUAL_PAYMENT_AM_02': 'Actual_payment_amount_2','ACTUAL_PAYMENT_AM_03': 'Actual_payment_amount_3','ACTUAL_PAYMENT_AM_04': 'Actual_payment_amount_4','ACTUAL_PAYMENT_AM_05': 'Actual_payment_amount_5',
                                              'ACTUAL_PAYMENT_AM_06': 'Actual_payment_amount_6','ACTUAL_PAYMENT_AM_07': 'Actual_payment_amount_7','ACTUAL_PAYMENT_AM_08': 'Actual_payment_amount_8','ACTUAL_PAYMENT_AM_09': 'Actual_payment_amount_9','ACTUAL_PAYMENT_AM_10': 'Actual_payment_amount_10',
                                              'ACTUAL_PAYMENT_AM_11': 'Actual_payment_amount_11','ACTUAL_PAYMENT_AM_12': 'Actual_payment_amount_12','ACTUAL_PAYMENT_AM_13': 'Actual_payment_amount_13','ACTUAL_PAYMENT_AM_14': 'Actual_payment_amount_14','ACTUAL_PAYMENT_AM_15': 'Actual_payment_amount_15',
                                              'ACTUAL_PAYMENT_AM_16': 'Actual_payment_amount_16','ACTUAL_PAYMENT_AM_17': 'Actual_payment_amount_17','ACTUAL_PAYMENT_AM_18': 'Actual_payment_amount_18','ACTUAL_PAYMENT_AM_19': 'Actual_payment_amount_19','ACTUAL_PAYMENT_AM_20': 'Actual_payment_amount_20',
                                              'ACTUAL_PAYMENT_AM_21': 'Actual_payment_amount_21','ACTUAL_PAYMENT_AM_22': 'Actual_payment_amount_22','ACTUAL_PAYMENT_AM_23': 'Actual_payment_amount_23','ACTUAL_PAYMENT_AM_24': 'Actual_payment_amount_24'}
                    adv_acc_hist_credit_limit = {'CREDIT_LIMIT_AM_01': 'Credit_limit_amount_1','CREDIT_LIMIT_AM_02': 'Credit_limit_amount_2','CREDIT_LIMIT_AM_03': 'Credit_limit_amount_3','CREDIT_LIMIT_AM_04': 'Credit_limit_amount_4','CREDIT_LIMIT_AM_05': 'Credit_limit_amount_5',
                                                 'CREDIT_LIMIT_AM_06': 'Credit_limit_amount_6','CREDIT_LIMIT_AM_07': 'Credit_limit_amount_7','CREDIT_LIMIT_AM_08': 'Credit_limit_amount_8','CREDIT_LIMIT_AM_09': 'Credit_limit_amount_9','CREDIT_LIMIT_AM_10': 'Credit_limit_amount_10',
                                                 'CREDIT_LIMIT_AM_11': 'Credit_limit_amount_11','CREDIT_LIMIT_AM_12': 'Credit_limit_amount_12','CREDIT_LIMIT_AM_13': 'Credit_limit_amount_13','CREDIT_LIMIT_AM_14': 'Credit_limit_amount_14','CREDIT_LIMIT_AM_15': 'Credit_limit_amount_15',
                                                 'CREDIT_LIMIT_AM_16': 'Credit_limit_amount_16','CREDIT_LIMIT_AM_17': 'Credit_limit_amount_17','CREDIT_LIMIT_AM_18': 'Credit_limit_amount_18','CREDIT_LIMIT_AM_19': 'Credit_limit_amount_19','CREDIT_LIMIT_AM_20': 'Credit_limit_amount_20',
                                                 'CREDIT_LIMIT_AM_21': 'Credit_limit_amount_21','CREDIT_LIMIT_AM_22': 'Credit_limit_amount_22','CREDIT_LIMIT_AM_23': 'Credit_limit_amount_23','CREDIT_LIMIT_AM_24': 'Credit_limit_amount_24'}
                    adv_acc_hist_day_past_due = {"PAST_DUE_AM_01":"past_due_amt_1","PAST_DUE_AM_02":"past_due_amt_2","PAST_DUE_AM_03":"past_due_amt_3","PAST_DUE_AM_04":"past_due_amt_4","PAST_DUE_AM_05":"past_due_amt_5","PAST_DUE_AM_06":"past_due_amt_6",
                                                 "PAST_DUE_AM_07":"past_due_amt_7","PAST_DUE_AM_08":"past_due_amt_8","PAST_DUE_AM_09":"past_due_amt_9","PAST_DUE_AM_10":"past_due_amt_10","PAST_DUE_AM_11":"past_due_amt_11","PAST_DUE_AM_12":"past_due_amt_12",
                                                 "PAST_DUE_AM_13":"past_due_amt_13","PAST_DUE_AM_14":"past_due_amt_14","PAST_DUE_AM_15":"past_due_amt_15","PAST_DUE_AM_16":"past_due_amt_16","PAST_DUE_AM_17":"past_due_amt_17","PAST_DUE_AM_18":"past_due_amt_18",
                                                 "PAST_DUE_AM_19":"past_due_amt_19","PAST_DUE_AM_20":"past_due_amt_20","PAST_DUE_AM_21":"past_due_amt_21","PAST_DUE_AM_22":"past_due_amt_22","PAST_DUE_AM_23":"past_due_amt_23","PAST_DUE_AM_24":"past_due_amt_24"}
                    adv_acc_hist_payment_rating = {"PAYMENT_RATING_CD_01":"payment_rating_1","PAYMENT_RATING_CD_02":"payment_rating_2","PAYMENT_RATING_CD_03":"payment_rating_3","PAYMENT_RATING_CD_04":"payment_rating_4","PAYMENT_RATING_CD_05":"payment_rating_5","PAYMENT_RATING_CD_06":"payment_rating_6",
                                                   "PAYMENT_RATING_CD_07":"payment_rating_7","PAYMENT_RATING_CD_08":"payment_rating_8","PAYMENT_RATING_CD_09":"payment_rating_9","PAYMENT_RATING_CD_10":"payment_rating_10","PAYMENT_RATING_CD_11":"payment_rating_11","PAYMENT_RATING_CD_12":"payment_rating_12",
                                                   "PAYMENT_RATING_CD_13":"payment_rating_13","PAYMENT_RATING_CD_14":"payment_rating_14","PAYMENT_RATING_CD_15":"payment_rating_15","PAYMENT_RATING_CD_16":"payment_rating_16","PAYMENT_RATING_CD_17":"payment_rating_17","PAYMENT_RATING_CD_18":"payment_rating_18",
                                                   "PAYMENT_RATING_CD_19":"payment_rating_19","PAYMENT_RATING_CD_20":"payment_rating_20","PAYMENT_RATING_CD_21":"payment_rating_21","PAYMENT_RATING_CD_22":"payment_rating_22","PAYMENT_RATING_CD_23":"payment_rating_23","PAYMENT_RATING_CD_24":"payment_rating_24"}
                    mapping = {"CUSTOMERID":"customer_id","ACCOUNT_NB":"account_number","M_SUB_ID":"entity_type",
                               "ACCT_TYPE_CD":"account_type","OPEN_DT":"open_date","ACTUAL_PAYMENT_AM":"actual_payment_amount","ASSET_CLASS_CD":"asset_classification",
                               "BALANCE_AM":"current_balance","BALANCE_DT":"date_reported","CHARGE_OFF_AM":"original_chargeoff_amount","CLOSED_DT":"date_closed",
                               "CREDIT_LIMIT_AM":"credit_limit_amount","DAYS_PAST_DUE":"days_past_due","DFLT_STATUS_DT":"default_status_date","LAST_PAYMENT_DT":"dateof_last_payment",
                               "ORIG_LOAN_AM":"highest_creditor_original_loan_amount","PAST_DUE_AM":"amount_past_due",
                               "PAYMENT_HISTORY_GRID":"payment_history_profile","SUIT_FILED_WILLFUL_DFLT":"suit_filed_wilful_default",
                               "WRITTEN_OFF_AND_SETTLED_STATUS":"writtenoff_settled_status","WRITE_OFF_STATUS_DT":"write_off_status_date",
                               "RESPONSIBILITY_CD":"account_holdertype_code","PORTFOLIO_RATING_TYPE_CD":"portfolio_type"}
                    mapping1 = {"CUSTOMERID":"customer_id"}
                    mapping0 = {"CUSTOMERID":"customer_id","score":"bureau_score"}
                    ind1, h1 = list(zip(*[(i, mapping1[x.value])for i, x in enumerate(d_score[0]) if x.value in mapping1]))
                    ind0, h0 = list(zip(*[(i, mapping0[x.value])for i, x in enumerate(d_score[0]) if x.value in mapping0]))
                    ind, h = list(zip(*[(i, mapping[x.value])for i, x in enumerate(d[0]) if x.value in mapping]))
                    ind_score, h_score = list(zip(*[(i, score_mapping[x.value])for i, x in enumerate(d_score[0]) if x.value in score_mapping]))
                    ind2,h2 = list(zip(*[(i, cais_acc_hist_mapp[x.value])for i, x in enumerate(d[0]) if x.value in cais_acc_hist_mapp]))
                    ind3,h3 = list(zip(*[(i, adv_acc_hist_balance_amt[x.value])for i, x in enumerate(d[0]) if x.value in adv_acc_hist_balance_amt]))
                    ind4,h4 = list(zip(*[(i, adv_acc_hist_actual_pay[x.value])for i, x in enumerate(d[0]) if x.value in adv_acc_hist_actual_pay]))
                    ind5,h5 = list(zip(*[(i, adv_acc_hist_credit_limit[x.value])for i, x in enumerate(d[0]) if x.value in adv_acc_hist_credit_limit]))
                    ind6,h6 = list(zip(*[(i, adv_acc_hist_day_past_due[x.value])for i, x in enumerate(d[0]) if x.value in adv_acc_hist_day_past_due]))
                    ind7,h7 = list(zip(*[(i, adv_acc_hist_payment_rating[x.value])for i, x in enumerate(d[0]) if x.value in adv_acc_hist_payment_rating]))
                    for i in range(1,len(d_score)):
                        r1 = dict(list(zip(h1, [self.formatValue(y, md) for j, y in enumerate(d_score[i]) if j in ind1])))
                        r0 = dict(list(zip(h0, [self.formatValue(y, md) for j, y in enumerate(d_score[i]) if j in ind0])))
                        inserted = db.Insert(db="experian_credit_details", table='credit_score_request_details', compulsory=False, date=False, 
                                         **utils.mergeDicts(r1, {"created_by":msgHeader["authLoginID"],
                                         "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                        credit_score_request_id = db.Query(db="experian_credit_details", primaryTable='credit_score_request_details', fields={
                                     "A": ["credit_score_request_id"]}, orderBy="credit_score_request_id desc", limit=1)
                        if credit_score_request_id["data"] != []:
                            credit_score_request_id = str(credit_score_request_id["data"][0]["credit_score_request_id"])
                        else:
                            credit_score_request_id = None
                        inserted = db.Insert(db="experian_credit_details", table='transaction_request_log', compulsory=False, date=False, 
                                         **utils.mergeDicts(r1, {"request":filename,"response":'SmartDash Admin',"credit_score_request_id":credit_score_request_id,
                                         "report_number":report_number,"report_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d"),
                                         "created_by":msgHeader["authLoginID"],"report_time":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%H:%M:%S"),"doc_id":docID,
                                         "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                        trans_req_log_id = db.Query(db="experian_credit_details", primaryTable='transaction_request_log', fields={
                                     "A": ["transaction_request_log_id"]}, orderBy="transaction_request_log_id desc", limit=1)
                        if trans_req_log_id["data"] != []:
                            trans_req_log_id = str(trans_req_log_id["data"][0]["transaction_request_log_id"])
                        else:
                            trans_req_log_id = None
                        inserted = db.Insert(db="experian_credit_details", table='cais_account_summary', compulsory=False, date=False, 
                                         **utils.mergeDicts(r1, {"transaction_request_log_id":trans_req_log_id,
                                         "created_by":msgHeader["authLoginID"],
                                         "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                        acc_summary_id = db.Query(db="experian_credit_details", primaryTable='cais_account_summary', fields={
                                     "A": ["account_summary_id"]}, orderBy="account_summary_id desc", limit=1)
                        if acc_summary_id["data"] != []:
                            acc_summary_id = str(acc_summary_id["data"][0]["account_summary_id"])
                        else:
                            acc_summary_id = None
                        inserted = db.Insert(db="experian_credit_details", table='customer_details', compulsory=False, date=False, 
                                         **utils.mergeDicts(r0, {"transaction_request_log_id":trans_req_log_id,
                                         "created_by":msgHeader["authLoginID"],
                                         "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                    for i in range(1, len(d)):
                        r2 = dict(list(zip(h2, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind2])))
                        r_balance_amt = dict(list(zip(h3, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind3])))
                        #print(r_balance_amt)
                        
                        balance_amount = []
                        balance_amount = [ele[1] for ele in r_balance_amt.items()]
                        #print(balance_amount)
                        r_actual_payment = dict(list(zip(h4, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind4])))
                        actual_payment_amount = []
                        actual_payment_amount = [ele[1] for ele in r_actual_payment.items()]
                        #print(actual_payment_amount)
                        r_credit_limit = dict(list(zip(h5, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind5])))
                        credit_limit = []
                        credit_limit = [ele[1] for ele in r_credit_limit.items()]
                        #print(len(credit_limit))
                        r_day_past_due = dict(list(zip(h6, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind6])))
                        amount_past_due = []
                        amount_past_due = [ele[1] for ele in r_day_past_due.items()]
                        r_payment_rating = dict(list(zip(h7, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind7])))
                        payment_rating = []
                        payment_rating = [ele[1] for ele in r_payment_rating.items()]    
                        r = dict(list(zip(h, [self.formatValue(y, md) for j, y in enumerate(d[i]) if j in ind])))
                        #q = Query.from_(trans_req_log).select("transaction_request_log_id").where(trans_req_log.customer_id==str(r['customer_id'])).orderBy(trans_req_log.created_date,limit=1)
                        trans_request_log_id = db.Query(db="experian_credit_details", primaryTable='transaction_request_log', fields={
                                     "A": ["transaction_request_log_id"]},conditions={"customer_id=":r['customer_id']}, orderBy="created_date desc", limit=1,debug = False)
                        #print(trans_request_log_id)
                        if trans_request_log_id["data"] != []:
                            trans_request_log_id = str(trans_request_log_id["data"][0]["transaction_request_log_id"])
                        else:
                            trans_request_log_id = None
                        #print(trans_request_log_id)
                        q = Query.from_(cais_acc_summary).select("account_summary_id").where(cais_acc_summary.transaction_request_log_id==trans_request_log_id)
                        account_summary_id = db.runQuery(q)
                        if account_summary_id["data"] != []:
                            account_summary_id = str(account_summary_id["data"][0]["account_summary_id"])
                        else:
                            account_summary_id = None
                        #print(account_summary_id)
                        code_old_q = Query.from_(acc_type).select("code_old").where(acc_type.code_new==r["account_type"])
                        code_old = db.runQuery(code_old_q)
                        if code_old["data"] !=[]:
                            code_old = str(code_old["data"][0]["code_old"])
                        else:
                            code_old = None
                        code_old_suit = Query.from_(suit_filed_code).select("code_old").where(suit_filed_code.code_new==r["suit_filed_wilful_default"])
                        code_old_suit = db.runQuery(code_old_suit)
                        if code_old_suit["data"] !=[]:
                            code_old_suit = str(code_old_suit["data"][0]["code_old"])
                        else:
                            code_old_suit = None
                        code_acc_holder_q = Query.from_(acc_holder_type).select("code_old").where(acc_holder_type.code_new==r["account_holdertype_code"])
                        code_acc_holder = db.runQuery(code_acc_holder_q)
                        if code_acc_holder["data"]!=[]:
                            code_acc_holder = str(code_acc_holder["data"][0]["code_old"])
                        else:
                            code_acc_holder = None
                        if (account_summary_id !=None) and (trans_request_log_id!=None):
                            inserted = db.Insert(db="experian_credit_details", table="cais_account_details", compulsory=False, date=False,debug =False,
                                                 **utils.mergeDicts(r, {"transaction_request_log_id":trans_request_log_id,"account_summary_id":account_summary_id,"open_date":(r['open_date'] if r["open_date"] not in (None,'') else 0),
                                                                        "date_reported":(r["date_reported"] if r["date_reported"] not in (None,'') else None),"CREATED_BY": msgHeader["authLoginID"],"actual_payment_amount":(r["actual_payment_amount"] if r["actual_payment_amount"] not in (None,'') else None),
                                                                        "original_chargeoff_amount":(r["original_chargeoff_amount"] if (r["original_chargeoff_amount"] not in (None,'')) else None ),"default_status_date":(r["default_status_date"] if (r["default_status_date"] not in (None,'') )else None),
                                                                        "write_off_status_date":(r["write_off_status_date"] if (r["write_off_status_date"] not in (None,'')) else None),"credit_limit_amount":(r["credit_limit_amount"] if (r["credit_limit_amount"] not in (None,'')) else None),
                                                                        "highest_creditor_original_loan_amount":(r["highest_creditor_original_loan_amount"] if (r["highest_creditor_original_loan_amount"] not in (None,''))else None),"current_balance":(r["current_balance"] if (r["current_balance"] not in (None,'')) else None),
                                                                        "amount_past_due":(r["amount_past_due"] if (r["amount_past_due"] not in (None,'')) else None),"days_past_due":(r["days_past_due"] if (r["days_past_due"] not in (None,''))else None),"account_type":(code_old if (code_old not in (None,'')) else None),
                                                                        "account_holdertype_code":(code_acc_holder if (code_acc_holder not in (None,'')) else None),"suit_filed_wilful_default":code_old_suit,"dateof_last_payment":(r["dateof_last_payment"] if (r["dateof_last_payment"] not in (None,'')) else 0),"date_closed":(r["date_closed"] if (r["date_closed"] not in (None,'')) else None),
                                                                        "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))
                        else:
                            customer_log.append(r["customer_id"])
                        acc_detail_id = db.Query(db="experian_credit_details", primaryTable='cais_account_details', fields={
                                     "A": ["account_detail_id"]}, orderBy="account_detail_id desc", limit=1)
                        if acc_detail_id["data"] != []:
                            acc_detail_id = str(acc_detail_id["data"][0]["account_detail_id"])
                        else:
                            acc_detail_id = None
                        q1 = Query.from_(cais_acc_detail).select(functions.Count(cais_acc_detail.account_number).as_(
                        "count")).where((cais_acc_detail.transaction_request_log_id == trans_request_log_id))
                        count_data = db.runQuery(q1)["data"]
                        if count_data!=[]:
                            count=count_data[0]["count"]
                        else:
                             count=0
                        #print(count)
                        indict1 = {"credit_account_total":str(count)}
                        if trans_request_log_id!=None:
                            inserted = db.Update(db="experian_credit_details", table="cais_account_summary", checkAll=False,debug =False,
                                                         conditions={"transaction_request_log_id=": trans_request_log_id},**indict1)
                        if trans_request_log_id!=None:
                            value=[]
                            for i,ele in enumerate(r2.items()):
                                tuple1=(trans_request_log_id,acc_detail_id,ele[1] if ele[1] not in (None,'') else None,i+1,datetime.now().strftime("%Y-%m-%d %H:%M:%S"),msgHeader["authLoginID"])
                                value.append(tuple1)
                            key = ['transaction_request_log_id','account_detail_id','days_past_due','time_period','CREATED_DATE','CREATED_BY']
                            #value = [trans_request_log_id,acc_detail_id,dayPastDue,timeperiod,'20-09-11 14:42:01','prasanna@mintwalk.com']
                            #value = ((trans_request_log_id,acc_detail_id,1,2,'20-09-11 14:42:01','prasanna@mintwalk.com'),(12421421,3532252,1,2,'20-09-11 14:42:01','prasanna@mintwalk.com'))
                            value = tuple(value)
                            #print(value)
                            inserted = db.InsertMany(db="experian_credit_details", table="cais_account_history", keys=key,values=value,debug =False)
                            #print(inserted)
                                                 #**utils.mergeDicts({"transaction_request_log_id":trans_request_log_id,"account_detail_id":str(acc_detail_id),"days_past_due":(ele[1] if (ele[1]) not in (None,'') else None) ,"time_period":str(i+1),"CREATED_BY": msgHeader["authLoginID"],
                                                  #                      "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))
                        if trans_request_log_id!=None:
                            #print("true")
                            value = []
                            for i in range(len(balance_amount)):
                                tuple1=(trans_request_log_id,acc_detail_id,i+1,msgHeader["authLoginID"],balance_amount[i] if balance_amount[i] not in (None,'') else None,actual_payment_amount[i] if actual_payment_amount[i] not in (None,'') else None,
                                        credit_limit[i] if credit_limit[i] not in (None,'') else None,amount_past_due[i] if amount_past_due[i] not in (None,'') else None,payment_rating[i] if payment_rating[i] not in (None,'') else None,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                value.append(tuple1)
                            key = ['transaction_request_log_id','account_detail_id','time_period','CREATED_BY','current_balance',
                                   'actual_payment_amount','credit_limit_amount','amount_past_due','payment_rating','CREATED_DATE']
                            value = tuple(value)
                            #print(key)
                            #print(value)
                            inserted = db.InsertMany(db="experian_credit_details", table="advanced_account_history",keys=key,values=value,debug =False)
                    if customer_log!=[]:
                        for i in range(len(customer_log)):
                            inserted = db.Insert(db="experian_credit_details", table='bulk_experian_upload_log', compulsory=False, date=False, 
                                         **utils.mergeDicts({"filename":filename,"report_number":report_number,"customer_id":customer_log[i],
                                                             "created_by":msgHeader["authLoginID"],"doc_id":docID,
                                                             "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                    if inserted:
                        #print(customer_log)
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            if customer_log!=[]:
                                output_dict["data"].update({"error": 0, "message": "file uploaded but some data not processed"})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 0, "message":success})
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
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update({"error": 1, "message":"file already exist"})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)        
                db._DbClose_()
                
                    #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            except Exception as ex:
                #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
                # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
                raise