from __future__ import absolute_import
import falcon
import json
import requests
import os
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType
import numpy as np
from datetime import datetime
from datetime import date
from calendar import monthrange


class EnkashWebhookResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)
    @staticmethod
    def last_day_of_month(date_value):
        return date_value.replace(day = monthrange(date_value.year, date_value.month)[1])
    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"error":0,'message':''},"msgHeader":{"authToken":""}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'enkashWebhook'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise
        try:
            db = DB(filename='mysql.config')
            clprofile=Table("mw_client_profile",schema="mint_loan")
            loanmaster = Table("mw_client_loan_master", schema="mint_loan")
            loandetails = Table("mw_client_loan_details", schema="mint_loan")
            loanlimit=Table("mw_client_loan_limit",schema='mint_loan')
            conf = Table("mw_configuration", schema="mint_loan_admin")
            custcred = Table("mw_customer_login_credentials", schema="mint_loan")
            clientmaster = Table("mw_finflux_client_master", schema="mint_loan")
            indict = input_dict
            lender="GETCLARITY"
            headers = (utils.finflux_headers[lender].copy() if lender in utils.finflux_headers else {})
            auth = utils.mifos_auth
            urlKey = "MIFOS_URL" 
            baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
            if baseurl["data"]:
                baseurl = baseurl["data"][0]["CONFIG_VALUE"]
            dict1={"ENCASH_CARD_TRANSACTION_ID":indict["enKashCardTransactionId"],"MASKED_CARD":indict["maskedCard"],
                                                         "ENKASH_CARD_ID":indict["enkashCardId"],"USER_ID":indict["userId"],"CARD_KIT_NUMBER":indict["cardKitNumber"],
                                                         "CARD_HOLDER_NAME":indict["cardHolderName"],"COMPANY_NAME":indict["companyName"],"COMPANY_ID":indict["companyId"],
                                                         "PRINCIPAL_AMOUNT":str(indict["principalAmount"]),"FCY_AMOUNT":str(indict["fcyAmount"]),"BALANCE":str(indict["balance"]),
                                                         "PG_REFERENCE_NUMBER":indict["pgReferenceNumber"],"DESCRIPTION":indict["description"],"CURRENCY":indict["currency"],
                                                         "FCY_CURRENCY":indict["fcyCurrency"],"CARD_TRANSACTION_TYPE":indict["cardTransactionType"],"STATUS":indict["status"],
                                                         "TYPE":indict["type"],"CREATED_ON":indict["createdOn"],"TXN_DATE":indict["txnDate"],"MODIFIED_ON":indict["modifiedOn"],
                                                         "ID":str(indict["txnDetails"]["id"]),"TXN_REF_NO":indict["txnDetails"]["txnRefNo"],"MERCHANT_NAME":indict["txnDetails"]["merchantName"],
                                                         "TRANSACTION_TYPE":indict["txnDetails"]["transactionType"],"MCC":indict["txnDetails"]["mcc"],"CHANNEL":indict["txnDetails"]["channel"],
                                                         "RETRIEVAL_REF_N0":indict["txnDetails"]["retrievalRefNo"],"TRACE_NO":indict["txnDetails"]["traceNo"],
                                                         "TXN_CURRENCY":indict["txnDetails"]["txnCurrency"],"NETWORK":indict["txnDetails"]["network"],"TRANSACTION_FEES":str(indict["txnDetails"]["transactionFees"])}
            inserted = db.Insert(db="mint_loan", table='mw_enkash_webhook', compulsory=False, date=False,debug=False,**dict1)
            auto_id= db.Query(db="mint_loan", primaryTable='mw_enkash_webhook', fields={"A": ["AUTO_ID"]}, orderBy="AUTO_ID desc",limit=1)
            if auto_id["data"]!=[]:
                auto_id=auto_id["data"][0]["AUTO_ID"]
            else:
                auto_id=None
            customer_id = db.runQuery(Query.from_(clprofile).select(clprofile.customer_id).where(clprofile.company_id==indict["companyId"]))
            if customer_id["data"]!=[]:
                customer_id =customer_id["data"][0]["customer_id"]
                q1 = Query.from_(loanmaster).join(loandetails, how=JoinType.inner).on(loanmaster.ID == loandetails.LOAN_MASTER_ID)
                q1 = q1.select(loanmaster.STATUS)
                loan_exist = db.runQuery(q1.where((loandetails.EXPECTED_MATURITY_DATE == self.last_day_of_month(datetime.today().date()))&(loandetails.CUSTOMER_ID==customer_id)))["data"]
                join = Query.from_(custcred).join(clientmaster, how=JoinType.left).on_field("CUSTOMER_ID").select(clientmaster.CLIENT_ID, clientmaster.FUND, clientmaster.FUND_CLIENT_ID)
                if loan_exist==[]:
                    q2=Query.from_(loanlimit).select(loanlimit.LOAN_LIMIT).where(loanlimit.CUSTOMER_ID==customer_id)
                    loanlimit=db.runQuery(q2)["data"]
                    #print(loanlimit)
                    if loanlimit!=[]:
                        loanlimit=loanlimit[0]["LOAN_LIMIT"]
                    else:
                        loanlimit=None
                    #print(loanlimit)
                    clientID = db.runQuery(join.where((custcred.CUSTOMER_ID == customer_id) & (clientmaster.LENDER == lender)))
                    #print(clientID)
                    if clientID["data"]:
                        clientID=clientID["data"][0]["CLIENT_ID"]
                    payload={"clientId":clientID,"productId":40,"disbursementData":[{"expectedDisbursementDate":date.today().strftime("%d %B %Y"),"principal":indict["principalAmount"]}],
                             "fundId":5,"principal":5*loanlimit,"loanTermFrequency":1,"loanTermFrequencyType":2,"numberOfRepayments":1,"repaymentEvery":1,
                             "repaymentFrequencyType":2,"interestRatePerPeriod":2,"amortizationType":0,"isEqualAmortization":False,"interestType":0,
                             "interestCalculationPeriodType":0,"allowPartialPeriodInterestCalcualtion":False,"transactionProcessingStrategyId":4,
                             "maxOutstandingLoanBalance":100000,"loanPurposeId":630,"charges":[{"chargeId":6,"amount":25.42},{"chargeId":11,"amount":4.58}],
                             "repaymentsStartingFromDate":self.last_day_of_month(datetime.today().date()).strftime("%d %B %Y"),"locale":"en","dateFormat":"dd MMMM yyyy","loanType":"individual",
                             "expectedDisbursementDate":date.today().strftime("%d %B %Y"),"submittedOnDate":date.today().strftime("%d %B %Y")}
                    #print(payload)
                    utils.logger.info("api request: " + json.dumps(payload), extra=logInfo)
                    r = requests.post(baseurl + "loans", data=json.dumps(payload), headers=headers,auth=auth, verify=False)
                    utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                    #print(r.json)
                    if 'resourceId' in r.json():
                        r2 = requests.get(baseurl + "loans/" + str(r.json()['resourceId']), headers=headers, auth=auth,data=json.dumps({}), verify=False)
                        resp2 = r2.json()
                        inserted = db.Insert(db='mint_loan', table='mw_client_loan_master',
                                             CUSTOMER_ID=str(customer_id), compulsory=False,LENDER=lender, STATUS="PENDING", DEBIT_TYPE='DIRECT_DEBIT',
                                             LOAN_REFERENCE_ID=(str(r.json()['resourceId'])),
                                             LOAN_ACCOUNT_NO=(str(r.json()['resourceId']) if lender == 'GETCLARITY' else None),
                                             AMOUNT=str(int(indict["principalAmount"])),
                                             LOAN_REQUEST_DATE=datetime.now().strftime("%Y-%m-%d"), CREATED_BY="Admin",
                                             CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), date=False)
                    indict2={"LOAN_CREATED":'1',"WEBHOOK_ACCESSED":"1"}
                    updated = db.Update(db="mint_loan", table="mw_enkash_webhook", checkAll=False,debug =False,
                                                             conditions={"AUTO_ID=": str(auto_id)},**indict2)
                else:
                    if loan_exist[0]["STATUS"]=='ACTIVE':
                        pass
                    '''
                        payload={"approvedLoanAmount":100000,"locale":"en","dateFormat":"dd MMMM yyyy",
                                 "disbursementData":[{"id":43,"principal":1000,"expectedDisbursementDate":"02 September 2020"},
                                                     {"id":46, "principal":1405,"expectedDisbursementDate":"03 September 2020"},
                                                     {"id":48, "principal":3205.5,"expectedDisbursementDate":"05 September 2020"}],
                                 "expectedDisbursementDate":"02 September 2020"}
                        #print(baseurl+ 'loans/'+str(customer_id)+'/disbursements/editDisbursements')
                        updated = db.Update(db="mint_loan", table="mw_enkash_webhook", checkAll=False,debug =False,
                                                             conditions={"AUTO_ID=": str(auto_id),"LOAN_CREATED":'1'})
                    '''
                    if loan_exist[0]["STATUS"]=='PENDING':
                        pass
                if updated:
                    token = generate(db).AuthToken(update=False)
                    output_dict["data"].update({"error": 0,"message":"successfull"})
                    output_dict["data"].update({"customerLoanRefID": str(r.json()['resourceId'])})
                    output_dict["msgHeader"].update({"authToken":token["token"]})
                else:
                    output_dict["data"].update({"error": 1, "message":errors["query"]})
            else:
                token = generate(db).AuthToken(update=False)
                output_dict["data"].update({"error": 0,"message":"customer does not exist"})
            resp.body = json.dumps(output_dict)
            db._DbClose_()
        except Exception as ex:
            raise
