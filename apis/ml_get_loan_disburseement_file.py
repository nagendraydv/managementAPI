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
import requests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions, JoinType, Order
from dateutil.relativedelta import relativedelta
from pypika import functions as fn

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class getLoanDisbursementFileResource:
    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"data": {},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "data found successfull"
        logInfo = {'api': 'getDisbursementFile'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise
        try:
            if not validate.Request(api='disbursementFile', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"], checkLogin=False)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    #custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    #custmap = Table("mw_customer_login_credentials_map", schema="mint_loan")
                    #clientmaster = Table("mw_finflux_client_master", schema="mint_loan")
                    #profile = Table("mw_client_profile", schema="mint_loan")
                    #aadhar = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    #conf2 = Table("mw_configuration", schema="mint_loan")
                    #cvalues = Table("mw_finflux_attribute_code_values", schema="mint_loan")
                    #panT = Table("mw_pan_status", schema="mint_loan")
                    #emip = Table("mw_finflux_emi_packs_master",schema="mint_loan")
                    #custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    loanmaster = Table("mw_client_loan_master", schema="mint_loan")
                    #loandetails = Table("mw_client_loan_details", schema="mint_loan")
                    today = datetime.now().strftime("%Y-%m-%d")
                    lender = "GETCLARITY"
                    urlKey = ("MIFOS_URL")
                    today = datetime.now().strftime("%d %B %Y")
                    baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    headers = (utils.finflux_headers[lender].copy() if lender in utils.finflux_headers else {})
                    auth = utils.mifos_auth
                    custID = input_dict["data"]["customerID"]
                    data = []
                    for j in range(len(custID)):
                        custId = custID[j]
                        loanID = db.runQuery(Query.from_(loanmaster).select(loanmaster.LOAN_REFERENCE_ID).where((loanmaster.CUSTOMER_ID == custId) &
                                                                                                  (loanmaster.STATUS == "PENDING") &
                                                                                                  (loanmaster.LENDER == "GETCLARITY")))
                        if loanID["data"]!=[]:
                            for i in range(len(loanID["data"])):
                                loanID = loanID["data"][i]["LOAN_REFERENCE_ID"]
                                utils.logger.debug("loan creation url %s hit" % (baseurl + "loans"), extra=logInfo)
                                utils.logger.debug("with following payload - " + json.dumps(loanID), extra=logInfo)
                                #print(baseurl)
                                url = baseurl + "loans/"+ str(loanID)+"/"+"?associations=all"
                                #print(url)
                                r = requests.get(url, headers=headers, auth=auth, verify=False)
                                utils.logger.debug("api response: " + json.dumps(r.json()), extra=logInfo)
                                res = r.json()#?a?associations=all?associations=all?associations=all?associations=allssociations=all
                                #res={}
                                matDate= res["timeline"]["expectedMaturityDate"]
                                disbDate = res["timeline"]["expectedDisbursementDate"]
                                outDict = {"loanID":res["id"],"amount":res["principal"],"interestRate":res["interestRatePerPeriod"],"numberOfRepayments":res["numberOfRepayments"],"repaymentEvery":res["repaymentEvery"],
                                           "expectedMaturityDate":str(matDate[0])+"/"+str(matDate[1])+"/"+str(matDate[2]),"expectedDisbursementDate":str(disbDate[0])+"/"+str(disbDate[1])+"/"+str(disbDate[2]),"termFrequency":res["termFrequency"],
                                           "termFrequencyEnum":res["repaymentFrequencyType"]["value"],"customerID":custId}    
                                #outDict = {}
                        data.append(outDict)
                    if data!=[]:
                        token = generate(db).AuthToken()
                        if "token" in list(token.keys()):
                            output_dict["data"].update({"error": 0, "message": success,"fileData":data})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"],"fileData":data})
                    else:
                        output_dict["data"].update({"error": 1, "message": "something went wrong","fileData":[]})
                    db._DbClose_()
                    resp.body = json.dumps(output_dict)
                    utils.logger.debug(
                        "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
            raise
