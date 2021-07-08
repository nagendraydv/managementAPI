from __future__ import absolute_import
import falcon
import json
#import inspect
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
import six


class CustDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"custCredentials": {}, "custPanNumber": "", "custAadharNumber": "", "custDetails": {},
                                                                "custKycDetails": {}, "loanLimit": "", "loanLimitComments": "", "clientDetails": [], "companyNumbers": []}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'customerDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
            #utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
            #gen.DBlog(logFrom="customerDetails", lineNo=inspect.currentframe().f_lineno, logMessage="Request: " + json.dumps(input_dict))
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='custDetails', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # , filename='mysql-slave.config') # setting an instance of DB class
                db = DB(input_dict["msgHeader"]["authLoginID"])
                dbw = DB(input_dict["msgHeader"]["authLoginID"])
                #gen = generate(dbw)
                #gen.DBlog(logFrom="customerDetails", lineNo=inspect.currentframe().f_lineno, logMessage="Request: " + json.dumps(input_dict))
                val_error = validate(dbw).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    invkyc = Table("pan_status_check", schema="gc_reliance")
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    custcredm = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    custkyc = Table("mw_aadhar_kyc_details",
                                    schema="mint_loan")
                    aadhar = Table("mw_aadhar_status", schema="mint_loan")
                    aadharKyc=Table("mw_aadhar",schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    sprofile = Table(
                        "mw_swiggy_loans_profile_data", schema="mint_loan")
                    compProf = Table("mw_profile_info", schema="mw_company_3")
                    compProf2 = Table(
                        "mw_unregistered_data_dump", schema="mw_company_3")
                    loanlimit = Table("mw_client_loan_limit",
                                      schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    income2 = Table("mw_driver_income_data",
                                    schema="mint_loan")
                    doc = Table("mw_cust_kyc_documents", schema="mint_loan")
                    custdata = Table("mw_customer_data", schema="mint_loan")
                    q = Query.from_(custdata).select(custdata.DATA_VALUE.as_(
                        "companyNumber")).where(custdata.CUSTOMER_ID == custID)
                    compNo = db.runQuery(q.where(custdata.DATA_KEY == "COMPANY_NUMBER").orderby(
                        custdata.ID, order=Order.desc))
                    clientIDs = db.runQuery(Query.from_(clientmaster).select(
                        clientmaster.star).where(clientmaster.CUSTOMER_ID == custID))
                    clientDetails = []
                    for lender in ["CHAITANYA", "GETCLARITY"]:
                        temp = [x for x in clientIDs["data"]
                                if x["LENDER"] == lender]
                        clientDetails.append(
                            {"LENDER_NAME": lender, "CLIENT_ID": temp[0]["CLIENT_ID"] if temp else ""})
                   # clientDetails = db.runQuery(Query.from_(clientmaster).select(clientmaster.star).where(clientmaster.CUSTOMER_ID==custID))
                    custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "LAST_LOGIN", "CUSTOMER_ID", "STAGE",
                                                                               "REGISTERED_IP_ADDRESS", "LAST_LOGGED_IN_IP_ADDRESS", "COMMENTS",
                                                                               "DEVICE_ID", "CREATED_DATE", "REJECTED",
                                                                               "REJECTION_REASON").where(custcred.CUSTOMER_ID == custID))
                    if (custCredentials["data"]):
                        logins = db.runQuery(Query.from_(custcredm).select(
                            "LOGIN_ID", "ACTIVE").where(custcredm.CUSTOMER_ID == custID))["data"]
                        custCredentials["data"][0].update(
                            {"LOGIN_IDS": logins})
                    custDetails = db.runQuery(Query.from_(profile).select(
                        profile.star).where(profile.CUSTOMER_ID == custID))
                    custKycDetails = db.runQuery(Query.from_(custkyc).select(
                        custkyc.star).where(custkyc.CUSTOMER_ID == custID))
                    custKycDocs = db.runQuery(Query.from_(doc).select(doc.star).where(
                        (doc.CUSTOMER_ID == custID) & (doc.DOCUMENT_TYPE_ID.isin(['108', '101', '116', '100']))))
                    kycDone = True if clientIDs["data"] != [] else (custKycDetails["data"][0]["DOB"] != '' if custKycDetails["data"][0]["DOB"] else False) & (custKycDetails["data"][0]["STATE"] != '' if custKycDetails["data"][0]["STATE"] else False) & (custKycDetails["data"][0]["NAME"] != '' if custKycDetails["data"][0]["NAME"] else False) & (
                        custKycDetails["data"][0]["GENDER"] != '' if custKycDetails["data"][0]["GENDER"] else False) & ((" ".join((val if val else '') for key, val in six.iteritems(custKycDetails["data"][0]) if key in ("VTC", "DISTRICT", "PIN_CODE", "CO", "HOUSE", "LM", "LC"))) not in ('', 'same as aadhar')) if custKycDetails["data"] else False
                    for datum in custKycDocs["data"]:
                        datum["DOCUMENT_URL"] = (datum["DOCUMENT_URL"].split(
                            "/")[-1] if datum["DOCUMENT_TYPE_ID"] not in (129, "129") else datum["DOCUMENT_URL"])
                    custAadharNo = Query.from_(aadhar).select("AADHAR_NO").where(
                        (aadhar.CUSTOMER_ID == custID) & (aadhar.ARCHIVED == 'N'))
                    custAadharNo = db.runQuery(custAadharNo.orderby(
                        aadhar.CREATED_DATE, order=Order.desc).limit(1))
                    custPan = Query.from_(pan).select("PAN_NO").where(
                        pan.CUSTOMER_ID == custID).orderby(pan.CREATED_DATE, order=Order.desc)
                    custPan = db.runQuery(custPan.limit(1))
                    invkycDone = db.runQuery(Query.from_(invkyc).select(invkyc.KYC_FLAG).where(invkyc.PAN == (
                        custPan["data"][0]["PAN_NO"] if custPan["data"] else '')).orderby(invkyc.ID, order=Order.desc).limit(1))["data"]
                    loanLimit = db.runQuery(Query.from_(loanlimit).select("LOAN_LIMIT", "EDUCATION_LOAN_LIMIT", "MOBILE_LOAN_LIMIT", "INSURANCE_LOAN_LIMIT", "INSURANCE_UPFRONT_LIMIT", "TYRE_LOAN_LIMIT", "COMMENTS").where((loanlimit.CUSTOMER_ID == custID) &
                                                                                                                                                                                                                             (loanlimit.ARCHIVED == 'N')))
                    profData = Query.from_(compProf).select(
                        compProf.star).where(compProf.CUSTOMER_ID == custID)
                    kycID = db.runQuery(Query.from_(aadharKyc).select(
                        aadharKyc.KYC_ID).where(aadharKyc.CUSTOMER_ID == custID))
                    profData = db.runQuery(profData.orderby(
                        compProf.AUTO_ID, order=Order.desc).limit(1))
                    sql_select="Select CREATED_DATE,VERIFIED,CAST(AES_DECRYPT(AADHAR_NO,'eHjboBoUWim4W9jQPhvSNxqskaYlEFjr') as CHAR(50) ) as aadhar_no from `mint_loan`.`mw_aadhar` where customer_id=%s and archived='N'"%(custID)
                    junk = db.dictcursor.execute(sql_select)
                    aadhar_no = list(db.dictcursor.fetchall())
                    #print(aadhar_no)
                    if not profData["data"]:
                        profData = Query.from_(compProf2).select(
                            compProf2.star).where(compProf2.CUSTOMER_ID == custID)
                        profData = db.runQuery(profData.orderby(
                            compProf2.AUTO_ID, order=Order.desc).limit(1))
                        junk = [ele.update(
                            {"CONFIRMED_CUSTOMER_ID": ele["CUSTOMER_ID"]}) for ele in profData["data"]]
                    # income = db.runQuery(Query.from_(sprofile).select(sprofile.star).where(sprofile.CUSTOMER_ID==custID)) if
                    income = db.runQuery(Query.from_(income).select(income.star).where(income.CUSTOMER_ID == custID).orderby(income.WEEK, order=Order.desc).limit(1)) if (
                        custDetails["data"][0]["COMPANY_NAME"] != "SWIGGY" if custDetails["data"] else True) else db.runQuery(Query.from_(sprofile).select(sprofile.star).where(sprofile.CUSTOMER_ID == custID))
                    if not income["data"]:
                        income = db.runQuery(Query.from_(income2).select(income2.star).where(
                            income2.CUSTOMER_ID == custID).orderby(income2.WEEK, order=Order.desc).limit(1))
                    if income["data"]:
                        income["data"][0].update({"profData": ([{k: (str(v) if v is not None else '') for k, v in six.iteritems(
                            profData["data"][0])}] if profData["data"] else [])})
                    else:
                        income["data"] = [{"profData": ([{k: (str(v) if v is not None else '') for k, v in six.iteritems(
                            profData["data"][0])}] if profData["data"] else [])}]
                    if (custCredentials["data"]):
                        token = generate(dbw).AuthToken()
                        if token["updated"]:
                            output_dict["data"]["companyNumbers"] = [
                                ele["companyNumber"] for ele in compNo["data"]]
                            output_dict["data"]["incomeData"] = utils.camelCase([{k: (v if type(v) == str else v) for k, v in six.iteritems(income["data"][0])}]) if income["data"] else []
                            output_dict["data"]["clientDetails"] = utils.camelCase(
                                clientDetails)
                            output_dict["data"]["invkycDone"] = invkycDone[0]["KYC_FLAG"] if invkycDone else 'N'
                            output_dict["data"]["kycDone"] = kycDone
                            output_dict["data"]["custCredentials"] = utils.camelCase(
                                custCredentials["data"][0])
                            output_dict["data"]["custDetails"] = utils.camelCase(
                                custDetails["data"][0]) if custDetails["data"] else []
                            output_dict["data"]["custKycDetails"] = utils.camelCase(
                                custKycDetails["data"][0]) if custKycDetails["data"] else []
                            output_dict["data"]["custKycDocs"] = utils.camelCase(
                                custKycDocs["data"])
                            #for i in range(len(aadhar_no)):
                                #if aadhar_no[i]["VERIFIED"]==1:        
                            output_dict["data"]["custAadharNumber"] = aadhar_no if aadhar_no!=[] else []#custAadharNo["data"][0]["AADHAR_NO"] if custAadharNo["data"] else ""                                    
                                    #output_dict["data"]["verified"] = 1
                                    #break
                                #else:#if aadhar_no[i]["VERIFIED"]!=1:
                                    #output_dict["data"]["custAadharNumber"] = aadhar_no[-1]["aadhar_no"] if aadhar_no!=[] else ''#custAadharNo["data"][0]["AADHAR_NO"] if custAadharNo["data"] else ""                                    
                                    #output_dict["data"]["verified"] = 0
                            output_dict["data"]["kycID"] = kycID["data"][0]["KYC_ID"] if kycID["data"] else ""
                            #print(kycID)
                            output_dict["data"]["custPanNumber"] = custPan["data"][0]["PAN_NO"] if custPan["data"] else ""
                            if loanLimit["data"]:
                                output_dict["data"]["loanLimit"] = loanLimit["data"][0][
                                    "LOAN_LIMIT"] if loanLimit["data"][0]["LOAN_LIMIT"] else "0"
                                output_dict["data"]["mobileLoanLimit"] = loanLimit["data"][0][
                                    "MOBILE_LOAN_LIMIT"] if loanLimit["data"][0]["MOBILE_LOAN_LIMIT"] else "0"
                                output_dict["data"]["tyreLoanLimit"] = loanLimit["data"][0][
                                    "TYRE_LOAN_LIMIT"] if loanLimit["data"][0]["TYRE_LOAN_LIMIT"] else "0"
                                output_dict["data"]["insuranceLoanLimit"] = loanLimit["data"][0][
                                    "INSURANCE_LOAN_LIMIT"] if loanLimit["data"][0]["INSURANCE_LOAN_LIMIT"] else "0"
                                output_dict["data"]["educationLoanLimit"] = loanLimit["data"][0][
                                    "EDUCATION_LOAN_LIMIT"] if loanLimit["data"][0]["EDUCATION_LOAN_LIMIT"] else "0"
                                output_dict["data"]["loanLimitComments"] = (loanLimit["data"][0]["COMMENTS"] if loanLimit["data"][0]["COMMENTS"]
                                                                            else "")
                            else:
                                output_dict["data"]["loanLimit"], output_dict["data"]["loanLimitComments"] = (
                                    "", "")
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                try:
                    resp.body = json.dumps(
                        output_dict, encoding='unicode-escape')
                except:
                    resp.body = json.dumps(output_dict)
                # print output_dict["msgHeader"]
                #utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                #gen.DBlog(logFrom="customerDetails", lineNo=inspect.currentframe().f_lineno, logMessage="Response: " + json.dumps(output_dict))
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
