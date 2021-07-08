from __future__ import absolute_import
import falcon
import json
import time
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
import six


class CustExtendedDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"custCredentials": {}, "name": "", "aadharDoc": "", "addressDoc": "", "incomeDoc": "", "carDoc": "", "lastStageChange": {},
                                                                "uberDoc": "", "chequeDoc": "", "incomeData": [], "custDetails": {}, "companyNumber": "",
                                                                "loanLimit": "", "loanLimitComments": "", "lastTwoLoans": {}, "callInfo": [], "reasons": [], "mandateData": [], "companyNumbers": []}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'customerDetailsCalling'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
            #utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
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
                val_error = validate(dbw).basicChecks(token=input_dict["msgHeader"]["authToken"], loginAuth=False)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    loginmap = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    invkyc = Table("pan_status_check", schema="gc_reliance")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    loanlimit = Table("mw_client_loan_limit",
                                      schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    loandetails = Table(
                        "mw_client_loan_details", schema="mint_loan")
                    uberdata = Table(
                        "mw_driver_income_data_new", schema="mint_loan")
                    doc = Table("mw_cust_kyc_documents", schema="mint_loan")
                    doctype = Table("mw_kyc_document_type", schema="mint_loan")
                    repayhist = Table(
                        "mw_client_loan_repayment_history_master", schema="mint_loan")
                    repaydata = Table(
                        "mw_loan_repayment_data", schema="mint_loan")
                    emi = Table("mw_client_loan_emi_details",
                                schema="mint_loan")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    reasons = Table(
                        "mw_call_interaction_reasons", schema="mint_loan")
                    resolutions = Table(
                        "mw_call_interaction_resolutions", schema="mint_loan")
                    custdata = Table("mw_customer_data", schema="mint_loan")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    avg = Table("mw_customer_derived_data", schema="mint_loan")
                    mandate = Table(
                        "mw_physical_mandate_status", schema="mint_loan")
                    q = Query.from_(avg).select("AVERAGE_3_WEEK", "AVERAGE_10_WEEK",
                                                "WEEK_FOR_LATEST_DATA").where(avg.CUSTOMER_ID == custID)
                    averages = db.runQuery(
                        q.orderby(avg.WEEK, order=Order.desc).limit(1))["data"]
                    name = db.runQuery(Query.from_(kyc).select(
                        "NAME").where((kyc.CUSTOMER_ID == custID)))["data"]
                    q = Query.from_(log).select("DATA_VALUE", "CREATED_DATE").where(
                        (log.DATA_KEY == "STAGE") & (log.CUSTOMER_ID == custID))
                    lastStageChange = db.runQuery(
                        q.orderby(log.AUTO_ID, order=Order.desc).limit(2))["data"]
                    lastStageChange = {"CREATED_DATE": lastStageChange[0]["CREATED_DATE"], "DATA_VALUE": lastStageChange[1]["DATA_VALUE"]} if len(
                        lastStageChange) > 1 else lastStageChange[0] if lastStageChange else {}
                    q = Query.from_(custdata).select(custdata.DATA_VALUE.as_(
                        "companyNumber")).where(custdata.CUSTOMER_ID == custID)
                    compNo = db.runQuery(q.where(custdata.DATA_KEY.isin(
                        ["COMPANY_NUMBER", "ALTERNATE_MOB_NO", "ALTERNATIVE_NUMBER"])).orderby(custdata.ID, order=Order.desc).limit(1))
                    Resolutions = db.runQuery(Query.from_(resolutions).select(
                        "AUTO_ID", "INTERACTION_RESOLUTION"))["data"]
                    Resolutions = {
                        ele["AUTO_ID"]: ele["INTERACTION_RESOLUTION"] for ele in Resolutions}
                    Reasons = db.runQuery(Query.from_(reasons).select(
                        "AUTO_ID", "INTERACTION_REASON"))["data"]
                    Reasons = {ele["AUTO_ID"]: ele["INTERACTION_REASON"]
                               for ele in Reasons}
                    callInfo = db.runQuery(Query.from_(calldata).select(calldata.star).where(
                        calldata.CUSTOMER_ID == custID).orderby(calldata.AUTO_ID, order=Order.desc))["data"]
                    callInfo = [{"CREATED_BY": ele["CREATED_BY"], "INTERACTION_REASON":Reasons[ele["INTERACTION_REASON_ID"]],
                                 "COMMENTS":ele["COMMENTS"], "INTERACTION_RESOLUTION":Resolutions[ele["INTERACTION_RESOLUTION_ID"]],
                                 "CREATED_DATE": ele["CREATED_DATE"], "CALLBACK_DATETIME":ele["CALLBACK_DATETIME"], "FOLLOW_UP_BY":ele["FOLLOW_UP_BY"]} for ele in callInfo if (ele["INTERACTION_REASON_ID"] != 0) and (ele["INTERACTION_RESOLUTION_ID"] != 0)]
                    Reasons = [{"REASON_ID": ele, "REASON": Reasons[ele]}
                               for ele in Reasons]
                    custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "CUSTOMER_ID", "STAGE", "COMMENTS", "DOCUMENT_COMMENTS",
                                                                               "REJECTED", "REJECTION_REASON").where(custcred.CUSTOMER_ID == custID))
                    latestLogin = db.runQuery(Query.from_(loginmap).select("LOGIN_ID").where(loginmap.CUSTOMER_ID == custID).where(
                        loginmap.ACTIVE == '1').orderby(loginmap.CREATED_DATE, order=Order.desc).limit(1))
                    q = Query.from_(profile).select(
                        "VERIFIED_NUMBER", "COMPANY_NAME", "EMAIL_ID", "NUMBER_COMMENT", "VERIFIED_NAME")
                    q = q.select("NUMBER_VERIFIED", "NAME_VERIFIED", "NAME",
                                 "NAME_COMMENT", "COMPANY_NUMBER", "CURRENT_CITY")
                    custDetails = db.runQuery(
                        q.where(profile.CUSTOMER_ID == custID))
                    custPan = Query.from_(pan).select("PAN_NO").where(
                        pan.CUSTOMER_ID == custID).orderby(pan.CREATED_DATE, order=Order.desc)
                    custPan = db.runQuery(custPan.limit(1))
                    invkycDone = db.runQuery(Query.from_(invkyc).select(invkyc.KYC_FLAG).where(invkyc.PAN == (
                        custPan["data"][0]["PAN_NO"] if custPan["data"] else '')).orderby(invkyc.ID, order=Order.desc).limit(1))["data"]
                    Doc = db.runQuery(Query.from_(doc).select(
                        doc.DOCUMENT_TYPE_ID, doc.VERIFICATION_STATUS).where(doc.CUSTOMER_ID == custID))["data"]
                    aadharDoc = (set([ele["VERIFICATION_STATUS"]
                                      for ele in Doc if ele["DOCUMENT_TYPE_ID"] in [108, 116]]))
                    if 116 not in [ele["DOCUMENT_TYPE_ID"] for ele in Doc]:
                        addressDoc = aadharDoc
                    else:
                        addressDoc = (set(
                            [ele["VERIFICATION_STATUS"] for ele in Doc if ele["DOCUMENT_TYPE_ID"] == 101]))
                    incomeDoc = (set([ele["VERIFICATION_STATUS"]
                                      for ele in Doc if ele["DOCUMENT_TYPE_ID"] == 110]))
                    chequeDoc = (set([ele["VERIFICATION_STATUS"]
                                      for ele in Doc if ele["DOCUMENT_TYPE_ID"] in [102, 106]]))
                    carDoc = (set([ele["VERIFICATION_STATUS"]
                                   for ele in Doc if ele["DOCUMENT_TYPE_ID"] == 111]))
                    uberDoc = (set([ele["VERIFICATION_STATUS"]
                                    for ele in Doc if ele["DOCUMENT_TYPE_ID"] == 113]))
                    loanLimit = db.runQuery(Query.from_(loanlimit).select("LOAN_LIMIT", "COMMENTS").where((loanlimit.CUSTOMER_ID == custID) &
                                                                                                          (loanlimit.ARCHIVED == 'N')))
                    join = Query.from_(loanmaster).join(loandetails, how=JoinType.left).on(
                        loanmaster.ID == loandetails.LOAN_MASTER_ID)
                    q = join.select(loanmaster.LOAN_REFERENCE_ID, loanmaster.STATUS,
                                    loandetails.PRINCIPAL, loandetails.TOTAL_OUTSTANDING)
                    q = q.select(loandetails.EXPECTED_MATURITY_DATE,
                                 loandetails.EXPECTED_DISBURSEMENT_DATE, loanmaster.LOAN_REQUEST_DATE)
                    q = q.select(loanmaster.AMOUNT, loanmaster.LOAN_ACCOUNT_NO)
                    q = q.where((loanmaster.STATUS != 'ML_REJECTED')
                                & (loanmaster.CUSTOMER_ID == custID))
                    ldetails = db.runQuery(
                        q.orderby(loanmaster.ID, order=Order.desc).limit(1))
                    for datum in ldetails["data"]:
                        if datum["LOAN_ACCOUNT_NO"]:
                            q = Query.from_(repayhist).select(
                                "TRANSACTION_MEDIUM", "AMOUNT", "TRANSACTION_STATUS", "TRANSACTION_DATE", )
                            q = q.where(repayhist.LOAN_ID ==
                                        datum["LOAN_ACCOUNT_NO"])
                            datum.update({"repayments": db.runQuery(
                                q.orderby(repayhist.ID, order=Order.desc).limit(5))["data"]})
                            q = Query.from_(repaydata).select(
                                "REPAY_INFO", "REPAY_AMOUNT", "REPAY_DATETIME", "MODE_OF_PAYMENT")
                            q = q.where(repaydata.LOAN_REF_ID ==
                                        datum["LOAN_REFERENCE_ID"])
                            datum.update({"repayinfo": db.runQuery(
                                q.orderby(repaydata.AUTO_ID, order=Order.desc).limit(5))["data"]})
                            q = Query.from_(emi).select(
                                "PERIOD", "TOTAL_DUE_FOR_PERIOD", "TOTAL_PAID_FOR_PERIOD", "OVERDUE_AMOUNT", "DUE_DATE")
                            datum.update({"emidata": db.runQuery(q.where(
                                emi.LOAN_ACCOUNT_NO == ldetails["data"][0]["LOAN_ACCOUNT_NO"]))["data"]})
                        else:
                            datum.update({"repayments": [], "repayinfo": [], "emidata": []})
                    income = Query.from_(uberdata).select(
                        "FIRST_TRIP_WEEK", "DRIVER_UUID", "CONTACT_NUMBER", "FIRST_NAME", "LAST_NAME")
                    income = db.runQuery(income.where(uberdata.CUSTOMER_ID == custID).groupby(uberdata.CONTACT_NUMBER))
                    mandateData = db.runQuery(Query.from_(mandate).select(mandate.star).where(mandate.CUSTOMER_ID == custID))
                    if (custCredentials["data"]):
                        #print("True")
                        token = generate(dbw).AuthToken() #if input_dict["msgHeader"]["authToken"].replace(" ", "+") != db._dbQuery_(Field='LOGIN_AUTH_TOKEN') else {k: (v if v else True) for k, v in six.iteritems(generate(dbw).AuthToken(update=False))}
                        if token["updated"]:
                            output_dict["data"]["loginID"] = latestLogin["data"][0]["LOGIN_ID"] if latestLogin[
                                "data"] else custCredentials["data"][0]["LOGIN_ID"] if custCredentials["data"] else ""
                            output_dict["data"]["mandateData"] = utils.camelCase(
                                mandateData["data"])
                            output_dict["data"]["averages"] = utils.camelCase(averages[0] if averages else {
                                                                              "AVERAGE_3_WEEK": 0, "AVERAGE_10_WEEK": 0, "WEEK_FOR_LATEST_DATA": ""})
                            output_dict["data"]["name"] = name[0]["NAME"] if (
                                name[0]["NAME"] if name else False) else ""
                            output_dict["data"]["lastStageChange"] = utils.camelCase(
                                lastStageChange) if lastStageChange else {}
                            output_dict["data"]["companyNumber"] = str(
                                compNo["data"][0]["companyNumber"]) if compNo["data"] else ""
                            output_dict["data"]["companyNumbers"] = [
                                str(ele["companyNumber"]) for ele in compNo["data"]]
                            output_dict["data"]["incomeData"] = utils.camelCase(
                                income["data"])
                            output_dict["data"]["callInfo"] = utils.camelCase(
                                callInfo)
                            output_dict["data"]["reasons"] = utils.camelCase(
                                Reasons)
                            output_dict["data"]["custCredentials"] = utils.camelCase(
                                custCredentials["data"][0])
                            output_dict["data"]["custPanNumber"] = custPan["data"][0]["PAN_NO"] if custPan["data"] else ""
                            output_dict["data"]["invkycDone"] = invkycDone[0]["KYC_FLAG"] if invkycDone else 'N'
                            output_dict["data"]["custDetails"] = utils.camelCase(custDetails["data"][0]) if custDetails["data"] else {}
                            output_dict["data"]["aadharDoc"] = "1" if "Y" in aadharDoc else "P" if None in aadharDoc else "0"
                            output_dict["data"]["addressDoc"] = "1" if "Y" in addressDoc else "P" if None in addressDoc else "0"
                            output_dict["data"]["incomeDoc"] = "1" if "Y" in incomeDoc else "P" if None in incomeDoc else "0"
                            output_dict["data"]["chequeDoc"] = "1" if "Y" in chequeDoc else "P" if None in chequeDoc else "0"
                            output_dict["data"]["carDoc"] = "1" if "Y" in carDoc else "P" if None in carDoc else "0"
                            output_dict["data"]["uberDoc"] = "1" if "Y" in uberDoc else "P" if None in uberDoc else "0"
                            output_dict["data"]["lastTwoLoans"] = [{k: (v if v is not None else 0) for k, v in six.iteritems(ele)} for ele in utils.camelCase(
                                ldetails["data"], modifyNullValues=False)]  # utils.camelCase(ldetails["data"], modifyNullValues=False)
                            if loanLimit["data"]:
                                output_dict["data"]["loanLimit"] = loanLimit["data"][0][
                                    "LOAN_LIMIT"] if loanLimit["data"][0]["LOAN_LIMIT"] else "0"
                                output_dict["data"]["loanLimitComments"] = (loanLimit["data"][0]["COMMENTS"] if loanLimit["data"][0]["COMMENTS"]
                                                                            else "")
                            else:
                                output_dict["data"]["loanLimit"], output_dict["data"]["loanLimitComments"] = (
                                    -1, "")
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            pass
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)
                # print output_dict#utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
                dbw._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
