from __future__ import absolute_import
import falcon
import json
import requests
import string
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, JoinType, functions, Order
import six
from six.moves import range


class ClientUpdateResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"updated": ""}}
        errors = utils.errors
        success = "Successfully updated"
        logInfo = {'api': 'investorUpdate'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            utils.logger.error("ExecutionError: ",extra=logInfo, exc_info=True)
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='clientUpdate', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
                utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                usertype = Table("mw_admin_user_account_type",schema="mint_loan_admin")
                prof = Table("mw_client_profile", schema="mint_loan")
                compProf = Table("mw_profile_info", schema="mw_company_3")
                uauth = Table("mw_authorization_dump", schema="mw_company_3")
                sess = Table("mw_company_login_session", schema="mint_loan")
                cdata = Table("mw_customer_data", schema="mint_loan")
                cred = Table("mw_customer_login_credentials",schema="mint_loan")
                inc = Table("mw_derived_income_data", schema="mw_company_3")
                q = db.runQuery(Query.from_(usertype).select("ACCOUNT_TYPE").where(usertype.LOGIN == input_dict["msgHeader"]["authLoginID"]))
                accTypes = [ele["ACCOUNT_TYPE"] for ele in q["data"]]
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                    utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                elif (all(x not in [ele["updateType"] for ele in input_dict["data"]["update"]] for x in ['customerCredentials', 'uberApi']) and
                      (('CALLING' in accTypes) or (accTypes == []) or ('OUTCALL' in accTypes) or ('FRONTDESK' in accTypes) or ('EXTNBFC' in accTypes))):  # or ('INVESTMENT' in accTypes))):
                    output_dict["data"].update({"error": 1, "message": "data updation not allowed for your usertype"})
                    resp.body = json.dumps(output_dict)
                    utils.logger.error("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                else:
                    custID = input_dict["data"]["customerID"]
                    updated = {"clientDocument": [], "customerCredentials": "", "clientBank": "", "clientLoanLimit": "", "clientUberName": "",
                               "clientUberNumber": "", "clientEmailID": "", "log": "", "uberData": "", "uberApi": ""}
                    boolmap = {True: 1, False: 0}
                    clientmaster = Table("mw_finflux_client_master", schema="mint_loan")
                    loanmaster = Table("mw_client_loan_master", schema="mint_loan")
                    cbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY == "FINFLUX_URL"))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    for element in input_dict["data"]["update"]:
                        if element["updateType"] == "clientDocument":
                            utils.logger.info("Updating document verification status", extra=logInfo)
                            dstatus = db.runQuery(Query.from_(cbank).select(cbank.DOCUMENT_STATUS).where((cbank.CUSTOMER_ID == custID) & (cbank.ACCOUNT_NO == str(element["accountNo"]))))["data"] if (element["accountNo"] if "accountNo" in element else False) else None
                            updated["clientDocument"].append({"docSeqID": element["docSeqID"],
                                                              "success":
                                                              (boolmap[db.Update(db="mint_loan", table="mw_cust_kyc_documents",
                                                                                 DOCUMENT_TYPE_ID=(element["docTypeID"] if "docTypeID" in element
                                                                                                   else None),
                                                                                 DOCUMENT_COMMENTS=(element["comments"]
                                                                                                    if "comments" in element else None),
                                                                                 VERIFICATION_STATUS=(element["verificationStatus"]
                                                                                                      if "verificationStatus" in element else None),
                                                                                 VERIFIED_BY=(input_dict["msgHeader"]["authLoginID"]
                                                                                              if "verificationStatus" in element else None),
                                                                                 VERIFIED_ON=((datetime.utcnow()
                                                                                               + timedelta(seconds=19800)).strftime("%Y-%m-%d")
                                                                                              if "verificationStatus" in element else None),
                                                                                 conditions={"CUSTOMER_ID = ": custID,
                                                                                             "DOC_SEQ_ID = ": str(element["docSeqID"])})]
                                                               if ((element["verificationStatus"] if "verificationStatus" in element else False)
                                                                   or (element["comments"] if "comments" in element else False) or (element["docTypeID"] if "docTypeID" in element else False)) else 1),
                                                              "successBank":
                                                              (boolmap[db.Update(db="mint_loan", table="mw_cust_bank_detail",
                                                                                 DOC_SEQ_ID=element["docSeqID"], DOCUMENT_STATUS=(
                                                                                     ((element["verificationStatus"] if element["verificationStatus"] else None) if "verificationStatus" in element else None) if dstatus[0]["DOCUMENT_STATUS"] != 'V' else None),
                                                                                 DEFAULT_STATUS_FLAG=(
                                                                                     (("1" if element["verificationStatus"] else "0") if "verificationStatus" in element else None) if dstatus[0]["DOCUMENT_STATUS"] != 'V' else None),
                                                                                 ADDITIONALLY_ADDED=(
                                                                                     (("0" if element["verificationStatus"] else "0") if "verificationStatus" in element else None) if dstatus[0]["DOCUMENT_STATUS"] != 'V' else None),
                                                                                 conditions={"CUSTOMER_ID = ": custID, "ADDITIONALLY_ADDED=": "1",
                                                                                             "ACCOUNT_NO = ": str(element["accountNo"])})]
                                                               if (element["accountNo"] if "accountNo" in element else False) else 0)
                                                              })
                        elif element["updateType"] == "custReject":
                            utils.logger.info("Rejecting a customer", extra=logInfo)
                            updated["customerCredentials"] = boolmap[db.Update(db="mint_loan", table="mw_customer_login_credentials",
                                                                               REJECTED="1", REJECTION_REASON=element["rejectedReason"],
                                                                               STAGE="REJECTED", conditions={"CUSTOMER_ID = ": str(custID)})]
                            clientID = db.runQuery(Query.from_(clientmaster).select("CLIENT_ID").where(clientmaster.CUSTOMER_ID == str(custID)))
                            clientID = clientID["data"][0]["CLIENT_ID"] if clientID["data"] else None
                          #  if clientID:
                           #     if "undoReject" in list(element.keys()):
                            #        if element["undoReject"] == 0:
                            #           payload = {"rejectionReasonId": 910, "rejectionDate": datetime.now().strftime("%d %B %Y"), "locale": "en","dateFormat": "dd MMMM yyyy"}
                            #            print(utils.finflux_headers)
                            #            r = requests.post(baseurl + "clients/" + clientID + "?command=reject", data=json.dumps(payload),auth=utils.finflux_auth, headers=utils.finflux_headers, verify=False)
                            #            junk = db.Update(db="mint_loan", table="mw_client_loan_master", STATUS="ML_REJECTED",conditions={"CUSTOMER_ID=": str(custID), "STATUS=": "REQUESTED"})
                            #        else:
                            #           payload = {"reopenedDate": datetime.now().strftime("%d %B %Y"), "locale": "en", "dateFormat": "dd MMMM yyyy"}
                            #            r = requests.post(baseurl + "clients/" + clientID + "?command=undoRejection", data=json.dumps(payload),auth=utils.finflux_auth, headers=utils.finflux_headers, verify=False)
                        elif element["updateType"] == "lowIncomeStage":
                            utils.logger.info("Retaining low income stage for the customer", extra=logInfo)
                            updated["log"] = boolmap[db.Update(db="mint_loan", table="mw_customer_change_log",
                                                               RETAINED_BY=input_dict["msgHeader"]["authLoginID"],
                                                               RETAINED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                               conditions={"DATA_VALUE=": "LOW_INCOME", "CUSTOMER_ID =": str(custID)})]
                        elif element["updateType"] == "customerCredentials":
                            utils.logger.info(
                                "Updating account status", extra=logInfo)
                            allowedUsers = [
                                "shiv@mintwalk.com", "nikhil@mintwalk.com", "uber-kurla", "dharam@mintwalk.com"]
                            cheques = (element["cheques"] if (("cheques" in element) & (input_dict["msgHeader"]["authLoginID"] in allowedUsers))
                                       else None)
                            lacc = Query.from_(loanmaster).select(loanmaster.LOAN_REFERENCE_ID, loanmaster.AMOUNT).where((loanmaster.STATUS.isin(['PENDING', 'WAITING_FOR_DISBURSA', 'WAITING_FOR_DISBURSAL'])) &
                                                                                                                         (loanmaster.CUSTOMER_ID == custID))
                            lacc = db.runQuery(lacc)["data"]
                            if (lacc != []) or (element["stage"] != "LOAN_APPROVED" if "stage" in element else True):
                                updated["customerCredentials"] = boolmap[db.Update(db="mint_loan", table="mw_customer_login_credentials",
                                                                                   ACCOUNT_STATUS=(element["accountStatus"]
                                                                                                   if "accountStatus" in element else None),
                                                                                   FAIL_ATTEMPT=0, CHEQUES=cheques,
                                                                                   DOCUMENT_COMMENTS=(element["docComments"]
                                                                                                      if "docComments" in element else None),
                                                                                   COMMENTS=(
                                                                                       element["comments"] if "comments" in element else None),
                                                                                   STAGE=(
                                                                                       element["stage"] if "stage" in element else None),
                                                                                   conditions={"CUSTOMER_ID = ": custID})]
                            else:
                                updated["customerCredentials"] = 0
                            if ("stage" in element) and (updated["customerCredentials"] == 1):
                                junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                 DATA_KEY="STAGE", DATA_VALUE=element["stage"], CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                 LOAN_REFERENCE_ID=lacc[0]["LOAN_REFERENCE_ID"] if lacc else None,
                                                 COMMENTS=(element["loanComments"] if "loanComments" in element else None))
                            if (element["stage"] == "LOAN_APPROVED" if "stage" in element else False) and (updated["customerCredentials"] == 1):
                                mapping = {'joined': 'WORKING_SINCE', 'partner': 'PARTNER_TYPE', 'Tier': 'TIER', 'Avg1Wk': 'LAST_WEEK_INCOME',
                                           'Avg10Wk': 'AVERAGE_10_WEEK_INCOME', 'Avg3Wk': 'AVERAGE_3_WEEK_INCOME', 'loan': 'LOANS',
                                           'present_status': 'PRESENT_STATUS', 'unknown': 'UNKNOWN'}
                                loanComments = (
                                    element["loanComments"] if "loanComments" in element else None)
                                try:
                                    loanComments = {mapping[(ele.split(":")[0] if ele.split(":")[0] in mapping else 'unknown')]: ":".join(
                                        ele.split(":")[1:]) for ele in loanComments.split(",") if len(ele.split(":")) >= 2}
                                    loanComments["PARTNER_TYPE"] = (loanComments["PARTNER_TYPE"].split("(")[0] if len(
                                        loanComments["PARTNER_TYPE"].split("(")) > 0 else None) if "PARTNER_TYPE" in loanComments else None
                                    loanComments["TIER"] = (loanComments["TIER"].split("(")[0] if len(
                                        loanComments["TIER"].split("(")) > 0 else None) if "TIER" in loanComments else None
                                    loanComments["PRESENT_STATUS"] = (loanComments["PRESENT_STATUS"].split("(")[0] if len(
                                        loanComments["PRESENT_STATUS"].split("(")) > 0 else None) if "PRESENT_STATUS" in loanComments else None
                                    try:
                                        loanStats = (json.loads(loanComments.pop("LOANS").replace(
                                            ';', ',')) if loanComments["LOANS"] else []) if "LOANS" in loanComments else []
                                    except:
                                        loanStats = []
                                    if loanStats != []:
                                        loanStats = sorted(
                                            loanStats, key=lambda i: i['date'], reverse=True)
                                    for i in range(5-len(loanStats)):
                                        loanStats.append(
                                            {"amount": None, "id": None, "date": None, "overdue": None, "avgDelay": None})
                                    loanStats = {"LOAN1_DATE": loanStats[0]["date"], "LOAN1_ID": loanStats[0]["id"],
                                                 "LOAN1_OVERDUE": loanStats[0]["overdue"], "LOAN1_DELAY": loanStats[0]["avgDelay"],
                                                 "LOAN1_AMOUNT": loanStats[0]["amount"], "LOAN2_DATE": loanStats[1]["date"],
                                                 "LOAN2_ID": loanStats[1]["id"], "LOAN2_OVERDUE": loanStats[1]["overdue"],
                                                 "LOAN2_DELAY": loanStats[1]["avgDelay"], "LOAN2_AMOUNT": loanStats[1]["amount"],
                                                 "LOAN3_DATE": loanStats[2]["date"], "LOAN3_ID": loanStats[2]["id"],
                                                 "LOAN3_OVERDUE": loanStats[2]["overdue"], "LOAN3_DELAY": loanStats[2]["avgDelay"],
                                                 "LOAN3_AMOUNT": loanStats[2]["amount"], "LOAN4_DATE": loanStats[3]["date"],
                                                 "LOAN4_ID": loanStats[3]["id"], "LOAN4_OVERDUE": loanStats[3]["overdue"],
                                                 "LOAN4_DELAY": loanStats[3]["avgDelay"], "LOAN4_AMOUNT": loanStats[3]["amount"],
                                                 "LOAN5_DATE": loanStats[4]["date"], "LOAN5_ID": loanStats[4]["id"],
                                                 "LOAN5_OVERDUE": loanStats[4]["overdue"], "LOAN5_DELAY": loanStats[4]["avgDelay"],
                                                 "LOAN5_AMOUNT": loanStats[4]["amount"]}
                                    loanComments = {k: v for k, v in six.iteritems(
                                        loanComments) if (k != 'UNKNOWN') and (v != '')}
                                except:
                                    loanComments = {}
                                if loanComments != {}:
                                    junk = db.Insert(db="mint_loan", table="mw_loan_approval_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     LOAN_AMOUNT=(
                                                         (str(int(lacc[0]["AMOUNT"])) if lacc[0]["AMOUNT"] else None) if lacc else None),
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                     LOAN_REFERENCE_ID=(
                                                         lacc[0]["LOAN_REFERENCE_ID"] if lacc else None),
                                                     **utils.mergeDicts(loanComments, {k: (str(v) if type(v) == int else v) for k, v in six.iteritems(loanStats)}))
                        elif element["updateType"] == "clientBank":
                            utils.logger.info(
                                "Updating bank information", extra=logInfo)
                            expKeys = ["ACTIVE_STATUS_FLAG", "DEFAULT_STATUS_FLAG", "DELETE_STATUS_FLAG", "PERSONAL_ACCOUNT_FLAG",  # "DOC_SEQ_ID",
                                       "ACCOUNT_NO", "IFSC_CODE", "BANK_NAME", "BRANCH", "CITY", "COMMON_ACCT_NO", "ACCOUNT_HOLDER_NAME"]
                            payload = {"accountNumber": element["ACCOUNT_NO"] if "ACCOUNT_NO" in element else element["accountNo"] if "accountNo" in element else None,
                                       "ifscCode": element["IFSC_CODE"] if "IFSC_CODE" in element else element["ifscCode"] if "ifscCode" in element else None, "locale": "en",
                                       "bankName": element["BANK_NAME"] if "BANK_NAME" in element else element["bankName"] if "bankName" in element else None, "dateFormat": "dd MMMM yyyy",
                                       "bankCity": element["CITY"] if "CITY" in element else element["city"] if "city" in element else None,
                                       "accountHolderName": element["ACCOUNT_HOLDER_NAME"] if "ACCOUNT_HOLDER_NAME" in element else element["accountHolderName"] if "accountHolderName" in element else None,
                                       "branchName": element["BRANCH"] if "BRANCH" in element else element["branch"] if "branch" in element else None}
                            payload = {k: v for k,
                                       v in payload.items() if v is not None}
                            inputDict = {key: element[utils.camelCase(key)] for key in [
                                k for k in expKeys if utils.camelCase(k) in list(element.keys())]}
                            updated["clientBank"] = boolmap[db.Update(conditions={"CUSTOMER_ID = ": custID, "ID = ": str(element["bankID"])},
                                                                      db="mint_loan", table="mw_cust_bank_detail", **inputDict)]
                            if (updated["clientBank"] == 1):
                                for key in element:
                                    junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False,
                                                     CUSTOMER_ID=custID, DATA_KEY=key, DATA_VALUE=element[key],
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif element["updateType"] == "uberApi":
                            utils.logger.info(
                                "Enabling uber api request for the customer", extra=logInfo)
                            updated["uberApi"] = boolmap[db.Insert(db="mint_loan", table="mw_show_third_party_login", compulsory=False, date=False,
                                                                   noIgnor=False, COMPANY_ID="3", ENABLED="1", CUSTOMER_ID=custID,
                                                                   CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                   CREATED_BY=input_dict["msgHeader"]["authLoginID"])]
                            db.Update(db="mint_loan", table="mw_show_third_party_login",
                                      ENABLED="1", conditions={"CUSTOMER_ID=": custID})
                        elif element["updateType"] == "loanLimit":
                            utils.logger.info(
                                "Updating loan limit", extra=logInfo)
                            updated["clientLoanLimit"] = boolmap[db.Update(db="mint_loan", table="mw_client_loan_limit",
                                                                           conditions={
                                                                               "CUSTOMER_ID = ": custID},
                                                                           LOAN_LIMIT=element["loanLimit"] if "loanLimit" in element else None,
                                                                           MOBILE_LOAN_LIMIT=element[
                                                                               "mobLimit"] if "mobLimit" in element else None,
                                                                           TYRE_LOAN_LIMIT=element[
                                                                               "tyreLimit"] if "tyreLimit" in element else None,
                                                                           INSURANCE_LOAN_LIMIT=element[
                                                                               "insuranceLimit"] if "insuranceLimit" in element else None,
                                                                           EDUCATION_LOAN_LIMIT=element[
                                                                               "educationLimit"] if "educationLimit" in element else None,
                                                                           COMMENTS=element["comments"])]
                            if (updated["clientLoanLimit"] == 1):
                                if "loanLimit" in element:
                                    junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                     DATA_KEY="LOAN_LIMIT", DATA_VALUE=element["loanLimit"], COMMENTS=element["comments"],
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                if "mobLimit" in element:
                                    junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                     DATA_KEY="MOBILE_LOAN_LIMIT", DATA_VALUE=element["mobLimit"], COMMENTS=element["comments"],
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                if "tyreLimit" in element:
                                    junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                     DATA_KEY="TYRE_LOAN_LIMIT", DATA_VALUE=element["tyreLimit"], COMMENTS=element["comments"],
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                if "insuranceLimit" in element:
                                    junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                     DATA_KEY="INSURANCE_LOAN_LIMIT", DATA_VALUE=element["insuranceLimit"], COMMENTS=element["comments"],
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                if "educationLimit" in element:
                                    junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                     DATA_KEY="EDUCATION_LOAN_LIMIT", DATA_VALUE=element["educationLimit"], COMMENTS=element["comments"],
                                                     CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif element["updateType"] == "city":
                            utils.logger.info(
                                "Updating uber number verification status", extra=logInfo)
                            updated["clientCity"] = boolmap[db.Update(db="mint_loan", table="mw_client_profile",
                                                                      conditions={"CUSTOMER_ID = ": custID}, CURRENT_CITY=element["city"])]
                            if (updated["clientCity"] == 1):
                                junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                 DATA_KEY="CITY_CHANGE", DATA_VALUE=element["city"], CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif element["updateType"] == "uberData":
                            vBy = input_dict["msgHeader"]["authLoginID"]
                            utils.logger.info(
                                "Verifying uber data", extra=logInfo)
                            q = Query.from_(compProf).select("DRIVER_ID", "AUTO_ID", "PHONE_NUMBER").where(
                                compProf.CUSTOMER_ID == custID)
                            profID = db.runQuery(
                                q.orderby(compProf.AUTO_ID, order=Order.desc).limit(1))["data"]
                            profID = profID[0] if profID else {
                                "DRIVER_ID": None, "AUTO_ID": "0", "PHONE_NUMBER": "+919930726420"}
                            exist = db.runQuery(Query.from_(cdata).select(functions.Count(cdata.CUSTOMER_ID).as_("c")).where(
                                (cdata.CUSTOMER_ID == custID) & (cdata.DATA_VALUE == (profID["PHONE_NUMBER"][-10:] if profID["PHONE_NUMBER"] else ''))))
                            loginID = db.runQuery(Query.from_(cred).select(
                                cred.LOGIN_ID).where(cred.CUSTOMER_ID == custID))["data"]
                            ut = utils()
                            respDict = ut.store_customer_data(dataKey="COMPANY_NUMBER", dataValue=(
                                profID["PHONE_NUMBER"][-10:] if profID["PHONE_NUMBER"] else ''), loginId=loginID[0]["LOGIN_ID"], adminId="ADMIN") if (loginID != []) and (exist["data"][0]["c"] == 0) else False
                            q = Query.from_(compProf).select(compProf.CONFIRMED_CUSTOMER_ID.as_(
                                "c")).where(compProf.DRIVER_ID == profID["DRIVER_ID"])
                            exist = db.runQuery(q.where((compProf.CONFIRMED_CUSTOMER_ID != 0) & (
                                compProf.CONFIRMED_CUSTOMER_ID.notnull())))["data"]
                            # exist condition is not checked when confirmed is 0
                            if (element["confirmed"] == 0) | (exist == []):
                                q = Query.from_(uauth).select(uauth.AUTO_ID).where(
                                    uauth.CUSTOMER_ID == custID).orderby(uauth.AUTO_ID, order=Order.desc)
                                autoID = db.runQuery(q.limit(1))["data"]
                                q = Query.from_(sess).select("REQUEST_ID").where(
                                    sess.CUSTOMER_ID == custID)
                                requestID = db.runQuery(
                                    q.orderby(sess.REQUEST_ID, order=Order.desc).limit(1))["data"]
                                Updated = db.Update(db="mw_company_3", table="mw_authorization_dump",
                                                    conditions={"AUTO_ID = ": str(autoID[0]["AUTO_ID"]) if autoID else "0"}, CONFIRMED_BY=vBy,
                                                    CONFIRMED_CUSTOMER_ID=custID if element[
                                                        "confirmed"] == 1 else "0",
                                                    CONFIRMED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                if (element["confirmed"] == 0):
                                    db.Update(db="mw_company_3", table="mw_authorization_dump", conditions={"CUSTOMER_ID=": str(
                                        custID), "CONFIRMED_CUSTOMER_ID IS NULL": ""}, CONFIRMED_CUSTOMER_ID="0", CONFIRMED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), CONFIRMED_BY=vBy)
                                    db.Update(db="mw_company_3", table="mw_profile_info", conditions={"CUSTOMER_ID=": str(
                                        custID), "CONFIRMED_CUSTOMER_ID IS NULL": ""}, CONFIRMED_CUSTOMER_ID="0", CONFIRMED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), CONFIRMED_BY=vBy)
                                    q = db.runQuery(Query.from_(inc).select(
                                        inc.AUTO_ID).where(inc.CUSTOMER_ID == custID))["data"]
                                    for aid in q:
                                        db.Update(db="mw_company_3", table="mw_derived_income_data", conditions={"AUTO_ID =": str(aid["AUTO_ID"])}, CUSTOMER_ID="0", PREVIOUS_CUSTOMER_ID=custID, PREVIOUS_CUSTOMER_ID_DATE=datetime.now(
                                        ).strftime("%Y-%m-%d %H:%M:%S"), MODIFIED_BY=vBy, MODIFIED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                    db.Update(db="mw_company_3", table="mw_driver_id_mapping", conditions={"AUTH_ID=": str(
                                        autoID[0]["AUTO_ID"])}, AUTH_ID="0", PREVIOUS_AUTH_ID=str(autoID[0]["AUTO_ID"]))
                            else:
                                Updated = 0
                                success = "driver id already mapped to customer id " + \
                                    (str(exist[0]["c"]) if exist != [] else "")
                            updated["uberData"] = 0
                            if (Updated == 1):
                                updated["uberData"] = boolmap[db.Update(db="mw_company_3", table="mw_profile_info", CONFIRMED_BY=vBy,
                                                                        conditions={"AUTO_ID = ": str(
                                                                            profID["AUTO_ID"]) if profID else "0"},
                                                                        CONFIRMED_CUSTOMER_ID=custID if element[
                                                                            "confirmed"] == 1 else "0",
                                                                        CONFIRMED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
                                db.Update(db="mint_loan", table="mw_company_login_session", LOGIN_SUCCESS="1" if element["confirmed"] == 1 else "0",
                                          conditions={"REQUEST_ID = ": str(requestID[0]["REQUEST_ID"]) if requestID else "0"})
                            if (updated["uberData"] == 1):
                                junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                 DATA_KEY="UBER_DATA_VERIFICATION", DATA_VALUE="VERIFIED" if element["confirmed"] == 1 else "REJECTED",
                                                 CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif element["updateType"] == "uberNumber":
                            vBy = input_dict["msgHeader"]["authLoginID"]
                            utils.logger.info(
                                "Updating uber number verification status", extra=logInfo)
                            updated["clientUberNumber"] = boolmap[db.Update(db="mint_loan", table="mw_client_profile",
                                                                            conditions={"CUSTOMER_ID = ": custID}, NUMBER_VERIFIED_BY=vBy,
                                                                            NUMBER_VERIFIED=element["numberVerified"],
                                                                            NUMBER_COMMENT=element["comments"],
                                                                            VERIFIED_NUMBER=element["number"] if "number" in element else None)]
                            if (updated["clientUberNumber"] == 1):
                                junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                 DATA_KEY="UBER_NUMBER_VERIFICATION", DATA_VALUE=element["numberVerified"],
                                                 COMMENTS=(
                                                     (element["number"] if "number" in element else "") + element["comments"]),
                                                 CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif element["updateType"] == "uberName":
                            vBy = input_dict["msgHeader"]["authLoginID"]
                            all_normal_characters = string.ascii_letters + string.digits + ' '
                            utils.logger.info(
                                "Updating uber name verification status", extra=logInfo)
                            updated["clientUberName"] = boolmap[db.Update(db="mint_loan", table="mw_client_profile",
                                                                          conditions={"CUSTOMER_ID = ": custID}, NAME_VERIFIED_BY=vBy,
                                                                          NAME_VERIFIED=element[
                                                                              "nameVerified"], NAME_COMMENT=element["comments"],
                                                                          VERIFIED_NAME=("".join([X if X in all_normal_characters else '' for X in element["name"]]) if "name" in element else None))]
                            if (updated["clientUberName"] == 1):
                                junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                 DATA_KEY="UBER_NUMBER_VERIFICATION", DATA_VALUE=element["nameVerified"],
                                                 COMMENTS=(("".join(
                                                     [X if X in all_normal_characters else '' for X in element["name"]]) if "name" in element else "") + element["comments"]),
                                                 CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif element["updateType"] == "emailID":
                            q = db.runQuery(Query.from_(prof).select(
                                prof.CUSTOMER_DATA).where(prof.CUSTOMER_ID == custID))["data"]
                            custData = (json.loads(
                                q[0]["CUSTOMER_DATA"]) if q[0]["CUSTOMER_DATA"] else {}) if q else {}
                            custData.update({"email": element["emailID"]})
                            utils.logger.info(
                                "Updating emailID", extra=logInfo)
                            updated["clientEmailID"] = boolmap[db.Update(db="mint_loan", table="mw_client_profile",
                                                                         conditions={"CUSTOMER_ID = ": custID}, EMAIL_ID=element["emailID"],
                                                                         CUSTOMER_DATA=json.dumps(
                                                                             custData),
                                                                         MODIFIED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                                         MODIFIED_BY=input_dict["msgHeader"]["authLoginID"])]
                            if (updated["clientEmailID"] == 1):
                                junk = db.Insert(db="mint_loan", table="mw_customer_change_log", date=False, compulsory=False, CUSTOMER_ID=custID,
                                                 DATA_KEY="EMAIL_ID", DATA_VALUE=element["emailID"],
                                                 CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    token = generate(db).AuthToken() if ("clientDocument" not in [ele["updateType"] for ele in input_dict["data"]["update"]]) else {
                        "updated": True, "token": input_dict["msgHeader"]["authToken"]}
                    if token["updated"]:
                        output_dict["data"]["updated"] = updated
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
