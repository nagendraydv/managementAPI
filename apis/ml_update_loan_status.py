from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
import six


class UpdateLoanStatusResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def formatDate(self, x):
        if isinstance(x, list) and len(x) == 3:
            try:
                return datetime(x[0], x[1], x[2]).strftime("%Y-%m-%d %H:%M:%S")
            except:
                return None

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Loan status updated successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='custDetails', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custID = input_dict["data"]["customerID"]
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    #params = Query.from_(conf2).select("CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin(["FinfluxAccessToken"]))
                    #params = {ele["CONFIG_KEY"]:ele["CONFIG_VALUE"] for ele in db.runQuery(params)["data"]}
                    #headers = utils.mergeDicts(utils.finflux_headers, {"Authorization":"bearer " + params["FinfluxAccessToken"]})
                    #baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY=="FINFLUX_URL"))
                    #baseurl = baseurl["data"][0]["CONFIG_VALUE"] if baseurl["data"] else ""
                    loanIDs = Query.from_(loanmaster).select("LOAN_REFERENCE_ID", "ID", "STATUS", "LOAN_ACCOUNT_NO", "LOAN_DISBURSED_DATE",
                                                             "LOAN_REQUEST_DATE", "CUSTOMER_ID", "LENDER").where(loanmaster.CUSTOMER_ID == custID)
                    loanIDs = db.runQuery(loanIDs.where(loanmaster.STATUS.notin(
                        ["REJECTED", "CLOSED", "ML_REJECTED", "WRITTEN-OFF"])))
                    mapp = {100: "PENDING", 200: "IN-APPROVAL", 300: "APPROVED",
                            400: "ACTIVE", 500: "REJECTED", 600: "REJECTED"}
                    mapp2 = {"GETCLARITY": {100: "PENDING", 200: "APPROVED", 300: "ACTIVE", 400: "REPAID", 500: "REJECTED", 600: "REPAID", 700: "REPAID"},
                             "CHAITANYA": {100: "PENDING_APPROVAL", 200: "WAITING_FOR_DISBURSAL", 300: "ACTIVE", 400: "CLOSED", 500: "REPAID",
                                           600: "REPAID", 700: "REPAID", 800: "REPAID", 900: "REPAID"}}
                    # 'CHAITANYA' in [ele['LENDER'] for ele in loanIDs["data"]]:
                    if True:
                        syncTranReq = {"msgHeader": {"loginId": "+919820409247", "consumerId": "407", "authToken": "", "channelType": "M"},
                                       "deviceFPmsgHeader": {"imeiNo": "352801082735635", "osName": "Android", "osVersion": "23", "versionCode": "23",
                                                             "versionName": "3.6", "dualSim": "true", "deviceModelNo": "SM-J210F", "country": "",
                                                             "deviceManufacturer": "samsung", "timezone": "Asia/Kolkata", "nwProvider": "Jio 4G",
                                                             "connectionMode": "4G", "latitude": "", "longitude": ""}, "data": {"customerId": str(custID)}}
                    syncTransactionURL = 'http://13.126.29.47:8080/mintLoan/mintloan/syncFinfluxTransactions'
                    count = 0
                    for datum in loanIDs["data"]:
                        if datum["LENDER"] == "GETCLARITY":
                            headers = (
                                utils.finflux_headers[datum["LENDER"]] if datum["LENDER"] in utils.finflux_headers else {})
                            auth = utils.mifos_auth
                            urlKey = "MIFOS_URL"
                            baseurl = db.runQuery(Query.from_(conf).select(
                                "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"] if baseurl["data"] else ""
                        else:
                            urlKey = "FINFLUX_URL"
                            baseurl = db.runQuery(Query.from_(conf).select(
                                "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"] if baseurl["data"] else ""
                            tokenKey = "MintwalkFinfluxAccessToken" if datum[
                                "LENDER"] == "GETCLARITY" else "FinfluxAccessToken"
                            params = db.runQuery(Query.from_(conf2).select(
                                "CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin([tokenKey])))
                            params = {
                                "FinfluxAccessToken": ele["CONFIG_VALUE"] for ele in params["data"]}
                            headers = utils.mergeDicts((utils.finflux_headers[datum["LENDER"]] if datum["LENDER"] in utils.finflux_headers else {}),
                                                       {"Authorization": "bearer " + params["FinfluxAccessToken"]})
                        if ((not datum["LOAN_ACCOUNT_NO"]) and (datum["LOAN_REFERENCE_ID"]) and (datum["LOAN_REFERENCE_ID"] != '0')):
                            loanID = datum["LOAN_REFERENCE_ID"]
                            r = requests.get(
                                baseurl + "loanapplicationreferences/" + loanID, data=json.dumps({}), headers=headers)
                            resp2 = r.json()
                            input_dict2 = {"STATUS": mapp[resp2["status"]["id"]] if resp2["status"]["id"] in mapp else None, "MODIFIED_BY": "CRON",
                                           "MODIFIED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                           "LOAN_APPLICATION_NO": resp2["loanApplicationReferenceNo"]}
                            if 'loanId' in resp2:
                                #q = db.runQuery(Query.from_(custcred).select("STAGE").where(custcred.CUSTOMER_ID==str(datum["CUSTOMER_ID"])))
                                # if q["data"][0]["STAGE"]!="CUSTOMER":
                                #    db.Update(db="mint_loan", table="mw_customer_login_credentials",
                                #              conditions={"CUSTOMER_ID = ": str(datum["CUSTOMER_ID"])}, checkAll=False, STAGE="CUSTOMER")
                                input_dict2.update(
                                    {"LOAN_ACCOUNT_NO": str(resp2['loanId'])})
                                db._DbClose_()
                                db = DB()
                                #params = db.runQuery(Query.from_(conf2).select("CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin(["FinfluxAccessToken"])))
                                #params = {ele["CONFIG_KEY"]:ele["CONFIG_VALUE"] for ele in params["data"]}
                                #headers = utils.mergeDicts(utils.finflux_headers, {"Authorization":"bearer " + params["FinfluxAccessToken"]})
                                r = requests.get(
                                    baseurl + "loans/" + input_dict2["LOAN_ACCOUNT_NO"], data=json.dumps({}), headers=headers)
                                resp2 = r.json()
                                input_dict2.update({"LOAN_DISBURSED_DATE": "-".join(str(ele)
                                                                                    for ele in (resp2["timeline"]["actualDisbursementDate"]
                                                                                                if "actualDisbursementDate" in resp2["timeline"]
                                                                                                else "")),
                                                    "LOAN_APPROVAL_DATE": "-".join(str(ele) for ele in (resp2["timeline"]["approvedOnDate"]
                                                                                                        if "approvedOnDate" in resp2["timeline"]
                                                                                                        else ""))})
                                input_dict2 = {k: v for k, v in six.iteritems(
                                    input_dict2) if ((v is not None) and (v != ''))}
                                db.Update(db="mint_loan", table="mw_client_loan_master", conditions={"ID = ": str(datum["ID"])}, checkAll=False,
                                          **input_dict2)
                                input_dict2 = {"APPROVED_PRINCIPAL": str(resp2["approvedPrincipal"]), "PRINCIPAL": str(resp2["principal"]),
                                               "PROPOSED_PRINCIPAL": str(resp2["proposedPrincipal"]),
                                               "INTEREST_RATE_PER_PERIOD": str(resp2["interestRatePerPeriod"]),
                                               "LOAN_TYPE": resp2["loanType"]["value"], "STATUS": resp2["status"]["value"],
                                               "AMORTIZATION_TYPE": resp2["amortizationType"]["id"],
                                               "EXPECTED_DISBURSEMENT_DATE": (self.formatDate(resp2["timeline"]["expectedDisbursementDate"])
                                                                              if "expectedDisbursementDate" in resp2["timeline"] else None),
                                               "ACTUAL_DISBURSEMENT_DATE": (self.formatDate(resp2["timeline"]["actualDisbursementDate"])
                                                                            if "actualDisbursementDate" in resp2["timeline"] else None),
                                               "EXPECTED_MATURITY_DATE": (self.formatDate(resp2["timeline"]["expectedMaturityDate"])
                                                                          if "expectedMaturityDate" in resp2["timeline"] else None)}
                                try:
                                    input_dict2.update({"TOTAL_EXPECTED_REPAYMENT": str(resp2["summary"]["totalExpectedRepayment"]),
                                                        "TOTAL_REPAYMENT": str(resp2["summary"]["totalRepayment"]),
                                                        "TOTAL_COST_OF_LOAN": str(resp2["summary"]["totalCostOfLoan"]),
                                                        "TOTAL_EXPECTEDCOST_OF_LOAN": str(resp2["summary"]["totalExpectedCostOfLoan"]),
                                                        "TOTAL_OUTSTANDING": str(resp2["summary"]["totalOutstanding"]),
                                                        "FEES_CHARGES_CHARGED": str(resp2["summary"]["feeChargesCharged"]),
                                                        "FEES_CHARGES_WAIVED": str(resp2["summary"]["feeChargesWaived"]),
                                                        "TOTAL_WAIVED": str(resp2["summary"]["totalWaived"]),
                                                        "TOTAL_OVERDUE": str(resp2["summary"]["totalOverdue"]),
                                                        "PAID_IN_ADVANCE": str(resp2["paidInAdvance"]["paidInAdvance"])})
                                except:
                                    pass
                                input_dict2 = {k: v for k, v in six.iteritems(
                                    input_dict2) if ((v is not None) and (v != ''))}
                                db.Update(db="mint_loan", table="mw_client_loan_details", checkAll=False,
                                          conditions={"LOAN_MASTER_ID = ": str(datum["ID"])}, **input_dict2)
                        elif datum["LOAN_ACCOUNT_NO"]:
                            loanID = datum["LOAN_ACCOUNT_NO"]
                            if datum["LENDER"] == 'GETCLARITY':
                                r = requests.get(
                                    baseurl + "loans/" + loanID, headers=headers, auth=auth, data=json.dumps({}), verify=False)
                            else:
                                r = requests.get(
                                    baseurl + "loans/" + loanID, data=json.dumps({}), headers=headers)
                            resp2 = r.json()
                            input_dict2 = {"STATUS": (mapp2[datum["LENDER"]][resp2["status"]["id"]]
                                                      if (resp2["status"]["id"] in mapp2[datum["LENDER"]] if "status" in resp2 else False) else None),
                                           "MODIFIED_BY": "CRON_STATUS_UPDATE",
                                           "LOAN_DISBURSED_DATE": "-".join(str(ele)
                                                                           for ele in (resp2["timeline"]["actualDisbursementDate"]
                                                                                       if (("actualDisbursementDate" in resp2["timeline"]) if "timeline" in resp2 else False) else "")),
                                           "LOAN_APPROVAL_DATE": "-".join(str(ele) for ele in (resp2["timeline"]["approvedOnDate"]
                                                                                               if (("approvedOnDate" in resp2["timeline"]) if "timeline" in resp2 else False) else "")),
                                           "MODIFIED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                            input_dict2 = {k: v for k, v in six.iteritems(
                                input_dict2) if ((v is not None) and (v != ''))}
                            if "timeline" in resp2:
                                db.Update(db="mint_loan", table="mw_client_loan_master", conditions={"ID = ": str(datum["ID"])}, checkAll=False,
                                          **input_dict2)
                            # if (input_dict2["STATUS"] in ("REPAID", "REJECTED", "CLOSED") if "STATUS" in input_dict2 else False):
                                #q = db.runQuery(Query.from_(custcred).select("STAGE").where(custcred.CUSTOMER_ID==str(datum["CUSTOMER_ID"])))
                                # if q["data"][0]["STAGE"]!="GOOD_TO_LEND":
                                #    db.Update(db="mint_loan", table="mw_customer_login_details",
                                #              conditions={"CUSTOMER_ID = ": str(datum["CUSTOMER_ID"])}, checkAll=False, STAGE="GOOD_TO_LEND")
                            input_dict2 = {"APPROVED_PRINCIPAL": str(resp2["approvedPrincipal"]) if "approvedPrincipal" in resp2 else None, "PRINCIPAL": str(resp2["principal"]) if "principal" in resp2 else None,
                                           "PROPOSED_PRINCIPAL": str(resp2["proposedPrincipal"]) if "proposedPrincipal" in resp2 else None,
                                           "INTEREST_RATE_PER_PERIOD": str(resp2["interestRatePerPeriod"]) if "interestRatePerPeriod" in resp2 else None,
                                           "LOAN_TYPE": resp2["loanType"]["value"] if "loanType" in resp2 else None, "STATUS": resp2["status"]["value"] if "status" in resp2 else None,
                                           "AMORTIZATION_TYPE": resp2["amortizationType"]["id"] if "amortizationType" in resp2 else None,
                                           "EXPECTED_DISBURSEMENT_DATE": (self.formatDate(resp2["timeline"]["expectedDisbursementDate"])
                                                                          if (("expectedDisbursementDate" in resp2["timeline"]) if "timeline" in resp2 else False) else None),
                                           "ACTUAL_DISBURSEMENT_DATE": (self.formatDate(resp2["timeline"]["actualDisbursementDate"])
                                                                        if (("actualDisbursementDate" in resp2["timeline"]) if "timeline" in resp2 else False) else None),
                                           "EXPECTED_MATURITY_DATE": (self.formatDate(resp2["timeline"]["expectedMaturityDate"])
                                                                      if (("expectedMaturityDate" in resp2["timeline"]) if "timeline" in resp2 else False) else None)}
                            try:
                                input_dict2.update({"TOTAL_EXPECTED_REPAYMENT": str(resp2["summary"]["totalExpectedRepayment"]),
                                                    "TOTAL_REPAYMENT": str(resp2["summary"]["totalRepayment"]),
                                                    "TOTAL_COST_OF_LOAN": str(resp2["summary"]["totalCostOfLoan"]),
                                                    "TOTAL_EXPECTEDCOST_OF_LOAN": str(resp2["summary"]["totalExpectedCostOfLoan"]),
                                                    "TOTAL_OUTSTANDING": str(resp2["summary"]["totalOutstanding"]),
                                                    "FEES_CHARGES_CHARGED": str(resp2["summary"]["feeChargesCharged"]),
                                                    "FEES_CHARGES_WAIVED": str(resp2["summary"]["feeChargesWaived"]),
                                                    "TOTAL_WAIVED": str(resp2["summary"]["totalWaived"]),
                                                    "TOTAL_OVERDUE": str(resp2["summary"]["totalOverdue"]),
                                                    "PAID_IN_ADVANCE": str(resp2["paidInAdvance"]["paidInAdvance"])})
                            except:
                                pass
                            input_dict2 = {k: v for k, v in six.iteritems(
                                input_dict2) if ((v is not None) and (v != ''))}
                            if "timeline" in resp2:
                                db.Update(db="mint_loan", table="mw_client_loan_details", checkAll=False,
                                          conditions={"LOAN_MASTER_ID = ": str(datum["ID"])}, **input_dict2)
                    if (datum["LENDER"] in ('CHAITANYA', 'GETCLARITY')) and (("timeline" in resp2) if "resp2" in locals() else False):
                        r = requests.post(syncTransactionURL, data=json.dumps(
                            syncTranReq), headers={'content-type': 'application/json'})
                    # if (r.status_code==200) & (r.json()["type"]=='success' if 'type' in r.json() else False):
                    #token = generate(db).AuthToken()
                    if True:  # token["updated"]:
                        output_dict["data"].update({"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                    # else:
                    #    output_dict["data"].update({"error":1, "message":"Message sending failed. Please try again."})
                    resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
