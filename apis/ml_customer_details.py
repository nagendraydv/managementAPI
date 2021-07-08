from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType
from pypika import functions as fn


class CustDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"custCredentials": {}, "custDetails": {}, "custKycDocuments": [], "docTypes": [],
                                                                "stages": []}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'customerDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
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
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    stagemaster = Table("mw_stage_master", schema="mint_loan")
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents",
                                    schema="mint_loan")
                    doctype = Table("mw_kyc_document_type", schema="mint_loan")
                    custkyc = Table("mw_aadhar_kyc_details",
                                    schema="mint_loan")
                    aadhar = Table("mw_aadhar_status", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    loanmaster = Table(
                        "mw_client_loan_master", schema="mint_loan")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    docGroup = Table("mw_document_group", schema="mint_loan")
                    groupMap = Table(
                        "mw_document_group_mapping", schema="mint_loan")
                    custCredentials = db.runQuery(Query.from_(custcred).select("LOGIN_ID", "ACCOUNT_STATUS", "LAST_LOGIN", "CUSTOMER_ID", "STAGE",
                                                                               "REGISTERED_IP_ADDRESS", "LAST_LOGGED_IN_IP_ADDRESS", "COMMENTS",
                                                                               "DEVICE_ID", "CREATED_DATE", "REJECTED", "DOCUMENT_COMMENTS",
                                                                               "REJECTION_REASON").where(custcred.CUSTOMER_ID == custID))
                    custBankDetails = db.runQuery(Query.from_(custbank).select(
                        custbank.star).where(custbank.CUSTOMER_ID == custID))
                    loan_status = [ele["STATUS"] for ele in db.runQuery(Query.from_(loanmaster).select(loanmaster.STATUS).where(
                        (loanmaster.CUSTOMER_ID == custID) & (loanmaster.STATUS.notin(['ML_REJECTED', 'REJECTED']))))["data"]]
                    custDetails = db.runQuery(Query.from_(profile).select(
                        profile.star).where(profile.CUSTOMER_ID == custID))
                    custKycDetails = db.runQuery(Query.from_(custkyc).select(
                        custkyc.star).where(custkyc.CUSTOMER_ID == custID))
                    custAadharNo = Query.from_(aadhar).select("AADHAR_NO").where(
                        (aadhar.CUSTOMER_ID == custID) & (aadhar.ARCHIVED == 'N'))
                    custAadharNo = db.runQuery(custAadharNo.orderby(
                        aadhar.CREATED_DATE, order=Order.desc).limit(1))
                    custPan = Query.from_(pan).select("PAN_NO").where(
                        pan.CUSTOMER_ID == custID).orderby(pan.CREATED_DATE, order=Order.desc)
                    custPan = db.runQuery(custPan.limit(1))
                    q = Query.from_(kycdocs).join(doctype).on_field(
                        "DOCUMENT_TYPE_ID").select(kycdocs.star, doctype.DOCUMENT_TYPE)
                    if (input_dict["data"]["docTypeIDs"] if "docTypeIDs" in input_dict["data"] else False):
                        q = q.where(kycdocs.DOCUMENT_TYPE_ID.isin(
                            input_dict["data"]["docTypeIDs"]))
                    custKycDocuments = db.runQuery(q.where(kycdocs.CUSTOMER_ID == custID).orderby(
                        kycdocs.CREATED_DATE, order=Order.desc))
                    docType = Query.from_(doctype).join(
                        groupMap, how=JoinType.left).on_field("DOCUMENT_TYPE_ID")
                    docType = docType.join(docGroup, how=JoinType.left).on(
                        docGroup.ID == groupMap.GROUP_ID)
                    docType = docType.select(
                        doctype.DOCUMENT_TYPE_ID, doctype.DOCUMENT_TYPE, docGroup.DOCUMENT_TYPE.as_("DOCUMENT_GROUP"))
                    #docType = Query.from_(doctype).select(doctype.DOCUMENT_TYPE_ID, doctype.DOCUMENT_TYPE, doctype.DOCUMENT_GROUP)
                    docType = db.runQuery(docType.where(
                        doctype.DOCUMENT_TYPE_ID.notin(["103", "104", "105", "107"])))

                    stages = [{"STAGE": ele["STAGE"], "DISABLED":ele["DISABLED"]} for ele in db.runQuery(
                        Query.from_(stagemaster).select("STAGE", "DISABLED"))["data"] if ele["STAGE"]]
                    for datum in custKycDocuments["data"]:
                        if datum["DOCUMENT_TYPE"] == "PAN":
                            panNo = custPan["data"][0]["PAN_NO"] if custPan["data"] else ""
                            father = custKycDetails["data"][0]["CO"] if custKycDetails["data"] else ""
                            name = (custKycDetails["data"][0]["NAME"] if custKycDetails["data"] else custDetails["data"][0]["NAME"]
                                    if custDetails["data"] else "")
                            dob = (custKycDetails["data"][0]["DOB"] if custKycDetails["data"] else custDetails["data"][0]["DATE_OF_BIRTH"]
                                   if custDetails["data"] else "")
                            datum.update({"DOCUMENT_DETAILS": "<br>".join(
                                [x for x in [name, father, dob, panNo] if x is not None])})
                        elif datum["DOCUMENT_TYPE"] in ("Address Proof", "Aadhar"):
                            if custKycDetails["data"]:
                                address = custKycDetails["data"][0]
                                address = " ".join([x for x in [address["HOUSE"], address["STREET"], address["VTC"], address["PIN_CODE"],
                                                                address["STATE"]] if x is not None])
                            elif custDetails["data"]:
                                address = custDetails["data"][0]["ADDRESS"]
                            else:
                                address = ""
                            if datum["DOCUMENT_TYPE"] == "Address Proof":
                                datum.update({"DOCUMENT_DETAILS": address})
                            elif datum["DOCUMENT_TYPE"] == "Aadhar":
                                aadharNo = custAadharNo["data"][0]["AADHAR_NO"] if custAadharNo["data"] else ""
                                name = (custKycDetails["data"][0]["NAME"] if custKycDetails["data"] else custDetails["data"][0]["NAME"]
                                        if custDetails["data"] else "")
                                datum.update({"DOCUMENT_DETAILS": "<br>".join(
                                    [x for x in [name, aadharNo, address] if x is not None])})
                        elif datum["DOCUMENT_TYPE"] in ["Bank Details", "Cheque"]:
                            if custBankDetails["data"]:
                                bank = custBankDetails["data"][0]
                                name = (custKycDetails["data"][0]["NAME"] if custKycDetails["data"] else custDetails["data"][0]["NAME"]
                                        if custDetails["data"] else "")
                                bankDetails = "<br>".join([x for x in [name, bank["ACCOUNT_NO"], bank["IFSC_CODE"], bank["BANK_NAME"],
                                                                       bank["BRANCH"], bank["CITY"]] if x is not None])
                            else:
                                bankDetails = ""
                            datum.update({"DOCUMENT_DETAILS": bankDetails})
                        elif datum["DOCUMENT_TYPE"] == "Income Proof":
                            if custDetails["data"]:
                                datum.update({"DOCUMENT_DETAILS": str(
                                    custDetails["data"][0]["MONTHLY_INCOME"])})
                            else:
                                datum.update({"DOCUMENT_DETAILS": ""})
                        else:
                            datum.update({"DOCUMENT_DETAILS": ""})
                    if (custCredentials["data"]):
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"]["firstLoan"] = 1 if len(
                                loan_status) <= 1 else 0
                            output_dict["data"]["docTypes"] = utils.camelCase(
                                docType["data"])
                            output_dict["data"]["stages"] = stages
                            output_dict["data"]["bankDetails"] = utils.camelCase(
                                custBankDetails["data"][0]) if custBankDetails["data"] else {}
                            output_dict["data"]["custCredentials"] = utils.camelCase(
                                custCredentials["data"][0])
                            output_dict["data"]["custDetails"] = utils.camelCase(
                                custDetails["data"][0]) if custDetails["data"] else []
                            output_dict["data"]["custKycDocuments"] = utils.camelCase(
                                custKycDocuments["data"]) if custKycDocuments["data"] else []
                            for datum in output_dict["data"]["custKycDocuments"]:
                                datum["documentUrl"] = datum["documentUrl"].split(
                                    "/")[-1]
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
