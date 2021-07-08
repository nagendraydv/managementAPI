from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime
from pypika import Query, Table, JoinType, functions, Order
import requests


class ClientRegisterResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"customerClientID": ""},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'clientRegistration'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='clientRegister2', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                if False:  # val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    custmap = Table(
                        "mw_customer_login_credentials_map", schema="mint_loan")
                    clientmaster = Table(
                        "mw_finflux_client_master", schema="mint_loan")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    aadhar = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    cvalues = Table(
                        "mw_finflux_attribute_code_values", schema="mint_loan")
                    pan = Table("mw_pan_status", schema="mint_loan")
                    custbank = Table("mw_cust_bank_detail", schema="mint_loan")
                    join = Query.from_(custcred).join(custmap, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(clientmaster, how=JoinType.left)
                    join = join.on_field("CUSTOMER_ID").join(
                        aadhar, how=JoinType.left).on_field("CUSTOMER_ID")
                    join = join.join(profile, how=JoinType.left).on_field(
                        "CUSTOMER_ID")
                    q = join.select(custcred.CUSTOMER_ID, custcred.LOGIN_ID, custcred.ACCOUNT_STATUS, custcred.FAIL_ATTEMPT, custcred.PIN_UPDATED_DATE,
                                    custcred.LAST_LOGIN, custcred.REGISTERED_IP_ADDRESS, custcred.LAST_LOGGED_IN_IP_ADDRESS, custcred.DEVICE_ID,
                                    custcred.CREATED_DATE, custmap.ACTIVE, custmap.COMMENTS, clientmaster.CLIENT_ID, clientmaster.FULL_NAME,
                                    clientmaster.ACCOUNT_NO, clientmaster.ACTIVATION_DATE, aadhar.NAME, aadhar.DOB, aadhar.GENDER, aadhar.AADHAR_NO,
                                    functions.Concat(aadhar.HOUSE, " ", aadhar.STREET, " ", aadhar.LM, " ", aadhar.LC, " ", aadhar.VTC, " ", aadhar.DISTRICT,
                                                     " ", aadhar.POST_OFFICE, " ", aadhar.STATE, " ", aadhar.PIN_CODE).as_("ADDRESS"),
                                    profile.NAME.as_("PROFILE_NAME"))
                    q = q.where(custcred.CUSTOMER_ID ==
                                input_dict['data']['customerID'])
                    Fields = db.runQuery(q)
                    baseurl = db.runQuery(Query.from_(conf).select(
                        "CONFIG_VALUE").where(conf.CONFIG_KEY == "FINFLUX_URL"))
                    custID = input_dict['data']['customerID']
                    today = datetime.now().strftime("%d %B %Y")
                    if Fields["data"] and custID != 0 and baseurl["data"]:
                        if (not Fields["data"][0]["CLIENT_ID"]):
                            data = Fields["data"][0]
                            baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                            name = data["NAME"].rstrip(
                                " ") if data["NAME"] else data["PROFILE_NAME"].strip(" ")
                            payload = {"officeId": 198, "firstname": " ".join(name.split(" ")[0:-1]) if len(name.split(" ")) > 1 else name,
                                       "lastname": name.split(" ")[-1], "externalId": "000000" + custID, "dateFormat": "dd MMMM yyyy",
                                       "locale": "en", "active": False, "activationDate": today, "submittedOnDate": today}
                            utils.logger.info(
                                "FINFLUX api URL: " + baseurl + "clients", extra=logInfo)
                            utils.logger.info(
                                "api request: " + json.dumps(payload), extra=logInfo)
                            r = requests.post(baseurl + "clients", data=json.dumps(payload), headers=utils.finflux_headers,
                                              auth=utils.finflux_auth, verify=False)
                            utils.logger.info(
                                "api response: " + json.dumps(r.json()), extra=logInfo)
                            if 'clientId' in r.json():
                                clientId = str(r.json()['clientId'])
                                inserted = db.Insert(db='mint_loan', table='mw_finflux_client_master', CUSTOMER_ID=custID,
                                                     CLIENT_ID=clientId, FULL_NAME=payload['firstname'] +
                                                     " " + payload['lastname'],
                                                     ACTIVE='0', ACTIVATION_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), CREATED_BY='Admin',
                                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), date=False, compulsory=False)
                                # stage = db.Update(db="mint_loan", table="mw_customer_login_credentials", STAGE="FINFLUX_REGISTERED",
                                #                  checkAll=False, conditions={"CUSTOMER_ID =":custID})
                                try:
                                    dob = datetime.strptime(
                                        Fields["data"][0]["DOB"], "%d-%m-%Y").strftime("%d %B %Y")
                                except:
                                    dob = None
                                try:
                                    q = Query.from_(cvalues).select(
                                        cvalues.VALUE_ID)
                                    gender = db.runQuery(q.where(cvalues.VALUE_NAME == (
                                        "Female" if Fields["data"][0]["GENDER"] == 'F' else "Male")))
                                    genderID = int(
                                        gender["data"][0]["VALUE_ID"]) if gender["data"] else None
                                    payload = {"locale": "en", "dateFormat": "dd MMMM yyyy", "dateOfBirth": dob, "maritalStatusId": 912,
                                               "genderId": genderID, "clientTypeId": 760, "clientClassificationId": 836,
                                               "mobileNo": data["LOGIN_ID"][-10:]}
                                    utils.logger.info(
                                        "FINFLUX api URL: " + baseurl + "clients/" + clientId, extra=logInfo)
                                    utils.logger.info(
                                        "api request: " + json.dumps(payload), extra=logInfo)
                                    r = requests.put(baseurl + "clients/" + clientId, verify=False, headers=utils.finflux_headers,
                                                     auth=utils.finflux_auth, data=json.dumps({k: v for k, v in payload.items() if v is not None}))
                                    utils.logger.info(
                                        "api response: " + json.dumps(r.json()), extra=logInfo)
                                    pan = Query.from_(pan).select("PAN_NO").where((pan.CUSTOMER_ID == Fields["data"][0]["CUSTOMER_ID"])
                                                                                  & (pan.ARCHIVED == 'N')).orderby(pan.CREATED_DATE, order=Order.desc)
                                    pan = db.runQuery(pan)
                                    if pan["data"]:
                                        payload = {
                                            "documentTypeId": 776, "status": 200, "documentKey": pan["data"][0]["PAN_NO"]}
                                        utils.logger.info(
                                            "FINFLUX api URL: " + baseurl + "clients/" + clientId + "/identifiers", extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload),
                                                          auth=utils.finflux_auth, headers=utils.finflux_headers, verify=False)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                    if data["AADHAR_NO"]:
                                        payload = {
                                            "documentTypeId": 3, "status": 200, "documentKey": data["AADHAR_NO"]}
                                        utils.logger.info(
                                            "FINFLUX api URL: " + baseurl + "clients/" + clientId + "/identifiers", extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        r = requests.post(baseurl + "clients/" + clientId + "/identifiers", data=json.dumps(payload),
                                                          auth=utils.finflux_auth, headers=utils.finflux_headers, verify=False)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                    bankDetails = db.runQuery(Query.from_(custbank).select(
                                        custbank.star).where(custbank.CUSTOMER_ID == custID))
                                    if bankDetails["data"]:
                                        bDetails = bankDetails["data"][0]
                                        payload = {"name": data["NAME"], "accountNumber": bDetails["ACCOUNT_NO"], "ifscCode": bDetails["IFSC_CODE"],
                                                   "mobileNumber": data["LOGIN_ID"][-10:], "bankName": bDetails["BANK_NAME"],
                                                   "bankCity": bDetails["CITY"], "branchName": bDetails["BRANCH"], "accountTypeId": 1,
                                                   "lastTransactionDate": today, "locale": "en", "dateFormat": "dd MMMM yyyy"}
                                        utils.logger.info("FINFLUX api URL: " + baseurl + "clients/" + clientId + "/bankaccountdetail",
                                                          extra=logInfo)
                                        utils.logger.info(
                                            "api request: " + json.dumps(payload), extra=logInfo)
                                        r = requests.post(baseurl + "clients/" + clientId + "/bankaccountdetail", data=json.dumps(payload),
                                                          auth=utils.finflux_auth, headers=utils.finflux_headers, verify=False)
                                        utils.logger.info(
                                            "api response: " + json.dumps(r.json()), extra=logInfo)
                                except:
                                    pass
                                if inserted:
                                    token = generate(db).AuthToken()
                                    if token["updated"]:
                                        output_dict["data"].update(
                                            {"customerClientID": clientId})
                                        output_dict["data"].update(
                                            {"error": 0, "message": success})
                                        output_dict["msgHeader"]["authToken"] = token["token"]
                                    else:
                                        output_dict["data"].update(
                                            {"error": 1, "message": errors["token"]})
                                else:
                                    output_dict["data"].update(
                                        {"error": 1, "message": errors["query"]})
                            else:
                                output_dict["data"].update({"error": 1,
                                                            "message": r.json()['defaultUserMessage'] if 'defaultUserMessage' in r.json()
                                                            else "some error occured"})
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": "Customer already registered or Aadhar KYC not done"})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": "No data found for the customer"})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
