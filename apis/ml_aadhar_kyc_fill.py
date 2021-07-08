from __future__ import absolute_import
import falcon
import json
import random
import requests
from mintloan_utils import DB, generate, validate, utils, datetime
from pypika import Query, Table, JoinType, Order, functions


class AadharFillResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {}, "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "Inserted the details successfully"
        logInfo = {'api': 'aadharFill'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
            # print input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='aadharFill', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                #resp.set_header('Access-Control-Allow-Origin', '*')
                resp.body = json.dumps(output_dict)
            else:
                # setting an instance of DB class
                db = DB(id=input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    #resp.set_header('Access-Control-Allow-Origin', '*')
                    resp.body = json.dumps(output_dict)
                else:
                    loginCred = Table("mw_customer_login_credentials", schema="mint_loan")
                    clientmaster = Table("mw_finflux_client_master", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    #conf = Table("mw_configuration", schema="mint_loan_admin")
                    #conf2 = Table("mw_configuration", schema="mint_loan")
                    cvalues = Table("mw_finflux_attribute_code_values", schema="mint_loan")
                    loginID = db.runQuery(Query.from_(loginCred).select(loginCred.LOGIN_ID).where(
                        loginCred.CUSTOMER_ID == str(input_dict["data"]["customerID"])))
                    #print(loginID)
                    #baseurl = db.runQuery(Query.from_(conf).select(
                    #    "CONFIG_VALUE").where(conf.CONFIG_KEY == "FINFLUX_URL"))
                    #baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    #params = db.runQuery(Query.from_(conf2).select("CONFIG_KEY", "CONFIG_VALUE").where(
                    #    conf2.CONFIG_KEY.isin(["FinfluxAccessToken"])))
                    #params = {ele["CONFIG_KEY"]: ele["CONFIG_VALUE"]
                    #          for ele in params["data"]}
                    baseurl='https://dev.mintwalk.com/tomcat/mintLoan/mintloan/storeAadharKycDetails'
                    #headers = utils.mergeDicts(utils.finflux_headers["CHAITANYA"], {
                                               #"Authorization": "bearer " + params["FinfluxAccessToken"]})
                    headers={'Content-type': 'application/json'}
                    auth = utils.mifos_auth
                    if loginID["data"]:
                        utils.logger.debug(
                            "Inserting new kyc data\n", extra=logInfo)
                        loginID=str(loginID["data"][0]["LOGIN_ID"])
                        data = input_dict["data"]
                        payload= {"data":{"dist":data["district"],"gender":data["gender"],"house":data["house"],"name":data["name"],"pc":data["pinCode"],"po":data["postOffice"],"state":data["state"],"uid":data["aadharNo"],"vtc":data["villageTownCity"],"yob":data["dob"]},
                                  "deviceFPmsgHeader":{"clientIPAddress":"25.73.165.160","connectionMode":"4G","country":"United Kingdom","deviceManufacturer":"samsung","deviceModelNo":"SM-A305F",
                                                       "dualSim":False,"imeiNo":"50bc7393895106aa","latitude":"13.070549201220274","longitude":"80.23136844858527","nwProvider":"xxxxxxxx","osName":"Android",
                                                       "osVersion":29,"timezone":"Asia/Kolkata","versionCode":"121","versionName":"6.3.2"},
                                  "msgHeader":{"authToken":"413f2449d3d6bc48b542a5c6adb4d433481dbgb8","channelType":"M","consumerId":"407","deviceId":"50bc7393895106aa","loginId":loginID,"source":"androidDirect"}
                                  }
                        r = requests.post(baseurl , data=json.dumps(payload),headers=headers,auth=auth, verify=False)
                    #token = generate(db).AuthToken()
                        utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                        #utils.logger.info(r, extra=logInfo)
                        res=r.json()
                        token = generate(db).AuthToken()
                        if res["data"]["successFlag"]:
                            output_dict["data"].update({"error": 0, "message": "data inserted successfully"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            """
                            insert_dict = {"CUSTOMER_ID": data["customerID"],"NAME": data["name"],
                                           "DOB": data["dob"], "GENDER": data["gender"], "HOUSE": data["house"], "STREET": data["street"],
                                           "CO": data["careOf"], "LM": data["landMark"], "LC": data["locality"], "VTC": data["villageTownCity"],
                                           "DISTRICT": data["district"], "SUB_DISTRICT": data["subDistrict"], "PIN_CODE": data["pinCode"],
                                           "POST_OFFICE": data["postOffice"], "STATE": data["state"], "COUNTRY": data["country"],
                                           "CREATED_BY": input_dict["msgHeader"]["authLoginID"],
                                           "MOTHERS_NAME": data["mothersName"] if "mothersName" in data else None,
                                           "FATHERS_NAME": data["fathersName"] if "fathersName" in data else None,
                                           "HOUSE_CORRESPONDENCE": data["houseCorr"] if "houseCorr" in data else None,
                                           "STREET_CORRESPONDENCE": data["streetCorr"] if "streetCorr" in data else None,
                                           "CO_CORRESPONDENCE": data["careOfCorr"] if "careOfCorr" in data else None,
                                           "LM_CORRESPONDENCE": data["landMarkCorr"] if "landMarkCorr" in data else None,
                                           "LC_CORRESPONDENCE": data["localityCorr"] if "localityCorr" in data else None,
                                           "VTC_CORRESPONDENCE": data["villageTownCityCorr"] if "villageTownCityCorr" in data else None,
                                           "DISTRICT_CORRESPONDENCE": data["districtCorr"] if "districtCorr" in data else None,
                                           "SUB_DISTRICT_CORRESPONDENCE": data["subDistrictCorr"] if "subDistrictCorr" in data else None,
                                           "PIN_CODE_CORRESPONDENCE": data["pinCodeCorr"] if "pinCodeCorr" in data else None,
                                           "POST_OFFICE_CORRESPONDENCE": data["postOfficeCorr"] if "postOfficeCorr" in data else None,
                                           "STATE_CORRESPONDENCE": data["stateCorr"] if "stateCorr" in data else None,
                                           "COUNTRY_CORRESPONDENCE": data["countryCorr"] if "countryCorr" in data else None,
                                           "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                            junk = db.Insert(db="mint_loan", table="mw_aadhar_kyc_details",
                                             compulsory=False, date=False, **insert_dict)
                            #junk = db.Update(db="mint_loan", table="mw_aadhar_status", conditions={"CUSTOMER_ID = ": data["customerID"]},
                                             #ARCHIVED="Y")
                            utils.logger.debug(
                                "Inserting aadhar status\n", extra=logInfo)
                            junk = db.Insert(db="mint_loan", table="mw_aadhar_status", compulsory=False, date=False,
                                             TRANSACTION_ID=str(random.getrandbits(50)), CUSTOMER_ID=data["customerID"], AADHAR_NO=data["aadharNo"],
                                             ARCHIVED="N", CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                             CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                            utils.logger.debug(
                                "Updating aadhar data in finflux\n", extra=logInfo)
                            payload = {"documentTypeId": 3, "status": 200,
                                       "documentKey": data["aadharNo"]}
                            clientID = Query.from_(clientmaster).select("CLIENT_ID").where(
                                clientmaster.CUSTOMER_ID == str(input_dict["data"]["customerID"]))
                            clientID = db.runQuery(clientID.where(
                                clientmaster.LENDER == 'CHAITANYA'))["data"]
                            if clientID:
                                r = requests.post(baseurl + "clients/" + clientID[0]["CLIENT_ID"] + "/identifiers", data=json.dumps(payload),
                                                  headers=headers, verify=False)
                            else:
                                utils.logger.debug(
                                    "Updating kyc data\n", extra=logInfo)
                            """
                            data = input_dict["data"]
                            insert_dict = {"NAME": data["name"], "DOB": data["dob"], "GENDER": data["gender"],
                                           "HOUSE": data["house"], "STREET": data["street"], "CO": data["careOf"], "LM": data["landMark"],
                                           "LC": data["locality"], "VTC": data["villageTownCity"], "DISTRICT": data["district"],
                                           "SUB_DISTRICT": data["subDistrict"], "PIN_CODE": data["pinCode"], "POST_OFFICE": data["postOffice"],
                                           "STATE": data["state"], "COUNTRY": data["country"], "CREATED_BY": input_dict["msgHeader"]["authLoginID"],
                                           "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                           "MOTHERS_NAME": data["mothersName"] if "mothersName" in data else None,
                                           "FATHERS_NAME": data["fathersName"] if "fathersName" in data else None,
                                           "HOUSE_CORRESPONDENCE": data["houseCorr"] if "houseCorr" in data else None,
                                           "STREET_CORRESPONDENCE": data["streetCorr"] if "streetCorr" in data else None,
                                           "CO_CORRESPONDENCE": data["careOfCorr"] if "careOfCorr" in data else None,
                                           "LM_CORRESPONDENCE": data["landMarkCorr"] if "landMarkCorr" in data else None,
                                           "LC_CORRESPONDENCE": data["localityCorr"] if "localityCorr" in data else None,
                                           "VTC_CORRESPONDENCE": data["villageTownCityCorr"] if "villageTownCityCorr" in data else None,
                                           "DISTRICT_CORRESPONDENCE": data["districtCorr"] if "districtCorr" in data else None,
                                           "SUB_DISTRICT_CORRESPONDENCE": data["subDistrictCorr"] if "subDistrictCorr" in data else None,
                                           "PIN_CODE_CORRESPONDENCE": data["pinCodeCorr"] if "pinCodeCorr" in data else None,
                                           "POST_OFFICE_CORRESPONDENCE": data["postOfficeCorr"] if "postOfficeCorr" in data else None,
                                           "STATE_CORRESPONDENCE": data["stateCorr"] if "stateCorr" in data else None,
                                           "COUNTRY_CORRESPONDENCE": data["countryCorr"] if "countryCorr" in data else None}
                            junk = db.Update(db="mint_loan", table="mw_aadhar_kyc_details", checkAll=False,
                                             conditions={"CUSTOMER_ID=": data["customerID"]}, **insert_dict)
                            utils.logger.debug("Updating dob, gender and name in finflux\n", extra=logInfo)
                            """
                            clientID = Query.from_(clientmaster).select("CLIENT_ID").where(clientmaster.CUSTOMER_ID == str(input_dict["data"]["customerID"]))
                            clientID = db.runQuery(clientID.where(
                                clientmaster.LENDER == 'CHAITANYA'))["data"]
                            if clientID:
                                try:
                                    dob = datetime.strptime(
                                        data["dob"], "%d-%m-%Y").strftime("%d %B %Y")
                                except:
                                    dob = None
                                q = Query.from_(cvalues).select(cvalues.VALUE_ID)
                                gender = db.runQuery(q.where(cvalues.VALUE_NAME == (
                                    "Female" if data["gender"] == 'F' else "Male")))
                                genderID = int(
                                    gender["data"][0]["VALUE_ID"]) if gender["data"] else None
                            """
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                if junk:
                                    output_dict["data"].update(
                                            {"error": 0, "message": "kyc edited successfully"})
                                    output_dict["msgHeader"]["authToken"] = token["token"]
                                else:
                                    output_dict["data"].update({"error": 0, "message": "kyc not edited successfully"})
                                    output_dict["msgHeader"]["authToken"] = token["token"]
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            raise 
