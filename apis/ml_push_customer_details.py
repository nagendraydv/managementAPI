from __future__ import absolute_import
import falcon
import json
from random import choice
import string
import requests
from mintloan_utils import DB, generate, validate, utils, datetime
from pypika import Query, Table, JoinType, Order, functions


class pushCustomerDetailsResource:

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
        #success = "Inserted the details successfully"
        logInfo = {'api': 'customer_details'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not True:#validate.Request(api='', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(id=input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    loginCred = Table("mw_customer_login_credentials", schema="mint_loan")
                    clntProf = Table("mw_client_profile", schema="mint_loan")
                    address = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    mapping = Table("mw_header_mapping", schema ="one_family")
                    dependent_Prof = Table("mw_dependent_profile", schema="one_family")
                    conf = Table("mw_configuration", schema="mint_loan")
                    occup = Table("mw_occupation_mapping", schema="mint_loan")
                    loginID = db.runQuery(Query.from_(loginCred).select(loginCred.LOGIN_ID).where(loginCred.CUSTOMER_ID == str(input_dict["data"]["customerID"])))
                    profile_id = db.runQuery(Query.from_(clntProf).select(clntProf.PROFILE_ID).where(clntProf.CUSTOMER_ID==3))
                    dependDetails=[]
                    if profile_id!=[]:
                        profile_id=profile_id["data"]
                    else:
                        profile_id = None
                    for i in range(len(profile_id)):
                        q = Query.from_(clntProf).join(dependent_Prof, how=JoinType.left).on(clntProf.PROFILE_ID == dependent_Prof.PROFILE_ID)
                        q=q.select(clntProf.CUSTOMER_ID,clntProf.PROFILE_ID,clntProf.IS_PRIMARY,clntProf.NAME,clntProf.COMPANY_NUMBER,clntProf.CUSTOMER_DATA,clntProf.GENDER,clntProf.DATE_OF_BIRTH,
                                   clntProf.EMAIL_ID,clntProf.MONTHLY_INCOME,dependent_Prof.RELATIONSHIP).where(clntProf.PROFILE_ID==3)
                        q1 = Query.from_(address).select(address.DISTRICT,address.PIN_CODE,address.STATE,address.COUNTRY,address.HOUSE,address.STREET,address.LC).where((address.PROFILE_ID==3))# & (address.VALID=='true')
                        addData = db.runQuery(q1)["data"][0]
                        dependentDetails = db.runQuery(q)
                        depedentDetails = (dependentDetails["data"][0])
                        email = depedentDetails["EMAIL_ID"]
                        q2 = Query.from_(mapping).select(mapping.CODE_KEY)
                        #print(addData)
                        stateCode = db.runQuery(q2.where(mapping.CODE_VALUE==(addData["STATE"].title() if addData["STATE"] is not None else "Maharashtra")))["data"]
                        if stateCode!=[]:
                            stateCode = stateCode[0]["CODE_KEY"]
                        else:
                            stateCode=None
                        #print(stateCode)
                        countryCode = db.runQuery(q2.where(mapping.CODE_VALUE==(addData["COUNTRY"].title() if addData["COUNTRY"] is not None else "India")))["data"]
                        if countryCode!=[]:
                            countryCode=countryCode[0]["CODE_KEY"]
                        else:
                            countryCode=None
                        q3 = Query.from_(clntProf).select(clntProf.CUSTOMER_DATA).where(clntProf.PROFILE_ID==profile_id)
                        occupation = db.runQuery(q3)
                        occupation = db.runQuery(q2.select(occup.OCCUPATION_CODE,occup.OCCUPATION_TYPE_CODE).where(occup.CODE_VALUE=="Shop-Meat/Poultry"))["data"]
                        if occupation!=[]:
                            occupCode=occupation[0]["OCCUPATION_CODE"]
                            occupTypeCode=occupation[0]["OCCUPATION_TYPE_CODE"]
                        else:
                            occupCode="AC"
                            occupTypeCode="AC"
                        #print(countryCode)
                        depedentDetails["RELATIONSHIP"]='son'
                        #print(depedentDetails["RELATIONSHIP"])
                        Address = {"IsPermanent":True,"address": "Baner", "VernacularAddress": "", "Landmark": addData["LC"], "PinCode": addData["PIN_CODE"], 
                                                                                            "StateCode": {"Code": stateCode if stateCode is not None else "MH"},"CountryCode": {"Code": countryCode if countryCode is not None else "IN"},"Email": email}
                        relCode = db.runQuery(q2.where(mapping.CODE_VALUE==depedentDetails["RELATIONSHIP"].title()))["data"]
                        if relCode!=[]:
                            relcode=relCode[0]["CODE_KEY"]
                        else:
                            relcode=None
                        sexCode = db.runQuery(q2.where(mapping.CODE_VALUE==depedentDetails["GENDER"]))["data"]
                        if sexCode!=[]:
                            sexcode=sexCode[0]["CODE_KEY"]
                        else:
                            sexcode=None    
                        Name = depedentDetails["NAME"].split(" ") if depedentDetails["NAME"]!=None else ""
                        #print(Name)
                        firstName = Name[0] if len(Name)>0 else ""
                        middleName = Name[1] if len(Name)>1 else ""
                        lastName = Name[2] if len(Name)>2 else ""
                        personOrgNumber = ("PONDJ" +  "".join(choice(string.digits+string.ascii_uppercase+string.digits+string.digits) for _ in range(10)))
                        dependentDetails = {"FirstName": firstName if firstName!='' else "nagendraq","MiddleName": middleName if middleName!='' else "mohanji","LastName":"yadav5", "DateOfBirth":depedentDetails["DATE_OF_BIRTH"], "DependentsRelationship": {"Code": relcode if relcode is not None else "PH"},
                                                                                "PersonOrganizationNR": "PO546556313471-725437","VernacularName": depedentDetails["NAME"], "Sex": {"Code": sexcode if sexcode is not None else "M"},"ContactNumber": depedentDetails["COMPANY_NUMBER"],"Place": "Pune", 
                                                                                "Address": Address,"Income": depedentDetails["MONTHLY_INCOME"],"OccupationType": {"Code": occupTypeCode},
                                                                                            "OccupationName": {"Code":occupCode}}
                        dependDetails.append(dependentDetails)
                    payload={"DurationInMonths": 12, "DependentList": dependDetails,"NumberOfInstallmentPerPaymentFrequency": 1, "Frequency": {"Code": "Monthly"}}
                    headers={'Content-type': 'application/json'}
                    token = db.runQuery(Query.from_(conf).select(conf.CONFIG_VALUE).where(conf.CONFIG_KEY=="upliftToken"))["data"]
                    if token!=[]:
                        token=token[0]["CONFIG_VALUE"]
                    else:
                        token=""    
                    if token!='':
                        #baseurl = "https://testapism.uttamsolutions.com/api/Policy?Token=7c3489da-55f4-4fe7-9759-7de16ad34336-202105061320200043&OrganisationName=SuperMoney_Test&Language=en"
                        baseurl = "https://testapism.uttamsolutions.com/api/Policy?"
                        url = baseurl + "Token=" + token + "&OrganisationName=SuperMoney_Test&Language=en"
                        #print(url)
                        #https://testapism.uttamsolutions.com/api/MedicalHistory?Token=ec5916d7-6041-4849-87a5-58fcc451706b-202102261119372152&OrganisationName=Uplift_Local&Language=en
                        medHistUrl = "https://testapism.uttamsolutions.com/api/Medicine?"
                        BaseUrlMed = medHistUrl  + "Token=" + token + "&OrganisationName=SuperMoney_Test&Language=en"
                        #print(payload)
                        headers = {'Content-Type': 'application/json'}
                        r = requests.request("POST", url, headers=headers, data=json.dumps(payload))
                        utils.logger.info("api request: " + json.dumps(payload), extra=logInfo)
                        utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                        #r = requests.post(url, headers=headers, data=payload)
                        payload3 = {"PersonID":239,"IsICD":True,"NameOfDiesease":{"Code": "B65-B83"},"IsCurrentPresent":"True","DiseaseFromDate":"","DiseaseToDate":"","IsHereditary":"true","IsSurgery":"False","DiseaseComments":"New"}

                        #print(response.text)
                        if r.status_code==200:
                            if isinstance(r.json(),list):
                                if ((r.json()[0]["StatusCode"]=="401 - Unauthorized")): 
                                    currtime = datetime.now()#.strftime("%Y-%m-%d %H:%M:%S")
                                    updatetime = db.runQuery(Query.from_(conf).select(conf.MODIFIED_DATE).where(conf.CONFIG_KEY=="upliftToken"))["data"]
                                    if updatetime!=[]:
                                        updatetime = datetime. strptime(updatetime[0]["MODIFIED_DATE"], '%Y-%m-%d %H:%M:%S')
                                    else:
                                        updatetime = None
                                #print(updatetime)
                                #print(currtime)
                                    timeDiff=(currtime-updatetime)
                                #print(timeDiff.total_seconds())
                                    if (timeDiff.total_seconds()<5):
                                        token = db.runQuery(Query.from_(conf).select(conf.CONFIG_VALUE).where(conf.CONFIG_KEY=="upliftToken"))["data"]
                                        if token!=[]:
                                            token=token[0]["CONFIG_VALUE"]
                                        else:
                                            token=""
                                    #print(token)
                                        url = baseurl + "Token=" + token + "&OrganisationName=SuperMoney_Test&Language=en"
                                        r = requests.request("POST", url, headers=headers, data=json.dumps(payload))
                                        utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                                        if (r.json()[0]["StatusCode"]=="401 - Unauthorized") and (r.status_code==200):
                                            token = generate(db).AuthToken()
                                            output_dict["data"].update({"error": 1, "message": "something went wrong"})
                                            output_dict["msgHeader"]["authToken"] = token["token"]
                                        else:
                                            token = generate(db).AuthToken()
                                            output_dict["data"].update({"error": 0, "message": "data pushed successfully"})
                                            output_dict["msgHeader"]["authToken"] = token["token"]
                                    else:
                                        url1 = "https://dev.mintwalk.com/python/mlUpliftGenerateToken"
                                        payload2 = {'data': {},'msgHeader': {'authToken': '','authLoginID': 'admin@mintloan.com','timestamp': 1583748704, 'ipAddress': '127.0.1'}}
                                    #payload="{\n\n    \"data\":{\n    },\n    \"msgHeader\":{\n        \"authToken\":\"\",\n        \"authLoginID\":\"admin@mintloan.com\",\n        \"timestamp\":1583748704,\n        \"ipAddress\":\"127.0.1\"\n    }\n\n}"
                                        headers = {'Content-Type': 'application/json'}
                                        response = requests.request("POST", url1, headers=headers, data=json.dumps(payload2))
                                        if response.status_code==200:
                                            token = db.runQuery(Query.from_(conf).select(conf.CONFIG_VALUE).where(conf.CONFIG_KEY=="upliftToken"))["data"]
                                            if token!=[]:
                                                token=token[0]["CONFIG_VALUE"]
                                            else:
                                                token=""
                                            r = requests.post(url , data=json.dumps(payload),headers=headers, verify=False)
                                            utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                                        if (r.status_code==200):
                                            token = generate(db).AuthToken()
                                            output_dict["data"].update({"error": 0, "message": "data pushed successfully"})
                                            output_dict["msgHeader"]["authToken"] = token["token"]
                                        else:
                                            token = generate(db).AuthToken()
                                            output_dict["data"].update({"error": 0, "message": "token not generated successfully"})
                                            output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                token = generate(db).AuthToken()
                                output_dict["data"].update({"error": 0, "message": "data pushed successfully"})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            token = generate(db).AuthToken()
                            output_dict["data"].update({"error": 0, "message": "something went wrong"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        #res=r.json()
                        """
                        if True:#res["data"]["successFlag"]:
                            output_dict["data"].update({"error": 0, "message": "data pushed successfully"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                if junk:
                                    output_dict["data"].update({"error": 0, "message": "kyc edited successfully"})
                                    output_dict["msgHeader"]["authToken"] = token["token"]
                                else:
                                    output_dict["data"].update({"error": 0, "message": "kyc not edited successfully"})
                                    output_dict["msgHeader"]["authToken"] = token["token"]
                        """
                resp.body = json.dumps(output_dict)
                utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",extra=logInfo, exc_info=True)
            raise 
