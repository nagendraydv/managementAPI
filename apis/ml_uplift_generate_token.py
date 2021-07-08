from __future__ import absolute_import
import falcon
import json
import random
import requests
from mintloan_utils import DB, generate, validate, utils, datetime
from pypika import Query, Table, JoinType, Order, functions


class upliftGenerateTokenResource:

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
        logInfo = {'api': 'GenerateToken'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not True:#validate.Request(api='generateToken', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(id=input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    #conf = Table("mw_configuration", schema="mint_loan")
                    baseurl='https://testapism.uttamsolutions.com/api/Login'
                    headers = {'Content-Type': 'application/json'}
                    payload = {"Username": "SuperMoney", "Password": "123@1FamUpSM", "OrganisationName": "SuperMoney_Test", "Language": "en"}
                    r = requests.post(baseurl , data=json.dumps(payload),headers=headers, verify=False)
                    utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                    if r.status_code ==200:
                        res=r.json()
                        #print(res)
                        indict={"CONFIG_VALUE":str(res[1]["LoginDetails"]["Token"]),"MODIFIED_DATE":str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}
                        updated = db.Update(db="mint_loan", table="mw_configuration", checkAll=False, CONFIG_VALUE=str(res[1]["LoginDetails"]["Token"]),conditions={"CONFIG_KEY=": upliftToken})
                        token = generate(db).AuthToken()
                        if updated:
                            output_dict["data"].update({"error": 0, "message": "token generated and updated successfully"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            token = generate(db).AuthToken()
                            output_dict["data"].update({"error": 0, "message": "token generated successfully"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": "something went wrong"})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                resp.body = json.dumps(output_dict)
                utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",extra=logInfo, exc_info=True)
            raise 
