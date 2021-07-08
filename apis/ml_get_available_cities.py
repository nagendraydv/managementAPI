from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType


class GetAvailableCitiesResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"citiesAvailable": []}}
        errors = utils.errors
        success = "Available cities laoded successfully"
        #logInfo = {'api': 'customerDetails'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            #utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            # not validate.Request(api='custDetails', request=input_dict):
            if False:
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # , filename='mysql-slave.config') # setting an instance of DB class
                db = DB(input_dict["msgHeader"]["authLoginID"])
                #dbw = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])  # change from dbw to db
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    comp = Table("mw_company_master", schema="mint_loan")
                    mapp = Table("mw_company_city_mapping", schema="mint_loan")
                    citym = Table("mw_city_master", schema="mint_loan")
                    q = Query.from_(comp).join(mapp, how=JoinType.left).on(
                        comp.SHORT_NAME == mapp.COMPANY_SHORT_NAME).join(citym, how=JoinType.left)
                    cities = db.runQuery(q.on(citym.CITY_ID == mapp.CITY_ID).select(
                        citym.CITY).where(comp.DISPLAY_NAME == input_dict["data"]["company"]))
                    cities = [ele["CITY"]
                              for ele in cities["data"] if ele["CITY"]]
                    #token = generate(db).AuthToken()
                    if True:  # token["updated"]:
                        output_dict["data"]["citiesAvailable"] = cities
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                #utils.logger.debug("Response: " + json.dumps(output_dict["msgHeader"]) + "\n", extra=logInfo)
                db._DbClose_()
                # dbw._DbClose_()   change
        except Exception as ex:
            #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
