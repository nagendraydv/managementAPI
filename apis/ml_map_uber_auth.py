from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
import string
import os
import subprocess
import xlrd
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table, functions
import requests
import inspect


class MapUberAuthResource:

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"message": "successfully mapped"}}
        errors = utils.errors
        success = "Mapped successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        if (not validate.Request(api='mapUberAuth', request=input_dict)):
            output_dict["data"].update({"error": 1, "message": errors["json"]})
            resp.body = json.dumps(output_dict, encoding='unicode-escape')
        else:
            db = DB()  # input_dict["msgHeader"]["authLoginID"])
            gen = generate(db)
            table = "mw_python_uber_api_integration_log"
            gen.DBlog(table=table, logFrom="mapUberAuth", lineNo=inspect.currentframe(
            ).f_lineno, logMessage="Request:" + json.dumps(input_dict))
            val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                 checkLogin=True, checkToken=False)
            if False:
                output_dict["data"].update({"error": 1, "message": val_error})
                resp.body = json.dumps(output_dict)
            else:
                try:
                    usession = Table(
                        "mw_company_login_session", schema="mint_loan")
                    prof = Table("mw_profile_info", schema="mw_company_3")
                    custProf = Table("mw_client_profile", schema="mint_loan")
                    unregData = Table(
                        "mw_unregistered_data_dump", schema="mw_company_3")
                    update = db.Update(db="mw_company_3", table="mw_unregistered_data_dump", CUSTOMER_ID=str(input_dict["data"]["custID"]),
                                       conditions={"PARTNER_ID=": input_dict["data"]["driverID"], "CUSTOMER_ID IS NULL": ""})
                    url = "https://login.uber.com/oauth/v2/authorize?response_type=code&client_id=U4XCFbyEXwwQ0TF0oLveLcXz-Vo_ddkn&"
                    url += "scope=partner.accounts+partner.payments+partner.rewards+partner.trips&"
                    url += "redirect_uri=https%3A%2F%2Fsmart-backend.mintwalk.com%2FmlGetUberAuth&state=D1MOFF"
                    inserted = db.Insert(db="mint_loan", table="mw_company_login_session", compulsory=False, date=False, COMPANY_ID="3",
                                         CUSTOMER_ID=str(input_dict["data"]["custID"]), STATE='D1MOFF', URL_GENERATED=url, LOGIN_SUCCESS="1",
                                         CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    q = db.runQuery(Query.from_(unregData).select(unregData.star).where(
                        unregData.PARTNER_ID == input_dict["data"]["driverID"]))["data"]
                    gen.DBlog(table=table, logFrom="mapUberAuth", lineNo=inspect.currentframe(
                    ).f_lineno, logMessage="UnregData after mapping : " + json.dumps(q[0] if q else {}))
                    custData = Query.from_(custProf).select(custProf.CUSTOMER_DATA).where(
                        custProf.CUSTOMER_ID == input_dict["data"]["custID"])
                    custData = db.runQuery(custData)["data"]
                    try:
                        custData = json.loads(
                            custData[0]["CUSTOMER_DATA"]) if custData else {}
                    except:
                        custData = {}
                    if (custData) and len(q) > 0:
                        nDaysDriving = ((datetime.now()-datetime.strptime(q[0]["FIRST_TRIP_WEEK"], "%Y-%m-%d")).days if q[0]["FIRST_TRIP_WEEK"]
                                        else 200)
                        custData.update({"userCategory": q[0]["TIER"], "experience": int(nDaysDriving/30),
                                         "monthlyIncome": int(q[0]["THREE_WEEK_AVERAGE"] if q[0]["THREE_WEEK_AVERAGE"] else 0)})
                        db.Update(db="mint_loan", table="mw_client_profile", conditions={"CUSTOMER_ID=": str(input_dict["data"]["custID"])},
                                  CUSTOMER_DATA=json.dumps(custData), debug=True)
                    #token = generate(db).AuthToken(update=False)
                    if True:  # token["updated"]:
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = ""
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                    resp.body = json.dumps(
                        output_dict, encoding='unicode-escape')
                except:
                    raise
