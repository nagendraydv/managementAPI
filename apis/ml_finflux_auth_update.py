from __future__ import absolute_import
import falcon
import json
import time
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType, Order


class FinfluxOauthUpdateResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "Finflux Oauth updated"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            # not validate.Request(api='stageSync', request=input_dict):
            if False:
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
                    mwConfiguration = Table(
                        "mw_configuration", schema="mint_loan")
                    configCredentials = Query.from_(mwConfiguration).select("CONFIG_KEY", "CONFIG_VALUE").where(
                        mwConfiguration.CONFIG_KEY.isin(["finfluxPassword", "finfluxUserName"]))
                    dataResults = db.runQuery(configCredentials)['data']
                    finfluxUserName = [
                        d['CONFIG_VALUE'] for d in dataResults if d['CONFIG_KEY'] == 'finfluxUserName'][0]
                    finfluxUserPassword = [
                        d['CONFIG_VALUE'] for d in dataResults if d['CONFIG_KEY'] == 'finfluxPassword'][0]
                    # OAuthTokenURL='https://chaitanya.confluxcloud.com/fineract-provider/api/oauth/token'
                    OAuthTokenURL = 'https://chaitanya.finflux.io/fineract-provider/api/oauth/token'
                    payload = {"client_id": "community-app", "client_secret": "123", "grant_type": "password",
                               "isPasswordEncrypted": "false", "password": finfluxUserPassword, "username": finfluxUserName}
                    refreshPayload = {"client_id": "community-app", "client_secret": "123", "grant_type": "refresh_token",
                                      "isPasswordEncrypted": "false", "password": finfluxUserPassword, "username": finfluxUserName}
                    r = requests.post(OAuthTokenURL, data=json.dumps(
                        payload), headers=utils.finflux_headers["CHAITANYA"])
                    result = r.json() if r.status_code==200 else {"expires_in":3000, "access_token":"", "refresh_token":""}
                    if result['expires_in'] < 2000:
                        refreshPayload['refresh_token'] = result['refresh_token']
                        r = requests.post(OAuthTokenURL, data=json.dumps(
                            refreshPayload), headers=utils.finflux_headers["CHAITANYA"])
                        refreshTokenResults = r.json()
                        q = Query.update(mwConfiguration).set('CONFIG_VALUE', refreshTokenResults['access_token']).set(
                            'MODIFIED_BY', 'CRON').set('MODIFIED_DATE', datetime.now()).where(mwConfiguration.CONFIG_KEY == 'FinfluxAccessToken')
                        db.dictcursor.execute(db.pikastr(q))
                        db.mydb.commit()
                        q = Query.update(mwConfiguration).set('CONFIG_VALUE', refreshTokenResults['expires_in']).set('MODIFIED_BY', 'CRON').set(
                            'MODIFIED_DATE', datetime.now()).where(mwConfiguration.CONFIG_KEY == 'FinfluxAccessTokenExpiry')
                        db.dictcursor.execute(db.pikastr(q))
                        db.mydb.commit()
                        q = Query.update(mwConfiguration).set('CONFIG_VALUE', refreshTokenResults['refresh_token']).set(
                            'MODIFIED_BY', 'CRON').set('MODIFIED_DATE', datetime.now()).where(mwConfiguration.CONFIG_KEY == 'FinfluxRefreshToken')
                        db.dictcursor.execute(db.pikastr(q))
                        db.mydb.commit()
                    else:
                        q = Query.update(mwConfiguration).set('CONFIG_VALUE', result['access_token']).set('MODIFIED_BY', 'CRON').set(
                            'MODIFIED_DATE', datetime.now()).where(mwConfiguration.CONFIG_KEY == 'FinfluxAccessToken')
                        db.dictcursor.execute(db.pikastr(q))
                        db.mydb.commit()
                        q = Query.update(mwConfiguration).set('CONFIG_VALUE', result['expires_in']).set('MODIFIED_BY', 'CRON').set(
                            'MODIFIED_DATE', datetime.now()).where(mwConfiguration.CONFIG_KEY == 'FinfluxAccessTokenExpiry')
                        db.dictcursor.execute(db.pikastr(q))
                        db.mydb.commit()
                        q = Query.update(mwConfiguration).set('CONFIG_VALUE', result['refresh_token']).set('MODIFIED_BY', 'CRON').set(
                            'MODIFIED_DATE', datetime.now()).where(mwConfiguration.CONFIG_KEY == 'FinfluxRefreshToken')
                        db.dictcursor.execute(db.pikastr(q))
                        db.mydb.commit()
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["msgHeader"]["authToken"] = token["token"]
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
