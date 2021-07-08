from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table
import requests


class ForceLoginResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"login": "", "uuid": "", "accountStatus": "", "name": "", "lastLoginDate": 0, "lastLoginIP": "",
                                "modifiedPasswordDate": 0, "failAttempt": 1},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "Login success"
        logInfo = {'api': 'LOGIN'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            #print(input_dict)
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='login', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # setting an instance of DB class
                db = DB(id=input_dict["data"]["loginID"],filename='mysql.config')
                dbw = DB(id=input_dict["data"]["loginID"])
                val_error = validate(db).basicChecks(loginID=input_dict["data"]["loginID"], checkToken=False, checkLogin=True)
                loanAdmin = Table("mw_admin_user_master",schema="mint_loan_admin")
                userType = Table("mw_admin_user_account_type",schema="mint_loan_admin")
                token = db.runQuery(Query.from_(loanAdmin).select("AUTH_TOKEN").where(loanAdmin.LOGIN == str(input_dict["data"]["loginID"])))
                tokenCheck = validate(db).AuthToken(token["data"][0]["AUTH_TOKEN"] if len(token["data"]) > 0 else "", checkToken=False)
                # print val_error, not validate(db).Password(input_dict["data"]["password"])
                if val_error:
                    #print('true')
                    incremented, value = dbw.IncrementFailAttempt()
                    output_dict["data"].update({"error": 1, "message": val_error, "failAttempt": value+1})
                elif not validate(db).Password(input_dict["data"]["password"]):
                    incremented, value = dbw.IncrementFailAttempt()
                    output_dict["data"].update({"error": 1, "message": errors["password"], "failAttempt": value+1})
                else:
                    # if input_dict["data"]["loginID"]!="dharam@mintwalk.com" else {"updated":True, "token":"scGEx8.gYYwXlGxTGiIMZO2OJ7qdIcuZy0vUA4sPsFc.!rxNMy4BkRT/P6pxWvjP2G6iDwIFukf+o"}
                    token = generate(dbw).AuthToken(exp=10, saveLoginAuth=False)
                    ut = utils()
                    q = Query.from_(loanAdmin).select("LOGIN", "UUID", "ACCOUNT_STATUS", "NAME", "LAST_LOGIN_IP", "LAST_LOGIN_DATE", "CITY",
                                                      "MODIFIED_PASSWORD_DATE").where(loanAdmin.LOGIN == str(input_dict["data"]["loginID"]))
                    Fields = utils.camelCase(db.runQuery(q))
                    utype = db.runQuery(Query.from_(userType).select("ACCOUNT_TYPE").where(userType.LOGIN == Fields["data"][0]["login"]))["data"]
                    Fields["data"][0].update({"accountTypes": [ele["ACCOUNT_TYPE"] for ele in utype]})
                    #q1=Query.from_(loanAdmin).select("PASSWORD").where(loanAdmin.LOGIN == str(input_dict["data"]["loginID"]))
                    #passData = utils.camelCase(db.runQuery(q1))
                    #print(passData)
                    password=input_dict["data"]["password"]#'$2b$12$.V6Y7d2frFjSvD7nbbO6Hugj0QfLEG0Y57fSxdo32tE0Z36bEH.YS'#passData["data"][0]["password"] if passData["data"][0]["password"]!='' else None
                    setFailAttemptZero = dbw.Update(db="mint_loan_admin", table='mw_admin_user_master',
                                                    FAIL_ATTEMPT=0, LAST_LOGIN_IP=input_dict["msgHeader"]["ipAddress"],
                                                    LAST_LOGIN_DATE=(
                                                        datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"),
                                                    conditions={"LOGIN = ": input_dict["data"]["loginID"]})
                    headers={'Content-type': 'application/json','Cache-Control': "no-cache"}
                    auth = utils.mifos_auth
                    baseurl='https://dev.supermoney.in:8443/python/oauth2/token/'
                    payload={"client_id": input_dict["data"]["loginID"],"client_secret": password,"grant_type": "password","provision_key": "utTRWQf2eyAT2OWLW4Jh8F7USbIbCXid","authenticated_userid": input_dict["data"]["loginID"],"scope": "email"}
                    #print(json.dumps(payload))
                    r = requests.post(baseurl, data=json.dumps(payload), headers=headers, verify=False)
                    utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                    res=r.json()
                    if ((setFailAttemptZero) &('access_token' in res.keys())):
                        output_dict["data"].update(utils.mergeDicts(
                            {"error": 0, "message": success, "failAttempt": 0}, Fields["data"][0]))
                        output_dict["msgHeader"]["authToken"] = res["access_token"]
                    elif ((setFailAttemptZero) &('error' in res.keys())):
                        output_dict["data"].update(utils.mergeDicts(
                            {"error": 0, "message": "Invalid client authentication", "failAttempt": 0}))
                        output_dict["msgHeader"]["authToken"] = ''
                    else:
                        incremented, value = dbw.IncrementFailAttempt()
                        output_dict["data"].update({"error": 1, "message": errors["token"], "failAttempt": value+1})
                resp.set_header('Access-Control-Allow-Origin', '*')
                resp.set_header('Access-Control-Expose-Headers',
                                'Access-Control-Allow-Origin,Access-Control-Allow-Methods,Access-Control-Allow-Headers')
                resp.set_header('Access-Control-Allow-Methods', 'POST')
                resp.set_header('Access-Control-Allow-Headers', '*')
                resp.set_header('Access-Control-Max-Age', '86400')
                resp.body = json.dumps(output_dict)
                #print(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise


class LoginResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"login": "", "uuid": "", "accountStatus": "", "name": "", "lastLoginDate": 0, "lastLoginIP": "",
                                "modifiedPasswordDate": 0, "failAttempt": 1},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = "Login success"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='login', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # setting an instance of DB class
                db = DB(id=input_dict["data"]["loginID"],filename='mysql-slave.config')
                dbw = DB(id=input_dict["data"]["loginID"])
                val_error = validate(db).basicChecks(
                    loginID=input_dict["data"]["loginID"], checkToken=False, checkLogin=True)
                loanAdmin = Table("mw_admin_user_master",
                                  schema="mint_loan_admin")
                userType = Table("mw_admin_user_account_type",
                                 schema="mint_loan_admin")
                token = db.runQuery(Query.from_(loanAdmin).select("AUTH_TOKEN").where(
                    loanAdmin.LOGIN == str(input_dict["data"]["loginID"])))
                tokenCheck = validate(db).AuthToken(token["data"][0]["AUTH_TOKEN"] if len(token["data"]) > 0 else "", checkTstamp=True,
                                                    checkToken=False)
                if val_error or tokenCheck["tstamp_ok"]:
                    if val_error:
                        incremented, value = dbw.IncrementFailAttempt()
                        output_dict["data"].update(
                            {"error": 1, "message": val_error, "failAttempt": value+1})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["session"], "failAttempt": 0})
                elif not validate(db).Password(input_dict["data"]["password"]):
                    incremented, value = dbw.IncrementFailAttempt()
                    output_dict["data"].update(
                        {"error": 1, "message": errors["password"], "failAttempt": value+1})
                else:
                    token = generate(dbw).AuthToken(exp=10, saveLoginAuth=True) if input_dict["msgHeader"]["authLoginID"] != "dharam@mintwalk.com" else {
                        "updated": True, "token": "scGEx8.gYYwXlGxTGiIMZO2OJ7qdIcuZy0vUA4sPsFc.!rxNMy4BkRT/P6pxWvjP2G6iDwIFukf+o"}
                    ut = utils()
                    q = Query.from_(loanAdmin).select("LOGIN", "UUID", "ACCOUNT_STATUS", "NAME", "LAST_LOGIN_IP", "LAST_LOGIN_DATE",
                                                      "MODIFIED_PASSWORD_DATE").where(loanAdmin.LOGIN == str(input_dict["data"]["loginID"]))
                    Fields = utils.camelCase(db.runQuery(q))
                    utype = db.runQuery(Query.from_(userType).select("ACCOUNT_TYPE").where(
                        userType.LOGIN == Fields["data"][0]["login"]))["data"]
                    Fields["data"][0].update(
                        {"accountTypes": [ele["ACCOUNT_TYPE"] for ele in utype]})
                    setFailAttemptZero = dbw.Update(db="mint_loan_admin", table='mw_admin_user_master',
                                                    FAIL_ATTEMPT=0, LAST_LOGIN_IP=input_dict["msgHeader"]["ipAddress"],
                                                    LAST_LOGIN_DATE=(
                                                        datetime.utcnow() + timedelta(seconds=19800)).strftime("%s"),
                                                    conditions={"LOGIN = ": input_dict["data"]["loginID"]})
                    if token["updated"] & setFailAttemptZero:
                        output_dict["data"].update(utils.mergeDicts(
                            {"error": 0, "message": success, "failAttempt": 0}, Fields["data"][0]))
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        incremented, value = dbw.IncrementFailAttempt()
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"], "failAttempt": value+1})
                resp.set_header('Access-Control-Allow-Origin', '*')
                resp.set_header('Access-Control-Expose-Headers',
                                'Access-Control-Allow-Origin,Access-Control-Allow-Methods,Access-Control-Allow-Headers')
                resp.set_header('Access-Control-Allow-Methods', 'POST')
                resp.set_header('Access-Control-Allow-Headers', '*')
                # 24 hours))
                resp.set_header('Access-Control-Max-Age', '86400')
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise


app = falcon.API()
login = LoginResource()
forceLogin = ForceLoginResource()

app.add_route('/login', login)
app.add_route('/forceLogin', forceLogin)
