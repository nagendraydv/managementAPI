

from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils
from pypika import Query, Table, functions, Order, JoinType


class AccountDeactivateAdminResource:

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
        success = "Account deactivated successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='deactivateAccountAdmin', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                loginID = input_dict["data"]["loginID"]
                adminMaster = Table("mw_admin_user_master",schema="mint_loan_admin")
                db = DB(input_dict["data"]["loginID"])
                userType = Table("mw_admin_user_account_type",schema="mint_loan_admin")
                authLoginID = input_dict['msgHeader']['authLoginID']
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                accType = db.runQuery(Query.from_(userType).select(userType.ACCOUNT_TYPE).where(userType.LOGIN==authLoginID))
                dbAccStatus = db.runQuery(Query.from_(adminMaster).select(adminMaster.ACCOUNT_STATUS).where(adminMaster.LOGIN==loginID))
                #print(dbAccStatus['data'][0]['ACCOUNT_STATUS'])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                #elif validate(db).Password(input_dict["data"]["oldPassword"]):
                if dbAccStatus['data'][0]['ACCOUNT_STATUS'] == "A":
                    if accType['data'][0]['ACCOUNT_TYPE'] == "SUPERUSER":
                        update = db.Update(ACCOUNT_STATUS='D', conditions={"LOGIN = ":loginID})
                        #print(update)
                        if update:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["msgHeader"]["authToken"] = token["token"]
                                output_dict["data"].update({"error": 0, "message": success})
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            output_dict["data"].update({"error":1,"message":"Account not updated"})
                    else:
                        output_dict["data"].update({"error":1,"message":"You are not authenticate to deactivate the account"})
                else:
                    output_dict["data"].update({"error":1,"message":"account is already deactivated"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
