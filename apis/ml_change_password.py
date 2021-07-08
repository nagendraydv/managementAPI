from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils


class ChangePasswordResource:

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
        success = "Password changed successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='changePassword', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                elif validate(db).Password(input_dict["data"]["oldPassword"]):
                    newPassword = generate(db).PasswordHash(input_dict["data"]["newPassword"].encode('utf-8'),update=True)
                    #print(newPassword)
                    if newPassword['password_ok'] and newPassword['updated']:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["msgHeader"]["authToken"] = token["token"]
                            output_dict["data"].update({"error": 0, "message": success})
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["strength"]})
                else:
                    output_dict["data"].update({"error": 1, "message": errors["password"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
