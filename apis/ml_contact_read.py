from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
from mintloan_utils import DB, generate, validate, utils


class ContactReadResource:

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
        success = "Success"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            print(input_dict)
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if False:  # not validate.Request(api='login', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB()  # setting an instance of DB class
                val_error = False  # validate(db).basicChecks(checkToken=False)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": errors["session"], "failAttempt": 0})
                    resp.body = json.dumps(output_dict)
                else:
                    for ele in input_dict["data"]["contactinfos"]:
                        if ele:
                            ex = db.Query(db="sms_analytics", primaryTable="mw_contact_dump", fields={"A": ["*"]},
                                          conditions={"A.NAME=": ele["name"], "A.DEVICE_ID=": input_dict["messageHeader"]["deviceId"]})["data"]
                            ex = (ex[0]["MOBILE_NUMBER1"] if ex[0]
                                  ["MOBILE_NUMBER1"] else None) if ex else None
                            try:
                                e1, e2, e3 = (("", "", "") if len(ele["email"]) == 0 else (ele["email"][0], "", "") if len(ele["email"]) == 1 else
                                              (ele["email"][0], ele["email"][1], "") if len(ele["email"]) == 2 else
                                              (ele["email"][0], ele["email"][1], ele["email"][2])) if "email" in ele else ("", "", "")
                                m1, m2, m3 = (("", "", "") if len(ele["phoneNo"]) == 0 else (ele["phoneNo"][0], "", "") if len(ele["phoneNo"]) == 1 else
                                              (ele["phoneNo"][0], ele["phoneNo"][1], "") if len(ele["phoneNo"]) == 2 else
                                              (ele["phoneNo"][0], ele["phoneNo"][1], ele["phoneNo"][2])) if "phoneNo" in ele else ("", "", "")
                                # if not ex:
                                #    junk=db.Insert(db="sms_analytics", table="mw_contact_dump", DEVICE_ID=input_dict["messageHeader"]["deviceId"],
                                #                   EMAIL1=e1, EMAIL2=e2, EMAIL3=e3, MOBILE_NUMBER1=m1, MOBILE_NUMBER2=m2, MOBILE_NUMBER3=m3,
                                #                   NAME=ele["name"] if "name" in ele else "", ACCESSED='0', compulsory=False)
                                # elif m1:
                                #    junk=db.Update(db="sms_analytics", table="mw_contact_dump", checkAll=False,
                                #                   conditions={"A.NAME=":ele["name"], "A.DEVICE_ID=": input_dict["messageHeader"]["deviceId"]},
                                #                   EMAIL1=e1, EMAIL2=e2, EMAIL3=e3, MOBILE_NUMBER1=m1, MOBILE_NUMBER2=m2, MOBILE_NUMBER3=m3)
                            except:
                                pass
                    token = generate(db).AuthToken(exp=1, update=False)
                    if True:  # token["updated"]:
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
