from __future__ import absolute_import
import falcon
import json
import random
from mintloan_utils import DB, generate, validate, utils, datetime
from pypika import Query, Table, JoinType, Order


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
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print "aadhar", input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='aadharFill', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                # setting an instance of DB class
                db = DB(id=input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    data = db.runQuery(Query.from_(kyc).select(kyc.CUSTOMER_ID).where(
                        kyc.CUSTOMER_ID == input_dict["data"]["customerID"]))
                    if not data["data"]:
                        data = input_dict["data"]
                        insert_dict = {"CUSTOMER_ID": data["customerID"], "AADHAR_NO": data["aadharNo"], "NAME": data["name"],
                                       "DOB": data["dob"], "GENDER": data["gender"], "HOUSE": data["house"], "STREET": data["street"],
                                       "CO": data["careOf"], "LM": data["landMark"], "LC": data["locality"], "VTC": data["villageTownCity"],
                                       "DISTRICT": data["district"], "SUB_DISTRICT": data["subDistrict"], "PIN_CODE": data["pinCode"],
                                       "POST_OFFICE": data["postOffice"], "STATE": data["state"], "COUNTRY": data["country"],
                                       "CREATED_BY": input_dict["msgHeader"]["authLoginID"],
                                       "CREATED_DATE": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                        junk = db.Insert(db="mint_loan", table="mw_aadhar_kyc_details",
                                         compulsory=False, date=False, **insert_dict)
                        junk = db.Update(db="mint_loan", table="mw_aadhar_status", conditions={"CUSTOMER_ID = ": data["customerID"]},
                                         ARCHIVED="Y")
                        junk = db.Insert(db="mint_loan", table="mw_aadhar_status", compulsory=False, date=False,
                                         TRANSACTION_ID=str(random.getrandbits(50)), CUSTOMER_ID=data["customerID"], AADHAR_NO=data["aadharNo"],
                                         ARCHIVED="N", CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                         CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": "Aadhar data exists for this user."})
                # print "aadhar", output_dict
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
