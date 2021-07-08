from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType
import numpy as np


class UpdateLeadResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"error":0,'message':''},"msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='update_lead', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    data = input_dict["data"]
                    msgHeader = input_dict["msgHeader"]
                    lf = Table("lead_profile", schema="mw_lead")
                    lad = Table("lead_additional_details", schema ="mw_lead")
                    cm = Table("campaign_master", schema= "mw_lead")
                    #prof_keys = ["primary_phone_number", "pan_number", "aadhar_number", "city", "gstin_number", "first_name",
                     #                "last_name", "address", "pincode", "email_id"]
                    #adv_data = {data_key:data_value for data_key,data_value in data.items() if data_key not in prof_keys}
                    lead_id = db.runQuery(Query.from_(lf).select(lf.LEAD_ID).where(lf.LEAD_ID==data["lead_id"]))
                    if lead_id:
                        lead_id = lead_id["data"][0]["LEAD_ID"]
                    else:
                        lead_id = None
                    if 'customer_id' not in data:
                        indict = {"CAMPAIGN_ID":data["campaign_id"] if 'campaign_id' in data else None, "PRIMARY_PHONE_NUMBER": data["primary_phone_number"] if 'primary_phone_number' in data else None,
                                  "PAN_NUMBER":data["pan_number"] if 'pan_number' in data else None, "AADHAR_NUMBER":data["aadhar_number"] if 'aadhar_number' in data else None,
                                  "GSTIN_NUMBER":data["gstin_number"] if 'gstin_number' in data else None,
                                  "FIRST_NAME":data["first_name"] if 'first_name' in data else None, "LAST_NAME":data["last_name"] if 'last_name' in data else None,
                                  "STAGE":data["stage"] if 'stage' in data else None,"COMPANY":data["company"] if 'company' in data else None,
                                  "CITY":data["city"] if 'city' in data else None,"PINCODE":data["pincode"] if 'pincode' in data else None,
                                  "EMAIL_ID":data["email_id"] if 'email_id' in data else None,"SOURCE":data["source"] if 'source' in data else None,
                                  "UNIQUE_ID":data["unique_id"] if "unique_id" in data else None, "IFSC_CODE": data["ifsc_code"] if "ifsc_code" in data else None,
                                  "BANK_ACCOUNT_NO": data["bank_account_number"] if "bank_account_number" in data else None, "ACCOUNT_DETAILS_VERIFIED": data["account_details_verified"] if "account_details_verified" in data else None,
                                  "PENNY_DROP_VERIFICATION": data["peny_drop_verification"] if "peny_drop_verification" in data else None, "ELIGIBLE_LIMIT": data["eligible_limit"] if "eligible_limit" in data else None,
                                  "PROFILE_CREATED": data["profile_created"] if "profile_created" in data else None, "BANK_ADDED": data["bank_added"] if "bank_added" in data else None,
                                  "MODIFIED_HISTORY":msgHeader["authLoginID"],"MODIFIED_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}
                        updated = db.Update(db="mw_lead", table="lead_profile", checkAll=False,debug =False,
                                                             conditions={"LEAD_ID=": str(data["lead_id"])},**indict)
                        success = "updated successfully"
                        if updated:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["data"].update({"error": 0, "message": success})
                                output_dict["data"].update({"error": 0})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            output_dict["data"].update({"error": 1, "message": "something went wrong"})
                    else:
                        cust_id = db.runQuery(Query.from_(lf).select(lf.CUSTOMER_ID).where(lf.CUSTOMER_ID==data["customer_id"]))
                        if cust_id["data"]==[]:
                            indict = {"CAMPAIGN_ID":data["campaign_id"] if 'campaign_id' in data else None, "PRIMARY_PHONE_NUMBER": data["primary_phone_number"] if 'primary_phone_number' in data else None,
                                  "PAN_NUMBER":data["pan_number"] if 'pan_number' in data else None, "AADHAR_NUMBER":data["aadhar_number"] if 'aadhar_number' in data else None,
                                  "GSTIN_NUMBER":data["gstin_number"] if 'gstin_number' in data else None, "CUSTOMER_ID":data["customer_id"] if 'customer_id' in data else None,
                                  "FIRST_NAME":data["first_name"] if 'first_name' in data else None, "LAST_NAME":data["last_name"] if 'last_name' in data else None,
                                  "STAGE":data["stage"] if 'stage' in data else None,"COMPANY":data["company"] if 'company' in data else None,
                                  "CITY":data["city"] if 'city' in data else None,"PINCODE":data["pincode"] if 'pincode' in data else None,
                                  "EMAIL_ID":data["email_id"] if 'email_id' in data else None,"SOURCE":data["source"] if 'source' in data else None,
                                  "UNIQUE_ID":data["unique_id"] if "unique_id" in data else None, "IFSC_CODE": data["ifsc_code"] if "ifsc_code" in data else None,
                                  "BANK_ACCOUNT_NO": data["bank_account_number"] if "bank_account_number" in data else None, "ACCOUNT_DETAILS_VERIFIED": data["account_details_verified"] if "account_details_verified" in data else None,
                                  "PENNY_DROP_VERIFICATION": data["peny_drop_verification"] if "peny_drop_verification" in data else None, "ELIGIBLE_LIMIT": data["eligible_limit"] if "eligible_limit" in data else None,
                                  "PROFILE_CREATED": data["profile_created"] if "profile_created" in data else None, "BANK_ADDED": data["bank_added"] if "bank_added" in data else None,
                                  "MODIFIED_HISTORY":msgHeader["authLoginID"],"MODIFIED_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}
                            updated = db.Update(db="mw_lead", table="lead_profile", checkAll=False,debug =False,
                                                         conditions={"LEAD_ID=": str(data["lead_id"])},**indict)
                            success = "updated successfully"
                            if updated:
                                token = generate(db).AuthToken()
                                if token["updated"]:
                                    output_dict["data"].update({"error": 0, "message": success})
                                    output_dict["data"].update({"error": 0})
                                    output_dict["msgHeader"]["authToken"] = token["token"]
                                else:
                                    output_dict["data"].update({"error": 1, "message": errors["token"]})
                            else:
                                output_dict["data"].update({"error": 1, "message": "something went wrong"})
                        else:
                            token = generate(db).AuthToken()
                            output_dict["msgHeader"]["authToken"] = token["token"]
                            output_dict["data"].update({"error": 0, "message": "this customer_id already mapped please provide correct customer_id"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise
