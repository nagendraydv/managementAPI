from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType
import numpy as np


class LeadCreationResource:

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
            if not validate.Request(api='lead_creation', request=input_dict):
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
                    leadMap = Table("mw_lead_product_mapping", schema="mw_lead")
                    if data["primary_phone_number"]!="":
                        exist = db.runQuery(Query.from_(lf).select(lf.LEAD_ID).where(lf.PRIMARY_PHONE_NUMBER==data["primary_phone_number"]))
                        #print(exist)
                        #print(exist["data"])
                        if exist["data"]==[]:
                            prof_keys = ["primary_phone_number", "pan_number", "aadhar_number", "city", "gstin_number", "first_name","company","address",
                                             "last_name", "address", "pincode", "email_id", "bank_account_number", "ifsc_code","peny_drop_verification", "account_details_verified","eligible_limit", "profile_created", "bank_added"]
                            adv_data = {data_key:data_value for data_key,data_value in data.items() if data_key not in prof_keys}
                            campaign_id = db.runQuery(Query.from_(cm).select(cm.CAMPAIGN_ID).where(cm.NAME=='UNREGISTERED'))
                            if campaign_id:
                                campaign_id = campaign_id["data"][0]["CAMPAIGN_ID"]
                            else:
                                campaign_id = None
                            q2 = Query.from_(cm).select(cm.PRODUCT).where(cm.CAMPAIGN_ID==campaign_id)
                            prod = db.runQuery(q2)["data"]
                            if prod!=[]:
                                prod = prod[0]["PRODUCT"]
                            else:
                                prod = None
                            inserted = db.Insert(db="mw_lead", table='lead_profile', compulsory=False, date=False, debug=False,
                                                     **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"COMPANY":data["company"] if "company" in data else None,
                                                                         "CAMPAIGN_ID":campaign_id, "PRIMARY_PHONE_NUMBER": data["primary_phone_number"], "PAN_NUMBER": data["pan_number"] if "pan_number" in data else None,
                                                                         "AADHAR_NUMBER": data["aadhar_number"] if "aadhar_number" in data else None, "GSTIN_NUMBER": data["gstin_number"] if "gstin_number" in data else None,
                                                                         "STAGE":"LEAD_CREATED","CITY":data["city"] if "city" in data else None, "CUSTOMER_ID":data["customer_id"] if "customer_id" in data else None,
                                                                         "FIRST_NAME":data["first_name"] if "first_name" in data else None, "LAST_NAME":data["last_name"] if "last_name" in data else None,
                                                                         "PINCODE":data["pincode"] if "pincode" in data else None, "EMAIL_ID":data["email_id"] if "email_id" in data else None, "SOURCE":data["source"] if "source" in data else None,
                                                                         "UNIQUE_ID":data["unique_id"] if "unique_id" in data else None, "IFSC_CODE": data["ifsc_code"] if "ifsc_code" in data else None,
                                                                         "BANK_ACCOUNT_NO": data["bank_account_number"] if "bank_account_number" in data else None, "ACCOUNT_DETAILS_VERIFIED": data["account_details_verified"] if "account_details_verified" in data else None,
                                                                         "PENNY_DROP_VERIFICATION": data["peny_drop_verification"] if "peny_drop_verification" in data else None, "ELIGIBLE_LIMIT": data["eligible_limit"] if "eligible_limit" in data else None,
                                                                         "PROFILE_CREATED": data["profile_created"] if "profile_created" in data else None, "BANK_ADDED": data["bank_added"] if "bank_added" in data else None,
                                                                         "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                            lead_id = db.Query(db="mw_lead", primaryTable='lead_profile', fields={
                                             "A": ["LEAD_ID"]}, orderBy="LEAD_ID desc", limit=1)
                            if lead_id["data"] != []:
                                lead_id = str(lead_id["data"][0]["LEAD_ID"])
                            else:
                                lead_id = None
                            if inserted:
                                for k,v in adv_data.items():
                                    inserted = db.Insert(db="mw_lead", table='lead_additional_details', compulsory=False, date=False, 
                                                         **utils.mergeDicts({"created_by":msgHeader["authLoginID"],"LEAD_ID":str(lead_id), "CAMPAIGN_ID":str(campaign_id),
                                                                             "DATA_KEY":str(k), "DATA_VALUE":str(v),
                                                                             "created_date":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                                inserted = db.Insert(db="mw_lead", table='mw_lead_product_mapping', compulsory=False, date=False, debug=False,
                                                     **utils.mergeDicts({"CREATED_BY":msgHeader["authLoginID"],"LEAD_ID":str(lead_id),
                                                                         "CAMPAIGN_ID":str(campaign_id), "PRODUCT_ID": "prodID", "PRODUCT_NAME": data["productName"] if "productName" in data else None,
                                                                         "ASSIGNED_TO": data["assignedTo"] if "assignedTo" in data else None,"CREATED_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")}))
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["data"].update({"error": 0, "message": "lead created successfully"})
                                output_dict["data"].update({"error": 0})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            token = generate(db).AuthToken()
                            output_dict["data"].update({"error": 0, "message": "lead already generated with this phone number"})
                            output_dict["data"].update({"error": 0})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update({"error": 1, "message": "phone number is empty"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise
