from __future__ import absolute_import
import falcon
import json
from datetime import date
#import datetime
from mintloan_utils import DB, generate, validate, utils, timedelta,datetime
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType
import numpy as np


class LeadSearchResource:
    
    def last_date_month(self,date):
        nxt_mnth = date.replace(day=28) + timedelta(days=4)
        month_last_date = nxt_mnth - timedelta(days=nxt_mnth.day)
        #print(res)
        return month_last_date

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"customerData":[],"error":0,'message':''},"msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if False:#not validate.Request(api='grantCustomer', request=input_dict):
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
                    indict = input_dict["data"]
                    lf = Table("lead_profile", schema="mw_lead")
                    ladetails = Table("lead_additional_details", schema="mw_lead")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    reasondata = Table("mw_call_interaction_reasons" ,schema = "mint_loan")
                    resoldata = Table("mw_call_interaction_resolutions" , schema = "mint_loan")
                    resp_data=[]
                    #q1 = Query.from_(lf).select(lf.LEAD_ID,lf.CAMPAIGN_ID,lf.PRIMARY_PHONE_NUMBER,lf.PAN_NUMBER,lf.AADHAR_NUMBER,lf.GSTIN_NUMBER,lf.CUSTOMER_ID,
                                                #lf.FIRST_NAME,lf.LAST_NAME,lf.STAGE,lf.COMPANY,lf.CITY,lf.PINCODE,
                                                #lf.EMAIL_ID,lf.SOURCE,lad.DATA_KEY,lad.DATA_VALUE).join(lad, how=JoinType.inner).on_field("LEAD_ID")
                    q1 = Query.from_(lf).select('*')
                    q2 = Query.from_(lf).join(ladetails,how=JoinType.left).on(lf.LEAD_ID==ladetails.LEAD_ID)
                    q3 = Query.from_(calldata)
                    q3 = q3.join(reasondata, how=JoinType.left).on(calldata.INTERACTION_REASON_ID==reasondata.AUTO_ID)
                    q2 = q2.select(lf.CAMPAIGN_ID,lf.LEAD_ID,lf.COMPANY,functions.Concat(lf.FIRST_NAME, ' ', lf.LAST_NAME).as_("NAME"),ladetails.DATA_VALUE.as_("insurance_expiry_date"))
                    if indict["searchBy"]  in ["campaignID","date","name","phoneNumber"]:
                        if indict["searchBy"] =="date":
                            if indict["searchText"] == "today":
                                today = datetime.now().strftime("%Y-%m-%d 00:00:00")
                                q1 = q1.where((lf.CREATED_DATE > today))
                            elif indict["searchText"] == "lastWeek":
                                days_7 = (datetime.now() - timedelta(days=7)
                                      ).strftime("%Y-%m-%d 00:00:00")
                                q1 = q1.where((lf.CREATED_DATE > days_7))
                            elif indict["searchText"] == "lastMonth":
                                days_30 = (datetime.now() - timedelta(days=30)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                                q1 = q1.where((lf.CREATED_DATE > days_30))
                            elif indict["searchText"] == "last3Month":
                                days_90 = (datetime.now() - timedelta(days=90)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                                q1 = q1.where((lf.CREATED_DATE > days_90))
                            #q1 = q1.where(lf.CAMPAIGN_ID==indict["searchText"])
                            resp_data = db.runQuery(q1)
                        if indict["searchBy"] =="campaignID":
                            q1 = q1.where(lf.CAMPAIGN_ID==indict["searchText"])
                            resp_data = db.runQuery(q1)
                        if indict["searchBy"] == "name":
                            q1 = q1.where((lf.FIRST_NAME.like("%" + indict["searchText"] + "%")).__or__(lf.LAST_NAME.like("%" + indict["searchText"] + "%")))
                            resp_data = db.runQuery(q1)
                        if indict["searchBy"] == "phoneNumber":
                            q1 = q1.where((lf.PRIMARY_PHONE_NUMBER.like("%" + indict["searchText"] + "%")))
                            resp_data = db.runQuery(q1)
                    elif indict["searchBy"] in ["nextMonth", "thisMonth", "thirdMonth", "afterThirdMonth"]:
                        if indict["searchBy"] == "nextMonth":
                            print(str(self.last_date_month(date.today())))
                            data = db.runQuery(q2.where((lf.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE[date.today():str(self.last_date_month(date.today()))])))
                            print(data)
                            if data["data"]!=[]:
                                for ele in data["data"]:
                                    print(ele["LEAD_ID"])
                                    q4 = Query.from_()
                            else:
                                resp_data=data
                        if indict["searchBy"] == "thisMonth":
                            resp_data = db.runQuery(q2)
                        if indict["searchBy"] == "thirdMonth":
                            resp_data = db.runQuery(q2)
                        if indict["searchBy"] == "afterThirdMonth":
                            resp_data = db.runQuery(q2)
                    else:
                        output_dict["data"].update({"error": 1, "message": "please enter correct parameter"})
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"customerData": utils.camelCase(resp_data["data"])})
                        output_dict["data"].update({"error": 0})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
