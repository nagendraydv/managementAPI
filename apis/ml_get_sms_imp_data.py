from __future__ import absolute_import
import falcon
import json
#import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table,JoinType,functions


class getSMSDataResource:

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
        #success = "company group successfully created"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:#not validate.Request(api='companyGroup', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    #loginID = input_dict["data"]["loginID"]
                    #cityMaster=Table("mw_city_master",schema="mint_loan")
                    #compGrp = Table("mw_company_group", schema="mint_loan")
                    #compMaster = Table("mw_company_master",schema="mint_loan")
                    #compCityMap = Table("mw_company_city_mapping", schema="mint_loan")
                    sms = Table("sms_data",schema="sms_analytics")
                    #compCityProdMap = Table("mw_company_city_product_mapping",schema="mint_loan")
                    #compOtherMap = Table("mw_company_other_details_mapping" ,schema="mint_loan")
                    #kyc = Table("mw_kyc_document_type",schema="mint_loan")
                    #created_date=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")
                    #join=Query.from_(sms).join(compDocMap,how=JoinType.left).on(kyc.DOCUMENT_TYPE_ID==compDocMap.DOCUMENT_ID).select(kyc.DOCUMENT_TYPE_ID,kyc.DOCUMENT_TYPE,kyc.DOCUMENT_GROUP)
                    q1=Query.from_(sms).select(functions.Max(sms.total_balance).as_("maxBAl"))
                    q1=q1.where(sms.customer_id==input_dict["data"]["customerID"])
                    #db.pikastr(q1)
                    totalBal=db.runQuery(q1)["data"]
                    #print(doc)
                    q2 = Query.from_(sms).select(sms.mode,sms.account_no,sms.amount,sms.sms_date,sms.device_id,sms.vendor).where(sms.mode.isin(['credited','Credited','debited','Debited']))
                    q2=q2.where(sms.customer_id==input_dict["data"]["customerID"])
                    smsData=db.runQuery(q2)["data"]
                    #print(totalBal)
                    if totalBal!=[] and smsData!=[]:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update({"error": 0, "message": "details found","maxBalance":totalBal,"smsData":smsData})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        token = generate(db).AuthToken()
                        output_dict["data"].update({"error": 0, "message":"details not found"})
                        output_dict["msgHeader"]["authToken"] = token["token"]    
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
