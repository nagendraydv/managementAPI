from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table,functions, JoinType, Order


class companyDetailsResource:

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
        success = "company group successfully created"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='companyGroup', request=input_dict):
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
                    compGrp = Table("mw_company_group", schema="mint_loan")
                    compMaster = Table("mw_company_master",schema="mint_loan")
                    cityMaster =Table("mw_city_master",schema="mint_loan")
                    compCityMap = Table("mw_company_city_mapping", schema="mint_loan")
                    compDocMap = Table("mw_company_document_mapping",schema="mint_loan")
                    compCityProdMap = Table("mw_company_city_product_mapping",schema="mint_loan")
                    compOtherMap = Table("mw_company_other_details_mapping" ,schema="mint_loan")
                    kyc=Table("mw_kyc_document_type",schema="mint_loan")
                    #created_date=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")
                    q1 = Query.from_(compGrp).select(compGrp.ID).where(compGrp.GROUP_NAME==input_dict["data"]["groupName"])
                    groupID = db.runQuery(q1)
                    if groupID["data"]:
                        groupId=groupID["data"][0]["ID"]
                    else:
                        groupId = None
                    #print(groupId)
                    q2 = Query.from_(compMaster).select('*').where(compMaster.GROUP_ID==groupId)
                    company=db.runQuery(q2)
                    companyShortName=list(ele["SHORT_NAME"] for ele in company["data"])if company["data"] else None
                    print(companyShortName)
                    join = Query.from_(cityMaster).join(compCityMap,how=JoinType.inner).on_field("CITY_ID").select(cityMaster.CITY,compCityMap.CITY_ID,compCityMap.COMPANY_SHORT_NAME,compCityMap.INSURANCE_ENABLED)
                    q3=join.where(compCityMap.COMPANY_SHORT_NAME.isin(companyShortName))
                    cityData=db.runQuery(q3)
                    join = Query.from_(kyc).join(compDocMap,how=JoinType.inner).on(kyc.DOCUMENT_TYPE_ID==compDocMap.DOCUMENT_ID).select(compDocMap.DOCUMENT_ID,kyc.DOCUMENT_TYPE,compDocMap.COMPANY_ID,compDocMap.COMPANY_NAME,compDocMap.UPFRONT_REQUIRED)
                    q4=join.where(compDocMap.COMPANY_NAME.isin(companyShortName))
                    docData=db.runQuery(q4)
                if True:
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        #output_dict["data"]["updated"] = company
                        output_dict["data"].update({"city":cityData["data"],"document":docData["data"],"company":company["data"]})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
