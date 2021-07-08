from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table,JoinType,functions


class cityMappingResource:

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
                    indict = input_dict["data"]
                    #compGrp = Table("mw_company_group", schema="mint_loan")
                    compMaster = Table("mw_company_master",schema="mint_loan")
                    cityMaster =Table("mw_city_master",schema="mint_loan")
                    compCityMap = Table("mw_company_city_mapping", schema="mint_loan")
                    #compDocMap = Table("mw_company_document_mapping",schema="mint_loan")
                    #compCityProdMap = Table("mw_company_city_product_mapping",schema="mint_loan")
                    #compOtherMap = Table("mw_company_other_details_mapping" ,schema="mint_loan")
                    #created_date=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")
                    #join = Query.from_(cityMaster).join(compCityMap,how=JoinType.inner).on_field("CITY_ID").select(functions.Count(compCityMap.CITY_ID).as_("cityCount"))
                    #q3=join.where((cityMaster.CITY==input_dict["data"]["city"]) &(compCityMap.COMPANY_SHORT_NAME==input_dict["data"]["shortName"]))
                    #cityCount=db.runQuery(q3)["data"]
                    #print(cityCount)
                    #cityExist= cityCount[0]["cityCount"]  if cityCount!=[] else 0
                    #print(cityExist)
                    q1=Query.from_(compCityMap).select(compCityMap.COMPANY_SHORT_NAME).where(compCityMap.CITY_ID==input_dict["data"]["cityID"])
                    companyName=db.runQuery(q1)["data"]
                    if companyName==[]:
                            #city_id=db.runQuery(Query.from_(cityMaster).select(cityMaster.CITY_ID).where(cityMaster.CITY==input_dict["data"]["city"]))["data"]
                            #print(city_id)
                        inserted = db.Insert(db="mint_loan", table="mw_company_city_mapping", compulsory=False,date=False,
                                                    **utils.mergeDicts({"COMPANY_SHORT_NAME":input_dict["data"]["companyShortName"] if input_dict["data"]["companyShortName"] else None,
                                                    "CITY_ID":input_dict["data"]["cityID"] if input_dict["data"]["cityID"] else None,
                                                    "INSURANCE_ENABLED":0,
                                                    "CREATED_BY":input_dict["msgHeader"]["authLoginID"],
                                                    "CREATED_DATE":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))#(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")
                        token = generate(db).AuthToken()
                        if token["updated"] and inserted:
                            output_dict["data"].update({"error": 0, "message": "city mapped successfully"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        token = generate(db).AuthToken()
                        output_dict["data"].update({"error": 0, "message": "city already mapped"})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
