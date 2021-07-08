from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table


class addCompanyGroupResource:

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
                    #compMaster = Table("mw_company_master",schema="mint_loan")
                    #compCityMap = Table("mw_company_city_mapping", schema="mint_loan")
                    #compDocMap = Table("mw_company_document_mapping",schema="mint_loan")
                    #compCityProdMap = Table("mw_company_city_product_mapping",schema="mint_loan")
                    #compOtherMap = Table("mw_company_other_details_mapping" ,schema="mint_loan")
                    #created_date=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S")  
                    if db.Query(primaryTable="mw_company_group", fields={"A": ["*"]}, conditions={"GROUP_NAME =": input_dict["data"]["groupName"]}, Data=False)["count"] == 0:
                        inserted = db.Insert(db="mint_loan", table="mw_company_group", compulsory=False,date=False,
                                                    **utils.mergeDicts({"GROUP_NAME":input_dict["data"]["groupName"] if input_dict["data"]["groupName"] else None,
                                                    "DISPLAY_NAME":input_dict["data"]["displayName"] if input_dict["data"]["displayName"] else None,
                                                    "ICON_URL":input_dict["data"]["iconUrl"] if input_dict["data"]["iconUrl"] else None,
                                                    "ACTIVE":input_dict["data"]["active"] if input_dict["data"]["active"] else None,
                                                    "CREATED_BY":input_dict["msgHeader"]["authLoginID"],
                                                    "CREATED_DATE":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))#(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                        token = generate(db).AuthToken()
                        if token["updated"] and inserted:
                            #output_dict["data"]["updated"] = updated
                            output_dict["data"].update({"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        token = generate(db).AuthToken()
                        output_dict["data"].update({"error": 0, "message":"group already exist"})
                        output_dict["msgHeader"]["authToken"] = token["token"]    
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
