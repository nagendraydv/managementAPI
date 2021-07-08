from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table


class addCompanyResource:

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
                    if db.Query(primaryTable="mw_company_master", fields={"A": ["*"]}, conditions={"SHORT_NAME =": input_dict["data"]["shortName"]}, Data=False)["count"] == 0:
                        q1=Query.from_(compGrp).select(compGrp.ID).where(compGrp.GROUP_NAME==input_dict["data"]["groupName"])
                        groupID = db.runQuery(q1)
                        if groupID["data"]:
                            groupId=str(groupID["data"][0]["ID"])
                        else:
                            groupId=None
                        print(groupId)
                        inserted= db.Insert(db="mint_loan", table="mw_company_master", compulsory=False,date=False,
                                            **utils.mergeDicts({"SHORT_NAME":input_dict["data"]["shortName"] if input_dict["data"]["groupName"] else None,
                                                                "DISPLAY_NAME":input_dict["data"]["displayName"] if input_dict["data"]["displayName"] else None,
                                                                "COMPANY_NAME":input_dict["data"]["companyName"] if input_dict["data"]["companyName"] else None,
                                                                "PAN_NUMBER":input_dict["data"]["panNumber"] if input_dict["data"]["panNumber"] else None,
                                                                "IS_VENDOR":input_dict["data"]["isVendor"] if input_dict["data"]["isVendor"] else None,
                                                                "PREFERRED":input_dict["data"]["preferred"] if input_dict["data"]["preferred"] else None,
                                                                "ICON_URL":input_dict["data"]["iconUrl"] if input_dict["data"]["iconUrl"] else None,
                                                                "OTHER_DETAILS_WEB_URL":input_dict["data"]["webUrl"] if input_dict["data"]["webUrl"] else '',
                                                                "GROUP_ID":str(groupId),
                                                                "CREATED_BY":input_dict["msgHeader"]["authLoginID"],
                                                                "CREATED_DATE":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))
                        token = generate(db).AuthToken()
                        if token["updated"] and inserted:
                            #output_dict["data"]["updated"] = updated
                            output_dict["data"].update({"error": 0, "message": "company added successfully"})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update({"error": 1, "message": errors["token"]})
                    else:
                        token = generate(db).AuthToken()
                        output_dict["data"].update({"error": 0, "message":"company already exist"})
                        output_dict["msgHeader"]["authToken"] = token["token"]    
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
