from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table,functions, JoinType, Order


class companyListAndGroupResource:

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
                    #cityMaster =Table("mw_city_master",schema="mint_loan")
                    #compCityMap = Table("mw_company_city_mapping", schema="mint_loan")
                    #compDocMap = Table("mw_company_document_mapping",schema="mint_loan")
                    #compCityProdMap = Table("mw_company_city_product_mapping",schema="mint_loan")
                    #compOtherMap = Table("mw_company_other_details_mapping" ,schema="mint_loan")
                    #kyc=Table("mw_kyc_document_type",schema="mint_loan")
                    q1 = Query.from_(compMaster).join(compGrp,how=JoinType.left).on(compGrp.ID==compMaster.GROUP_ID).select(compMaster.SHORT_NAME,compMaster.DISPLAY_NAME,compMaster.COMPANY_NAME,compMaster.GROUP_ID,compGrp.GROUP_NAME)
                    company=db.runQuery(q1)["data"]
                    #print(company)
                    q2 = Query.from_(compGrp).select('*')
                    cmGroup=db.runQuery(q2)["data"]
                    print(cmGroup)
                if True:
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        #output_dict["data"]["updated"] = company
                        output_dict["data"].update({"company":company,"group":cmGroup})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
