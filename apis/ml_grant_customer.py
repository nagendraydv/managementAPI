
from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType


class grantCustomerResource:

    def on_get(self, req, resp):
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTPError, 'error', ex.message)

    def on_post(self, req, resp):
        '''Handles post request'''
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"CustomerId":"","Name":""}}
        errors = utils.errors
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTPError, "invalid json", "json was incorrect")
        try:
            if not validate.Request(api="grantCustomer", request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    #phonenumber = input_dict["data"]["phoneNumber"]
                    #panNumber = input_dict["data"]["panNumber"]
                    indict = input_dict["data"]
                    bulkCustCreate = Table("mw_bulk_customer_create", schema="mint_loan")
                    custmaster = Table("mw_client_loan_master", schema="mint_loan")
                    q1 = Query.from_(bulkCustCreate).select(bulkCustCreate.CUSTOMER_ID,bulkCustCreate.NAME)
                    if indict["searchBy"] in ["panNumber","phoneNumber"]:
                        if indict["searchBy"]=="panNumber":
                            q1 = q1.where((bulkCustCreate.PAN_NO ==indict["searchText"]) & (bulkCustCreate.COMPANY == "WEBGRANT2"))
                        if  indict["searchBy"] == "phoneNumber":
                            q1 = q1.where((bulkCustCreate.CONTACT_NUMBER == indict["searchText"]) & (bulkCustCreate.COMPANY == "WEBGRANT2"))
                    #print(q1)
                    data=db.runQuery(q1)["data"]
                    if data !=[]:
                        q2 = Query.from_(custmaster).select(custmaster.LOAN_REFERENCE_ID).where((custmaster.CUSTOMER_ID == data[0]["CUSTOMER_ID"]))
                        #print(q2)
                        q2 = db.runQuery(q2)
                        #print(q2)
                        if q2["data"] !=[]:
                            token = generate(db).AuthToken()
                            output_dict["msgHeader"].update({"authToken": token["token"]})
                            output_dict["msgHeader"].update({"error": 0, "message": "loan exist"})
                            output_dict["data"].update({"Name":data[0]["NAME"],"CustomerId":data[0]["CUSTOMER_ID"],"loanExist":1,"customerExist":1})
                        else:
                            token = generate(db).AuthToken()
                            output_dict["msgHeader"].update({"authToken": token["token"]})
                            output_dict["msgHeader"].update({"error": 0, "message": "loan not exist"})
                            output_dict["data"].update({"Name":data[0]["NAME"],"CustomerId":data[0]["CUSTOMER_ID"],"loanExist":0,"customerExist":1})
                    else:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"].update({"authToken": token["token"]})
                        output_dict["msgHeader"].update({"error": 0, "message": "customer not exist"})
                        output_dict["data"].update({"loanExist":0,"customerExist":0})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise 
