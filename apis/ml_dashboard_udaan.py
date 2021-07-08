from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class DashboardUdaanResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"activeCustomer": [], "repaidCustomer": []},"msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='DashboardUdaan', request=input_dict):
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
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    clProf = Table("mw_client_profile" , schema = 'mint_loan')
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    q1 = Query.from_(lm).select(functions.Count(lm.CUSTOMER_ID).as_('c')) 
                    q1 = q1.join(clProf, how=JoinType.inner).on_field("CUSTOMER_ID").join(custcred, how=JoinType.inner).on_field("CUSTOMER_ID").where(custcred.STAGE=='LOAN_ACTIVE')
                    if 'company' in input_dict['data']:
                        q1 = q1.where(clProf.COMPANY_NAME == input_dict['data']['company'])
                    if 'city' in input_dict['data']:
                        q1 = q1.where(clProf.current_city==input_dict['data']['city'])
                    print(q1)
                    activeCustomer = db.runQuery(q1)
                    q2 = Query.from_(lm).select(functions.Count(lm.CUSTOMER_ID).as_('c'))
                    q2 = q2.join(clProf, how=JoinType.inner).on_field("CUSTOMER_ID").join(custcred, how=JoinType.inner).on_field("CUSTOMER_ID").where(custcred.STAGE!='LOAN_ACTIVE')
                    if 'company' in input_dict['data']:
                        q2 = q2.where(clProf.COMPANY_NAME == input_dict['data']['company'])
                    if 'city' in input_dict['data']:
                        q2 = q2.where(clProf.current_city==input_dict['data']['city'])
                    print(q2)
                    repaidCustomer = db.runQuery(q2)
                    #print(activeCustomer['data'])
                    #print(inactiveCustomer['data'])
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"activeCustomer": activeCustomer['data'][0]['c'], "repaidCustomer": repaidCustomer['data'][0]['c']})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
