from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class DashboardClearTaxResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"noOfLeeds": [], "leadConverted": []},"msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='DashboardClearTax', request=input_dict):
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
                    lf = Table("lead_profile", schema="mw_lead")
                    q1 = Query.from_(lf).select(functions.Count(lf.LEAD_ID).as_('c')) 
                    q1 = q1.where(lf.CAMPAIGN_ID==2)
                    no_Of_Lead = db.runQuery(q1)
                    q2 = Query.from_(lf).select(functions.Count(lf.CUSTOMER_ID).as_('c'))
                    q2 = q2.where((lf.CUSTOMER_ID!='NULL')&(lf.CAMPAIGN_ID==2))
                    leadConverted = db.runQuery(q2)
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"noOfLeeds": no_Of_Lead['data'][0]['c'], "leadConverted": leadConverted['data'][0]['c']})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
