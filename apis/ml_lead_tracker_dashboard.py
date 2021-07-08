from __future__ import absolute_import
import falcon
import json
from datetime import date
import datetime
from mintloan_utils import DB, generate, validate, timedelta, utils
from pypika import Query, Table, functions, Order, JoinType

class LeadTrackerDashboardResource:
    
    def last_date_month(self,date):
        nxt_mnth = date.replace(day=28) + datetime.timedelta(days=4)
        month_last_date = nxt_mnth - datetime.timedelta(days=nxt_mnth.day)
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
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = ""
        logInfo = {'api': 'LeadTrackerDashboard'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not True:#validate.Request(api='custDetails', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    lprof = Table("lead_profile", schema="mw_lead")
                    ladetails = Table("lead_additional_details", schema="mw_lead")
                    calldata = Table("mw_call_data", schema="mint_loan")
                    #clientmaster = Table("mw_finflux_client_master", schema="mw_lead")
                    q1 = Query.from_(lprof).join(ladetails,how=JoinType.left).on(lprof.LEAD_ID==ladetails.LEAD_ID)
                    #q1 = q1.join(calldata, how=JoinType.left).on(lprof.LEAD_ID==calldata.LEAD_ID)
                    q1 = q1.select(functions.Count(lprof.LEAD_ID).as_("count"))
                    #print(db.pikastr(q1.where((lprof.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE<str(self.last_date_month(date.today()))))))
                    this_month = db.runQuery(q1.where((lprof.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE[date.today():str(self.last_date_month(date.today()))])))["data"]
                    #print(this_month)
                    next_month = db.runQuery(q1.where((lprof.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE[date.today():str(self.last_date_month(date.today()) + datetime.timedelta(days=30))])))["data"]
                    third_month = db.runQuery(q1.where((lprof.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE[str(self.last_date_month(date.today()) + datetime.timedelta(days=30)):str(self.last_date_month(date.today()) + datetime.timedelta(days=60))])))["data"]
                    #print(db.pikastr(q1.where((lprof.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE[str(self.last_date_month(date.today()) + datetime.timedelta(days=30)):str(self.last_date_month(date.today()) + datetime.timedelta(days=60))]))))
                    after_third_month = db.runQuery(q1.where((lprof.CAMPAIGN_ID==6) & (ladetails.DATA_KEY=="INSURANCE_EXPIRY") & (ladetails.DATA_VALUE>str(self.last_date_month(date.today()) + datetime.timedelta(days=60)))))["data"]
                    q2 = Query.from_(ladetails).select(ladetails.DATA_VALUE).as_("TOKEN").distinct().where(ladetails.DATA_KEY=="TOKEN")
                    print(db.pikastr(q2))
                    vehi_type = db.runQuery(q2)["data"]
                    print(vehi_type)
                    if this_month:
                        token = generate(db).AuthToken()
                        output_dict["data"].update({"error": 0, "message": success, "this_month_expiry": this_month[0]["count"],"next_month":next_month[0]["count"],"third_month": third_month[0]["count"],"after_third_month":after_third_month[0]["count"]})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["query"]})
                try:
                    resp.body = json.dumps(output_dict, encoding='unicode-escape')
                except:
                    resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",extra=logInfo, exc_info=True)
            raise
