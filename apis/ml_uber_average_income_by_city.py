from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType, functions


class UberAvgIncByCityResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"incomeDetails": []}}
        errors = utils.errors
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if False:  # not validate.Request(api='', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    inc = Table("mw_driver_income_data_new",schema="mint_loan")
                    city = Table("mw_uber_city_id_mapping", schema="mint_loan")
                    r = Query.from_(inc).join(city, how=JoinType.left).on(
                        inc.CITY_ID == city.UBER_CITY_ID)
                    r = r.select(city.CITY, inc.WEEK, functions.Count(
                        inc.CUSTOMER_ID).distinct().as_("NO_OF_CUSTOMERS"))
                    r = r.select(functions.Avg(inc.INCOME).as_("AVERAGE_INCOME")).where(
                        inc.PARTNER_TYPE == "single_dco")
                    r = r.where((inc.WEEK > "2017-01-01") &
                                (inc.CUSTOMER_ID != '0') & (city.CITY.notnull()))
                    r = db.runQuery(r.groupby(inc.WEEK, inc.CITY_ID).orderby(
                        inc.CITY_ID, inc.WEEK))["data"]
                    outdict = {ele["CITY"]: [] for ele in r}
                    for ele in r:
                        c = ele.pop("CITY")
                        outdict[c].append(ele)
                    if True:  # token["updated"]:
                        output_dict["data"]["incomeDetails"] = utils.camelCase(
                            r)
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
