from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetStandardQueryListResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {"queries": []}}
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
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(
                    token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    stdq = Table("mw_standard_queries", schema="mint_loan")
                    query_list = db.runQuery(Query.from_(stdq).select(
                        "QUERY_TYPE", "QUERY_NAME", "AUTO_ID"))["data"]
                    if query_list:
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update(
                                {"error": 0, "message": success, "queries": utils.camelCase(query_list)})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        input_dict["data"].update(
                            {"error": 1, "message": "could not find data"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
