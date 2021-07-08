from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType
from six.moves import range


class ShowFinfluxInsertLogResource: #Any change in this class will change the dblog line number. So need to update the line number in shofinfluxlog file

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"error": 0, "message": ""}}
        errors = utils.errors
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            # not validate.Request(api='approveLoan', request=input_dict):
            if False:
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
                    log = Table("mw_python_api_log", schema="mint_loan")
                    q = db.runQuery(Query.from_(log).select(log.star).where((log.API_NAME == 'repayBulkUpload') &
                                                                            (log.CREATED_AT > (datetime.now()-timedelta(days=3)).strftime("%Y-%m-%d"))))
                    requests = []
                    for ele in filter(lambda x:x["LINE_NUMBER"] in (94, 103, 104), q["data"]):
                        requests.append({"id": q["data"].index(ele), "REQUEST_TIME": ele["CREATED_AT"], "REQUEST": json.loads(ele["LOG"][9:]),
                                         "TRANSACTIONS": []})
                    for i in range(len(requests)):
                        for j in range(requests[i]["id"]+1, requests[i+1]["id"] if i+1 < len(requests) else len(q["data"]), 3):
                            try:
                                loanID = q["data"][j]["LOG"].split("loans/")[1].split("/")[0]
                            except:
                                loanID = ''
                            try:
                                request = json.loads(q["data"][j+1]["LOG"].split("api request: ")[1])
                            except:
                                request = {}
                            try:
                                response = json.loads(q["data"][j+2]["LOG"].split("api response: ")[1])
                            except:
                                response = {}
                            requests[i]["TRANSACTIONS"].append(
                                {"loanID": loanID, "request": request, "response": response})
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"error": 0, "message": "", "logDetails": requests})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                #utils.logger.debug("Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            #utils.logger.error("ExecutionError: ", extra=logInfo, exc_info=True)
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
