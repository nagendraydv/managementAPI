from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class ApproveLoanResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"docdetails": []}}
        errors = utils.errors
        success = "loan approved successfully"
        logInfo = {'api': 'approveLoan'}
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug(
                "Request: " + json.dumps(input_dict), extra=logInfo)
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='approveLoan', request=input_dict):
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
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    headers = utils.finflux_headers["GETCLARITY"]
                    auth = utils.mifos_auth
                    baseurl = db.runQuery(Query.from_(conf).select("CONFIG_VALUE").where(conf.CONFIG_KEY == "MIFOS_URL"))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    payload = {"approvedLoanAmount": input_dict["principal"] if "principal" in input_dict else "5500",
                               "approvedOnDate": datetime.now().strftime("%d %B %Y"), "dateFormat": "dd MMMM yyyy",
                               "expectedDisbursementDate": datetime.now().strftime("%d %B %Y"), "locale": "en"}
                    utils.logger.info("api URL: " + baseurl + "loans/" +
                                      input_dict["data"]["loanID"] + "?command=approve", extra=logInfo)
                    r = requests.post(baseurl + "loans/" + input_dict["data"]["loanID"] + "?command=approve", data=json.dumps(payload),headers=headers, auth=auth, verify=False)
                    utils.logger.info("api response: " + json.dumps(r.json()), extra=logInfo)
                    print(r.status_code)
                    if r.status_code == 200:
                        updated = db.Update(db="mint_loan", table="mw_client_loan_master", checkAll=False,
                                            conditions={"ID = ": str(input_dict["data"]["id"])}, STATUS="APPROVED")
                        if updated:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["data"].update({"error": 0, "message": success})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            input_dict["data"].update({"error": 1, "message": "Something went wrong"})
                    else:
                        input_dict["data"].update({"error": 1, "message": "could not approve loan"})
                resp.body = json.dumps(output_dict)
                utils.logger.debug(
                    "Response: " + json.dumps(output_dict) + "\n", extra=logInfo)
                db._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
