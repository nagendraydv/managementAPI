from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class RejectLoanResource:

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
        success = "data loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
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
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    cred = Table("mw_customer_login_credentials",
                                 schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    usertype = Table("mw_admin_user_account_type",
                                     schema="mint_loan_admin")
                    baseurl = db.runQuery(Query.from_(conf).select(
                        "CONFIG_VALUE").where(conf.CONFIG_KEY == "FINFLUX_URL"))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    q = db.runQuery(Query.from_(usertype).select("ACCOUNT_TYPE").where(
                        usertype.LOGIN == input_dict["msgHeader"]["authLoginID"]))
                    accTypes = [ele["ACCOUNT_TYPE"] for ele in q["data"]]
                    params = Query.from_(conf2).select("CONFIG_KEY", "CONFIG_VALUE").where(
                        conf2.CONFIG_KEY.isin(["FinfluxAccessToken"]))
                    junk = False
                    if (input_dict["data"]["requested"] != 1 if "requested" in input_dict["data"] else True):
                        exist = db.runQuery(Query.from_(lm).join(cred, how=JoinType.left).on_field("CUSTOMER_ID").select(lm.star).where((lm.LOAN_REFERENCE_ID == str(
                            input_dict["data"]["loanRefID"])) & (cred.STAGE.notin(["LOAN_IN_PROCESS", "LOAN_APPROVED"]))))["data"]  # & (lm.LENDER=='CHAITANYA')))["data"]
                    elif ("VERIFICATION" in accTypes) | ("SUPERUSER" in accTypes):
                        junk = db.Update(db="mint_loan", table="mw_client_loan_master", STATUS='ML_REJECTED', MODIFIED_BY=input_dict["msgHeader"]["authLoginID"], MODIFIED_DATE=datetime.now(
                        ).strftime("%Y-%m-%d %H:%M:%S"), conditions={"STATUS=": "REQUESTED", "CUSTOMER_ID=": str(input_dict["data"]["custID"])}, checkAll=False)
                        # db.runQuery(Query.from_(lm).select("*").where((lm.STATUS=='REQUESTED') & (lm.CUSTOMER_ID==str(input_dict["data"]["custID"]))))
                        exist = False
                    else:
                        exist, junk = False
                    # print exist
                    if exist:
                        urlKey = (
                            "MIFOS_URL" if exist[0]["LENDER"] == "GETCLARITY" else "FINFLUX_URL")
                        baseurl = db.runQuery(Query.from_(conf).select(
                            "CONFIG_VALUE").where(conf.CONFIG_KEY == urlKey))
                        baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                        if exist[0]["LENDER"] == "GETCLARITY":
                            headers = (
                                utils.finflux_headers[exist[0]["LENDER"]] if exist[0]["LENDER"] in utils.finflux_headers else {})
                            auth = utils.mifos_auth
                            posturl = baseurl + "loans/" + \
                                str(input_dict["data"]["loanRefID"])
                            print(posturl)
                            payload = {"locale": "en", "dateFormat": "dd MMMM yyyy",
                                       "rejectedOnDate": datetime.now().strftime("%d %b %Y")}
                            r = requests.post(posturl + "?command=" + 'reject', data=json.dumps(payload), headers=headers, auth=auth, verify=False)
                        else:
                            tokenKey = "MintwalkFinfluxAccessToken" if exist[0]["LENDER"] == "GETCLARITY" else "FinfluxAccessToken"
                            params = db.runQuery(Query.from_(conf2).select(
                                "CONFIG_KEY", "CONFIG_VALUE").where(conf2.CONFIG_KEY.isin([tokenKey])))
                            params = {"FinfluxAccessToken": ele["CONFIG_VALUE"] for ele in params["data"]}
                            headers = utils.mergeDicts((utils.finflux_headers[exist[0]["LENDER"]] if exist[0]["LENDER"] in utils.finflux_headers else {}), {"Authorization": "bearer " + params["FinfluxAccessToken"]})
                        #params = {ele["CONFIG_KEY"]:ele["CONFIG_VALUE"] for ele in db.runQuery(params)["data"]}
                        #headers = utils.mergeDicts(utils.finflux_headers['CHAITANYA'], {"Authorization":"bearer " + params["FinfluxAccessToken"]})
                            r = requests.put(baseurl + "loanapplicationreferences/" + str(input_dict["data"]["loanRefID"]) + "?command=reject", headers=headers, verify=False)
                        # print r
                        # print r.json()
                        updated = False
                        if r.status_code == 200:
                            updated = db.Update(db="mint_loan", table="mw_client_loan_master", checkAll=False,
                                                conditions={"ID = ": str(exist[0]["ID"])}, STATUS="REJECTED")
                        if updated:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["data"].update({"error": 0, "message": success})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            output_dict["data"].update({"error": 1, "message": "Something went wrong"})
                    elif (input_dict["data"]["requested"] == 1 if "requested" in input_dict["data"] else False):
                        if junk:
                            token = generate(db).AuthToken()
                            if token["updated"]:
                                output_dict["data"].update({"error": 0, "message": success})
                                output_dict["msgHeader"]["authToken"] = token["token"]
                            else:
                                output_dict["data"].update({"error": 1, "message": errors["token"]})
                        else:
                            output_dict["data"].update({"error": 1, "message": "Rejection failed. You are possibly not allowed to do this."})
                    else:
                        output_dict["data"].update({"error": 1, "message": "could not find data to update"})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
