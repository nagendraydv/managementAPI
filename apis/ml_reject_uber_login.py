from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class RejectUberLoginResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"message": "updated successfully"}
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
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    cred = Table("mw_customer_login_credentials_map", schema="mint_loan")
                    compProf = Table("mw_profile_info", schema="mw_company_3")
                    uauth = Table("mw_authorization_dump",schema="mw_company_3")
                    cid = db.runQuery(Query.from_(cred).select("CUSTOMER_ID").where((cred.LOGIN_ID == input_dict["data"]["loginID"]) & (cred.ACTIVE == "1")))["data"]
                    custID = str(cid[0]["CUSTOMER_ID"]) if cid else "0"
                    q = Query.from_(uauth).select(uauth.AUTO_ID).where(
                        uauth.CUSTOMER_ID == custID).orderby(uauth.AUTO_ID, order=Order.desc)
                    autoID = db.runQuery(q.limit(1))["data"]
                    updated = db.Update(db="mw_company_3", table="mw_authorization_dump", CONFIRMED_CUSTOMER_ID="0",
                                        conditions={"AUTO_ID = ": str(autoID[0]["AUTO_ID"]) if autoID else "0"}, CONFIRMED_BY="CUSTOMER",
                                        CONFIRMED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    q = Query.from_(compProf).select("DRIVER_ID", "AUTO_ID").where(
                        compProf.CUSTOMER_ID == custID)
                    profID = db.runQuery(
                        q.orderby(compProf.AUTO_ID, order=Order.desc).limit(1))["data"]
                    profID = profID[0] if profID else {
                        "DRIVER_ID": None, "AUTO_ID": "0"}
                    updated = db.Update(db="mw_company_3", table="mw_profile_info", CONFIRMED_BY="CUSTOMER",
                                        conditions={"AUTO_ID = ": str(
                                            profID["AUTO_ID"]) if profID else "0"},
                                        CONFIRMED_CUSTOMER_ID="0", CONFIRMED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
