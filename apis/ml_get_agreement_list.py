from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class GetAgreementListResource:

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
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            # not validate.Request(api='fetchOnCustomerID', request=input_dict):
            if False:
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
                    doc = Table("mw_cust_kyc_documents", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    prof = Table("mw_client_profile", schema="mint_loan")
                    respdict = db.runQuery(Query.from_(doc).select(doc.star).where((doc.DOCUMENT_TYPE_ID == '113') &
                                                                                   (doc.VERIFICATION_STATUS.isnull())))
                    for datum in respdict["data"]:
                        kycname = db.runQuery(Query.from_(kyc).select("NAME").where(
                            kyc.CUSTOMER_ID == datum["CUSTOMER_ID"]))["data"]
                        profname = db.runQuery(Query.from_(prof).select("NAME").where(
                            prof.CUSTOMER_ID == datum["CUSTOMER_ID"]))["data"]
                        datum.update({"KYC_NAME": kycname[0]["NAME"] if kycname else "",
                                      "PROFILE_NAME": profname[0]["NAME"] if profname else ""})
                    if True:  # token["updated"]:
                        output_dict["data"]["docdetails"] = utils.camelCase(
                            respdict["data"])
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
