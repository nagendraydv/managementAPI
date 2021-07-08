from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class ShowMandateDataResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"mandateData": []}}
        errors = utils.errors
        success = "mandates loaded successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
            # print input_dict
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='showMandateData', request=input_dict):
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
                    page = input_dict["data"]['page']
                    mandate = Table("mw_physical_mandate_status", schema="mint_loan")
                    mandocs = Table("mw_mandate_documents", schema="mint_loan")
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    q = Query.from_(mandate).join(custcred, how=JoinType.left).on_field(
                        "CUSTOMER_ID").join(kyc, how=JoinType.left)
                    q = q.on_field("CUSTOMER_ID").join(mandocs, how=JoinType.left).on(
                        mandate.DOC_ID == mandocs.AUTO_ID)
                    q = q.select(mandate.star, mandocs.MANDATE_FORMAT,
                                 custcred.LOGIN_ID, kyc.NAME)
                    indict = input_dict["data"]
                    if indict["fromDate"]:
                        q = q.where(mandate.MANDATE_DATE >= indict["fromDate"])
                    if indict["toDate"]:
                        q = q.where(mandate.MANDATE_DATE <= indict["toDate"])
                    if indict["days"] > 0:
                        q = q.where(mandate.MANDATE_DATE >= (
                            datetime.now() - timedelta(days=indict["days"]-1)).strftime("%Y-%m-%d"))
                    if indict["mandateID"] != "":
                        q = q.where(mandate.ID == indict["mandateID"])
                    respdict = db.runQuery(q.orderby(mandate.MANDATE_DATE, order=Order.desc).limit(
                        "%i,%i" % (page["startIndex"], page["size"])))
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"]["mandateData"] = utils.camelCase(
                            respdict["data"])
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                # print output_dict["msgHeader"]
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
