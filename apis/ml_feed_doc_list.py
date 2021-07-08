"""
@author: nagendra
"""
from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, functions, JoinType


class FeedDocListResource:

    def on_get(self, req, resp):
        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTPError, 'error', ex.message)

    def on_post(self, req, resp):
        '''Handles post request'''
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTPError, "invalid json", "json was incorrect")
        try:
            if not validate.Request(api="docList", request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    docType = input_dict["data"]["docType"]
                    days = input_dict["data"]["days"]
                    otherDoc = Table("mw_other_documents", schema="mint_loan")
                    purchase=Table("mw_physical_purchase_request",schema="mf_investment")
                    q1=Query.from_(otherDoc).join(purchase,how=JoinType.inner).on_field("DOC_SEQ_ID")
                    q1 = q1.select(otherDoc.DOC_SEQ_ID,otherDoc.DOCUMENT_URL,otherDoc.CREATED_BY,otherDoc.CREATED_DATE).where((otherDoc.DOCUMENT_FOLDER==docType)&(otherDoc.CREATED_DATE>(datetime.now()-timedelta(days=100)).strftime("%Y-%m-%d")))
                    #print(q1)
                    data = db.runQuery(q1)
                    #print(data)
                    if data["data"]!=[]:
                        for i in range(len(data["data"])):
                            data["data"][i].update({"DOCUMENT_URL":data["data"][i]["DOCUMENT_URL"].split('/')[-1]})
                    if data["data"]!=[]:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"].update({"authToken": token["token"]})
                        output_dict["msgHeader"].update({"error": 0, "message": "data fetched successfully"})
                        output_dict["data"].update({"data":data["data"]})
                        resp.body = json.dumps(output_dict)
                    else:
                        token = generate(db).AuthToken()
                        output_dict["msgHeader"].update({"authToken": token["token"]})
                        output_dict["msgHeader"].update({"error": 0, "message": "no data found"})
                        output_dict["data"].update({"data":data["data"]})
                        resp.body = json.dumps(output_dict)
            db._DbClose_()
        except Exception as ex:
            raise #falcon.HTTPError(falcon.HTTPError, "error")
