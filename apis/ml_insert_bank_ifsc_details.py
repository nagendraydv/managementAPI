from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType, functions


class InsertBankIfscDetailsResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"msgHeader": {"authToken": ""},
                       "data": {"ifscDetails": []}}
        errors = utils.errors
        success = "data inserted successfully"
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
        try:
            if not validate.Request(api='insertIfsc', request=input_dict):
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
                    bank = Table("mw_bank_ifsc_code", schema="mint_loan")
                    exist = db.runQuery(Query.from_(bank).select(bank.star).where(
                        bank.IFSC == input_dict["data"]["ifsc"]))["data"]
                    if exist == []:
                        if len(input_dict["data"]["ifsc"]) == 11:
                            indict = input_dict["data"]
                            idm = db.runQuery(Query.from_(bank).select(
                                functions.Max(bank.id).as_("idm")))["data"][0]["idm"]
                            print(idm)
                            DB.pikastr()
                            j = db.Insert(db="mint_loan", table="mw_bank_ifsc_code", compulsory=False, date=False, BANK=indict["bank"],
                                          IFSC=indict["ifsc"], BRANCH=indict["branch"], CONTACT=indict["contact"], ADDRESS=indict["address"],
                                          CITY=indict["city"], DISTRICT=indict["district"], STATE=indict["state"], MICR=indict["micr"],
                                          STATUS=indict["status"], BANK_CODE=indict["bankcode"], CREATED_BY=input_dict["msgHeader"]["authLoginID"],
                                          CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        else:
                            success = "wrong ifsc format"
                    else:
                        success = "ifsc already exist"
                    if True:  # token["updated"]:
                        output_dict["data"].update({"error": 0, "message": success})
                        # token["token"]
                        output_dict["msgHeader"]["authToken"] = input_dict["msgHeader"]["authToken"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise# falcon.HTTPError(falcon.HTTP_400, 'Invalid JSON', 'The JSON was incorrect.')
