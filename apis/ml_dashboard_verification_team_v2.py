from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType


class DashboardVerificationTeamV2Resource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {},
                       "msgHeader": {"authToken": ""}}
        errors = utils.errors
        success = ""
        try:
            raw_json = req.stream.read()
            input_dict = json.loads(raw_json, encoding='utf-8')
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if not validate.Request(api='dashboard_verification', request=input_dict):
                output_dict["data"].update({"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"])
                if val_error:
                    output_dict["data"].update({"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custcred = Table("mw_customer_login_credentials", schema="mint_loan")
                    #tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    #bank = Table("mw_cust_bank_detail", schema="mint_loan")
                    #lm = Table("mw_client_loan_master", schema="mint_loan")
                    #income = Table("mw_driver_income_data_new",schema="mint_loan")
                    #derived = Table("mw_customer_derived_data",
                     #               schema="mint_loan")
                    #leadProfile=Table("lead_profile",schema="mw_lead")
                    profile = Table("mw_client_profile", schema="mint_loan")
                    log = Table("mw_customer_change_log", schema="mint_loan")
                    #kyc = Table("mw_aadhar_kyc_details", schema="mint_loan")
                    kycdocs = Table("mw_cust_kyc_documents", schema="mint_loan")
                    #cprof = Table("mw_profile_info", schema="mw_company_3")
                    days_7 = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
                    # FIND MAXDATE OF CHANGELOG FOR EACH CUSTOMER
                    q4 = Query.from_(custcred).join(profile, how=JoinType.left).on_field("CUSTOMER_ID").select("CUSTOMER_ID")
                    q4 = q4.where(custcred.STAGE == 'AWAITING_ADDITIONAL_DOCS').where(profile.COMPANY_NAME==input_dict["data"]["company_name"])
                    q3 = Query.from_(log).select(log.CUSTOMER_ID, functions.Max(log.CREATED_DATE).as_("maxdate"))
                    q3 = q3.where(log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS').groupby(log.CUSTOMER_ID)
                    q2 = Query.from_(log).join(q3).on_field("CUSTOMER_ID").select(log.star).where((log.CREATED_DATE == q3.maxdate) &
                                                                                                  (log.CUSTOMER_ID.isin(q4)) &
                                                                                                  (log.DATA_VALUE == 'AWAITING_ADDITIONAL_DOCS'))
                    q1 = Query.from_(kycdocs).join(q2, how=JoinType.left).on_field("CUSTOMER_ID")
                    q1 = q1.select("CUSTOMER_ID").distinct().where((kycdocs.CUSTOMER_ID.isin(q4)) & (kycdocs.CREATED_DATE > days_7) & (kycdocs.VERIFICATION_STATUS.isnull()) &
                                                                   ((q2.CREATED_DATE.isnull()) | (kycdocs.CREATED_DATE > q2.CREATED_DATE)))
                    q = Query.from_(custcred).select(functions.Count(custcred.CUSTOMER_ID).distinct().as_("c"))
                    #print(db.pikastr(q.where((custcred.CUSTOMER_ID.isin(q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))))
                    junk = db.dictcursor.execute(db.pikastr(q.where((custcred.CUSTOMER_ID.isin(q1)) & (custcred.STAGE == "AWAITING_ADDITIONAL_DOCS"))))
                    #print(db.pikastr(junk))
                    fdata = db.dictcursor.fetchall()
                    fdata = fdata[0]["c"]
                    #print(fdata)
                    company={}
                    q = Query.from_(profile).join(custcred, how=JoinType.left).on_field("CUSTOMER_ID")
                    q = q.select(functions.Count(profile.CUSTOMER_ID).as_("C"))
                    q = q.where(profile.COMPANY_NAME==input_dict["data"]["company_name"])
                    #print(db.pikastr(q))
                    company.update({"awaitingVerification":db.runQuery(q.where(custcred.STAGE=='AWAITING_VERIFICATION'))["data"][0]["C"],"awaitingRe-verification":db.runQuery(q.where(custcred.STAGE=='AWAITING_RE-VERIFICATION'))["data"][0]["C"],
                                   "awaitingKyc":db.runQuery(q.where(custcred.STAGE=='AWAITING_KYC'))["data"][0]["C"], "awaitingAdditionalDocs":db.runQuery(q.where(custcred.STAGE=='AWAITING_ADDITIONAL_DOCS'))["data"][0]["C"], "awaitingAgreement":db.runQuery(q.where(custcred.STAGE=='AWAITING_AGREEMENT'))["data"][0]["C"], "loanActive":db.runQuery(q.where(custcred.STAGE=='LOAN_ACTIVE'))["data"][0]["C"], "awaitingLoan":db.runQuery(q.where(custcred.STAGE=='AWAITING_LOAN_APPLICATION'))["data"][0]["C"]})
                    token = generate(db).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update({"company":{"firstPriority":company}, "company_name":input_dict["data"]["company_name"]})
                        output_dict["data"].update({"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update({"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            raise

