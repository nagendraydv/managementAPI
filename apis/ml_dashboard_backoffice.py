from __future__ import absolute_import
import falcon
import json
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pypika import Query, Table, functions, JoinType
import six


class DashboardBackofficeResource:

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_post(self, req, resp):
        """Handles POST requests"""
        output_dict = {"data": {"firstPriority": 0, "secondPriority": 0, "thirdPriority": 0, "fourthPriority": 0, "todaysTasks": 0,
                                "pendingTasks": 0, "upcomingTasks": 0},
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
            # not validate.Request(api='dashboard', request=input_dict):
            if False:
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB(input_dict["msgHeader"]["authLoginID"],
                        filename='mysql-slave.config')
                dbw = DB(input_dict["msgHeader"]["authLoginID"])
                val_error = validate(db).basicChecks(token=input_dict["msgHeader"]["authToken"], loginID=input_dict["msgHeader"]["authLoginID"],
                                                     checkLogin=True)
                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    custcred = Table(
                        "mw_customer_login_credentials", schema="mint_loan")
                    tasks = Table("mw_task_lists", schema="mint_loan_admin")
                    income = Table("mw_driver_income_data_new",
                                   schema="mint_loan")
                    docs = Table("mw_cust_kyc_documents", schema="mint_loan")
                    data = Table("mw_backoffice_data", schema="mint_loan")
                    days_20 = (datetime.now() - timedelta(days=20)
                               ).strftime("%Y-%m-%d %H:%M:%S")
                    indict = input_dict["data"]
                    #q3 = Query.from_(income).select(income.CUSTOMER_ID).distinct()
                    # q2 = Query.from_(docs).select(docs.CUSTOMER_ID).distinct().where((docs.DOCUMENT_TYPE_ID=='113') &
                    #                                                                 (docs.CUSTOMER_ID.notin(q3)) &
                    #                                                                 (docs.VERIFICATION_STATUS=='Y'))
                    #q = Query.from_(custcred).select(functions.Count(custcred.CUSTOMER_ID).distinct().as_("c"))
                    #fdata = db.runQuery(q.where(custcred.CUSTOMER_ID.isin(q2)))
                    #fdata = fdata["data"][0]["c"]
                    #q3 = Query.from_(income).select(income.CUSTOMER_ID, functions.Max(income.WEEK).as_("maxdate")).groupby(income.CUSTOMER_ID)
                    #q2 = Query.from_(income).join(q3).on_field("CUSTOMER_ID").select(functions.Count(income.CUSTOMER_ID).distinct().as_("c"))
                    #sdata = db.runQuery(q2.where(q3.maxdate<days_20))["data"]
                    #sdata = sdata[0]["c"] if sdata else 0
                    as_of_date = ((input_dict["data"]["as_of_date"] if input_dict["data"]["as_of_date"] else datetime.now().strftime("%Y-%m-%d"))
                                  if "as_of_date" in input_dict["data"] else datetime.now().strftime("%Y-%m-%d"))
                    bckoffice_data = db.runQuery(Query.from_(data).select(data.star).where(
                        functions.Date(data.CREATED_DATE) == as_of_date))["data"]
                    if not bckoffice_data:
                        bckoffice_data = db.runQuery(Query.from_(
                            data).select(data.star).limit(1))["data"]
                        bckoffice_data = [
                            {k: 0 for k, v in six.iteritems(bckoffice_data[0])}]
                    q = Query.from_(tasks).select(functions.Count(tasks.star).as_('C')).where(
                        tasks.LOGIN_ID == input_dict["msgHeader"]["authLoginID"])
                    q1 = q.where((tasks.TASK_DATETIME >= int(datetime.now().date().strftime("%s"))) & (tasks.STATUS.notin(["COMPLETED", "CANCEL"]) &
                                                                                                       (tasks.TASK_DATETIME < int((datetime.now()+timedelta(days=1)).date().strftime("%s")))))
                    tTasks = db.runQuery(q1.orderby(
                        tasks.TASK_DATETIME))["data"]
                    tTasks = tTasks[0]["C"] if tTasks else 0
                    yTasks = db.runQuery(q.where((tasks.TASK_DATETIME < int(datetime.now().date().strftime("%s"))) &
                                                 (tasks.STATUS.notin(["COMPLETED", "CANCEL"]))).orderby(tasks.TASK_DATETIME))
                    yTasks = yTasks["data"][0]["C"] if yTasks["data"] else 0
                    uTasks = db.runQuery(q.where((tasks.TASK_DATETIME > int(datetime.now().date().strftime("%s"))) &
                                                 (tasks.STATUS.notin(["COMPLETED", "CANCEL"]))).orderby(tasks.TASK_DATETIME))
                    uTasks = uTasks["data"][0]["C"] if uTasks["data"] else 0
                    token = generate(dbw).AuthToken()
                    if token["updated"]:
                        output_dict["data"].update(
                            {"todaysTasks": tTasks, "pendingTasks": yTasks, "upcomingTasks": uTasks})
                        output_dict["data"].update(utils.camelCase(
                            bckoffice_data[0]) if bckoffice_data else {})
                        output_dict["data"].update(
                            {"error": 0, "message": success})
                        output_dict["msgHeader"]["authToken"] = token["token"]
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["token"]})
                resp.body = json.dumps(output_dict)
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
