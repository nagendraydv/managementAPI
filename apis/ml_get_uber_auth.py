from __future__ import absolute_import
import falcon
import json
import requests
import grequests
import inspect
from uber_rides.auth import AuthorizationCodeGrant
from uber_rides.session import Session
from uber_rides.client import UberRidesClient
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType, functions
import six
from six.moves import range


class GetUberAuthResource:

    def insert_payment_row(self, ele, db=None):
        try:
            header = ["PAYMENT_ID", "CATEGORY", "PARTNER_AMOUNT", "AUTH_ID", "CASH_COLLECTED", "CURRENCY", "EVENT_TIME",
                      "SERVICE_FEE", "OTHER", "TOLL", "RIDER_FEES", "CREATED_DATE"]  # , "CUSTOMER_ID"]#AUTH_ID#PARTNER_ID, DRIVER_ID
            keys = ["payment_id", "category", "amount", "auth_id",
                    "cash_collected", "currency_code"]  # auth_id#partner_id, driver_id
            values = []
            fields = ' (`' + '`, `'.join(header) + '`)'
            c = 0
            for ele2 in ele:
                values.append(tuple([((str(ele2[key]) if ele2[key] is not None else None) if key in ele2 else None) for key in keys] + [datetime.fromtimestamp(ele2["event_time"]).strftime("%Y-%m-%d %H:%M:%S") if "event_time" in ele2 else None, (((str(int(100*(ele2["breakdown"]["service_fee"]))) if ele2["breakdown"]["service_fee"] is not None else None) if ("service_fee" in ele2["breakdown"]) else None) if ("breakdown" in ele2) else None), (((str(int(100*(ele2["breakdown"]["other"] if type(ele2["breakdown"]["other"]) != dict else sum(
                    ele2["breakdown"]["other"].values())))) if ele2["breakdown"]["other"] is not None else None) if ("other" in ele2["breakdown"]) else None) if ("breakdown" in ele2) else None), (((str(int(100*ele2["breakdown"]["toll"])) if ele2["breakdown"]["toll"] is not None else None) if ("toll" in ele2["breakdown"]) else None) if ("breakdown" in ele2) else None), (str(int(100*sum(ele2["rider_fees"].values())) if type(ele2["rider_fees"]) == dict else 0) if ("rider_fees" in ele2) else None), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]))  # , str(custID)]))
                c += 1
                if c > 3000:
                    db.dictcursor.executemany("insert ignore into " + 'mw_company_3.mw_unregistered_payments_info' +
                                              fields + " values (" + " ,".join(['%s' for _ in header]) + ")", values)
                    db.mydb.commit()
                    c = 0
                    values = []
            db.dictcursor.executemany("insert ignore into " + 'mw_company_3.mw_unregistered_payments_info' +
                                      fields + " values (" + " ,".join(['%s' for _ in header]) + ")", values)
            db.mydb.commit()
        except:
            raise  # print ele #pass

    def insert_trip_row(ele, db=None):
        try:
            header = ["TRIP_ID", "DRIVER_AUTO_ID", "FARE", "DISTANCE", "STATUS", "DURATION", "SURGE_MULTIPLIER", "START_CITY",
                      "CITY_LAT", "CITY_LONG", "DROPOFF", "PICKUP", "TRIP_DATETIME", "CREATED_DATE"]  # AUTH_ID#, "CUSTOMER_ID","DRIVER_ID"]
            keys = ["trip_id", "driver_auto_id", "fare", "distance",
                    "status", "duration", "surge_multiplier"]  # auth_id
            values = []
            fields = ' (`' + '`, `'.join(header) + '`)'
            c = 0
            for ele2 in ele:
                values.append(tuple([((str(ele2[key]) if ele2[key] is not None else None) if key in ele2 else None) for key in keys] + [(ele2["start_city"]["display_name"] if "display_name" in ele2["start_city"] else None) if "start_city" in ele2 else None, (str(ele2["start_city"]["latitude"]) if "latitude" in ele2["start_city"] else None) if "start_city" in ele2 else None, (str(ele2["start_city"]["longitude"]) if "longitude" in ele2["start_city"] else None)
                                                                                                                                        if "start_city" in ele2 else None, datetime.fromtimestamp(ele2["dropoff"]["timestamp"]).strftime("%Y-%m-%d %H:%M:%S") if ele2["dropoff"] else None, datetime.fromtimestamp(ele2["pickup"]["timestamp"]).strftime("%Y-%m-%d %H:%M:%S") if ele2["pickup"] else None, (datetime.fromtimestamp(ele2["status_changes"][-1]["timestamp"]).strftime("%Y-%m-%d %H:%M:%S") if ele2["status_changes"] else None), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]))  # , str(custID)]))
                c += 1
                if c > 3000:
                    db.dictcursor.executemany("insert ignore into " + 'mw_company_3.mw_unregistered_trips_info' +
                                              fields + " values (" + " ,".join(['%s' for _ in header]) + ")", values)
                    db.mydb.commit()
                    c = 0
                    values = []
            db.dictcursor.executemany("insert ignore into " + 'mw_company_3.mw_unregistered_trips_info' +
                                      fields + " values (" + " ,".join(['%s' for _ in header]) + ")", values)
            db.mydb.commit()
        except:
            raise  # pass#raise

    def on_get(self, req, resp):
        """Handles GET requests"""

        try:
            usession = Table("mw_company_login_session", schema="mint_loan")
            prof = Table("mw_profile_info", schema="mw_company_3")
            custProf = Table("mw_client_profile", schema="mint_loan")
            uauth = Table("mw_authorization_dump", schema="mw_company_3")
            custcredm = Table(
                "mw_customer_login_credentials_map", schema="mint_loan")
            refral = Table("mw_user_reference_details", schema="mint_loan")
            lm = Table("mw_client_loan_master", schema="mint_loan")
            ll = Table("mw_client_loan_limit", schema="mint_loan")
            lp = Table("mw_finflux_loan_product_master", schema="mint_loan")
            cdata = Table("mw_customer_data", schema="mint_loan")
            cprof = Table("mw_client_profile", schema="mint_loan")
            inc = Table("mw_driver_income_data_new", schema="mint_loan")
            uuidMap = Table("mw_uber_uuid_mapping", schema="mint_loan")
            uuidm = Table("mw_driver_uuid_master", schema="mint_loan")
            unregData = Table("mw_unregistered_data_dump",
                              schema="mw_company_3")
            drivs = Table("mw_unregistered_driver_id_mapping",
                          schema="mw_company_3")
            url = "https://login.uber.com/oauth/v2/token"
            logInfo = {'api': 'getUberAuth'}
            utils.logger.debug((",".join("%s:%s" % (k, v)
                                         for k, v in six.iteritems(req.params))), extra=logInfo)
            if "error" in req.params:
                # print req.params
                utils.logger.debug(
                    "error while fetching access token. Error - " + req.params["error"], extra=logInfo)
                baseurl2 = "https://www.supermoney.in/supermoney_forms/#/uber_loader?" if req.params[
                    "state"] != "D1MOFF" else "https://www.supermoney.in/uber_connect/#/?"
                stdError = "There was an Error in processing your request. Kindly retry in sometime"
                grantError = "We did not receive permissions to get your data. Kindly grant permissions to Supermoney in Uber app"
                payload = {"message": grantError if req.params["error"] ==
                           "access_denied" else stdError, "error": 1, "name": "", "email": "", "mobNumber": ""}
                qparams = "".join(("&"+key+"="+str(value))
                                  for key, value in six.iteritems(payload))[1:]
                raise falcon.HTTPStatus(falcon.HTTP_302, headers={
                                        "Location": baseurl2 + qparams})
            else:
                # print req.params
                acode = req.params["code"] if "code" in req.params else ''
                state = req.params["state"] if "state" in req.params else ''
                # filename="mysql-uat.config", path='/home/centos/mintwalk/mintloanAdmin/.')
                devdb = db = DB()
                gen = generate(db)
                table = "mw_python_uber_api_integration_log"
                gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                ).f_lineno, logMessage=(",".join("%s:%s" % (k, v) for k, v in six.iteritems(req.params))))
                q = devdb.runQuery(Query.from_(usession).select("CUSTOMER_ID", "REQUEST_ID").where(usession.STATE == state))[
                    "data"] if state != 'D1MOFF' else [{"CUSTOMER_ID": "uber_app", "REQUEST_ID": "uber_app"}]
                custID, reqID = (q[0]["CUSTOMER_ID"], q[0]
                                 ["REQUEST_ID"]) if q else ("", "")
                gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                ).f_lineno, logMessage="customer id:%s" % custID)
                gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                ).f_lineno, logMessage="Generating auth token and refresh token from the access code obtained for the user: " + acode)
                utils.logger.debug("customer id:%s" % custID, extra=logInfo)
                utils.logger.debug(
                    "Access code obtained for the user: " + acode, extra=logInfo)
                utils.logger.debug(
                    "Generating the auth token and refresh token using the access code", extra=logInfo)
                payload = {"client_id": "U4XCFbyEXwwQ0TF0oLveLcXz-Vo_ddkn", "client_secret": "JnH50ymytYbXqceXnUMSnY_Qf99rtHTQ6YcZVVJ_",
                           "grant_type": "authorization_code", "code": acode, "redirect_uri": "https://smart-backend.mintwalk.com/mlGetUberAuth"}
                utils.logger.debug("Post api url: " + url, extra=logInfo)
                gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                ).f_lineno, logMessage="Request: "+json.dumps(payload))
                utils.logger.debug(
                    "Request: " + json.dumps(payload), extra=logInfo)
                r = requests.post(url, data=payload)
                gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                ).f_lineno, logMessage="Response: "+json.dumps(r.json()))
                utils.logger.debug(
                    "Response: " + json.dumps(r.json()), extra=logInfo)
                q = Query.from_(uauth).select(
                    functions.Count(uauth.star).as_("c"))
                q = q.where(((uauth.CUSTOMER_ID == str(custID)) & (
                    uauth.CONFIRMED_CUSTOMER_ID.isnull())) | (uauth.CONFIRMED_CUSTOMER_ID == str(custID)))
                exist = devdb.runQuery(q)["data"] if custID != "uber_app" else [
                    {"c": -1}]
                if r.status_code == 200:
                    resp_json = r.json()
                    cust_headers = {"content-type": "application/json",
                                    "Authorization": "Bearer "+resp_json["access_token"]}
                    utils.logger.debug(
                        "Get api url: https://api.uber.com/v1/partners/me", extra=logInfo)
                    loanExist = devdb.runQuery(Query.from_(lm).select(functions.Count(
                        lm.CUSTOMER_ID).as_("c")).where(lm.CUSTOMER_ID == custID))["data"]
                    loanExist = loanExist[0]["c"] > 0
                    baseurl = "https://api.uber.com/v1/partners/payments"
                    baseurl2 = "https://api.uber.com/v1/partners/trips"
                    payUrl21 = baseurl+"?limit=50&from_time=%s" % ((datetime.now()-timedelta(days=21)).date(
                    ).strftime("%s")) + "&to_time=%s" % ((datetime.now()-timedelta(days=11)).date().strftime("%s"))
                    payUrl11 = baseurl+"?limit=50&from_time=%s" % ((datetime.now()-timedelta(days=11)).date(
                    ).strftime("%s")) + "&to_time=%s" % ((datetime.now()-timedelta(days=1)).date().strftime("%s"))
                    tripUrl1 = baseurl2 + \
                        "?limit=1&from_time=%s" % datetime(
                            2000, 1, 1).strftime("%s")
                    urls = ["https://api.uber.com/v1/partners/me",
                            "https://api.uber.com/v1/partners/me/rewards/tier", payUrl21, payUrl11, tripUrl1]
                    rs = (grequests.get(u, headers=cust_headers) for u in (
                        urls if (not loanExist) or (custID == "uber_app") else urls[0:2]))
                    resps = grequests.map(rs)
                    resp2, resp3 = ((resps[0].json() if resps[0].status_code == 200 else {}) if resps[0] else {
                    }), ((resps[1].json() if resps[1].status_code == 200 else {}) if resps[1] else {})
                    resp4 = (resps[4].json() if resps[4].status_code == 200 else {}) if (
                        not loanExist) else {}
                    offset = (resp4["count"] - 2) if "count" in resp4 else 0
                    offset = offset if offset > 0 else 0
                    tier = ([x for x in resp3["tiers"] if (x["tier"] if "tier" in x else None) == resp3["current_tier"]]
                            if (("current_tier" in resp3) & ("tiers" in resp3)) else None)
                    tier = (
                        tier[0]["display_name"] if "display_name" in tier[0] else None) if tier else None
                    gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                    ).f_lineno, logMessage="Profile api response: "+json.dumps(resps[0].json()))
                    utils.logger.debug(
                        "Response: " + json.dumps(resps[0].json()), extra=logInfo)
                    # & (resps[2].status_code==200) & (resps[3].status_code==200):
                    if (not loanExist) and (custID != "uber_app") and (resps[0].status_code == 200):
                        cust_payments = ((resps[2].json()["payments"] if resps[2].status_code == 200 else [
                        ]) + (resps[3].json()["payments"] if resps[3].status_code == 200 else [])) if len(resps) >= 4 else []
                        urls2 = [baseurl2 + "?offset=%s&limit=1&from_time=%s" % (offset, datetime(2000, 1, 1).strftime("%s"))] + [payUrl21.replace("?", "?offset=%s&" % i) for i in range(50, (resps[2].json(
                        )["count"] if "count" in resps[2].json() else 100), 50)] + [payUrl11.replace("?", "?offset=%s&" % i) for i in range(50, (resps[3].json()["count"] if "count" in resps[3].json() else 100), 50)]
                        rs2 = (grequests.get(u, headers=cust_headers)
                               for u in urls2)
                        resps2 = grequests.map(rs2)
                        resp = resps2[0].json(
                        ) if resps2[0].status_code == 200 else {}
                        timest = ((((resp["trips"][0]["status_changes"][-1] if resp["trips"][0]["status_changes"] else None)
                                    if "status_changes" in resp["trips"][0] else None) if resp["trips"] else 0) if "trips" in resp else None)
                        # default assumes that the driver is driving for 6 months if the data cannot be fetched
                        nDaysDriving = ((datetime.now()-datetime.utcfromtimestamp(
                            timest["timestamp"])).days if "timestamp" in timest else 200) if timest else 200
                        timest = (datetime.utcfromtimestamp(timest["timestamp"]).strftime(
                            "%Y-%m-%d") if "timestamp" in timest else None) if timest else None
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="FIRST TRIP WEEK: " + (timest if timest else ''))
                        utils.logger.debug(
                            "FIRST TRIP WEEK: " + (timest if timest else ''), extra=logInfo)
                        for resp in resps2[1:]:
                            cust_payments += (resp.json()["payments"]
                                              if resp.status_code == 200 else [])
                        threeWeekAverage = 4.25 * \
                            sum(ele["amount"] for ele in cust_payments)/3.
                        lastWeekEpoch = (int((datetime.now(
                        )-timedelta(days=7)).strftime("%s"))), int(datetime.now().strftime("%s"))
                        lastWeekIncome = 4.25*sum(ele["amount"] for ele in [x for x in cust_payments if x["event_time"] < max(
                            lastWeekEpoch) and x["event_time"] > min(lastWeekEpoch)])
                        custData = db.runQuery(Query.from_(custProf).select(
                            custProf.CUSTOMER_DATA).where(custProf.CUSTOMER_ID == custID))["data"]
                        try:
                            custData = json.loads(
                                custData[0]["CUSTOMER_DATA"]) if custData else None
                        except:
                            custData = {}
                        if custData:
                            custData.update({"oneWeekIncome": int(lastWeekIncome), "driverCount": len(set(
                                [ele["driver_id"] for ele in cust_payments])), "userCategory": tier, "monthlyIncome": int(threeWeekAverage), "experience": int(nDaysDriving/30)})
                            db.Update(db="mint_loan", table="mw_client_profile", conditions={
                                      "CUSTOMER_ID=": str(custID)}, CUSTOMER_DATA=json.dumps(custData))
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="Three week income: " + str(threeWeekAverage))
                        utils.logger.debug(
                            "Three week income: " + str(threeWeekAverage), extra=logInfo)
                        mapp = {"Diamond": 10000, "Platinum": 10000,
                                "Gold": 5000, "Blue": 5000}
                        city = devdb.runQuery(Query.from_(cprof).select(
                            cprof.CURRENT_CITY, cprof.COMPANY_NAME).where(cprof.CUSTOMER_ID == custID))["data"]
                        pilotCity = (city[0]["CURRENT_CITY"].lower() not in (
                            'chennai') if city[0]["CURRENT_CITY"] else False) if city else False
                        ndrivers = len(set([ele["driver_id"]
                                            for ele in cust_payments]))
                        loanLimit = 10000 if ((threeWeekAverage > 125000) and (
                            pilotCity) and ndrivers >= 2) else None
                        loanLimit = ((mapp[tier] if tier in mapp else 5000 if nDaysDriving >= 180 else 2500) if threeWeekAverage >
                                     13000 else 5000 if nDaysDriving >= 180 else 2500) if loanLimit is None else loanLimit
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="Derived loan limit for the customer: " + (str(loanLimit) if loanLimit else "0"))
                        utils.logger.debug("Derived loan limit for the customer: " + (
                            str(loanLimit) if loanLimit else "0"), extra=logInfo)
                        refCode = db.runQuery(Query.from_(refral).join(custcredm, how=JoinType.left).on_field(
                            "LOGIN_ID").select("REFER_CODE").where(custcredm.CUSTOMER_ID == custID))["data"]
                        insLimit = 0 if ((refCode[0]["REFER_CODE"].lower(
                        ) != "acemoto") if refCode else True) else 10000 if threeWeekAverage > 30000 else 10000 if threeWeekAverage > 25000 else 5000 if threeWeekAverage > 20000 else 0
                        loanLimit = 0 if ((refCode[0]["REFER_CODE"].lower(
                        ) == "acemoto") if refCode else False) else loanLimit
                        limitExist = devdb.runQuery(Query.from_(ll).select(functions.Count(
                            ll.CUSTOMER_ID).as_("c")).where(ll.CUSTOMER_ID == str(custID)))["data"]
                        # and ((refCode[0]["REFER_CODE"].lower()!="acemoto") if refCode else True):
                        if (limitExist[0]["c"] > 0) and ((city[0]["COMPANY_NAME"] == "UBER") if city else False):
                            devdb.Update(db="mint_loan", table="mw_client_loan_limit", conditions={"CUSTOMER_ID=": str(
                                custID)}, LOAN_LIMIT=str(loanLimit), INSURANCE_LOAN_LIMIT=str(insLimit))
                        # and ((refCode[0]["REFER_CODE"].lower()!="acemoto") if refCode else True):
                        elif (limitExist[0]["c"] == 0) and ((city[0]["COMPANY_NAME"] == "UBER") if city else False):
                            devdb.Insert(db="mint_loan", table="mw_client_loan_limit", CUSTOMER_ID=str(custID), LOAN_LIMIT=str(loanLimit), INSURANCE_LOAN_LIMIT=str(
                                insLimit), CREATED_BY='UBER_LOGIN_API', CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), compulsory=False, date=False, noIgnor=False)
                        if ((city[0]["COMPANY_NAME"] == "UBER") if city else False):
                            devdb.Insert(db="mint_loan", table="mw_customer_change_log", CUSTOMER_ID=str(custID), DATA_KEY='LOAN_LIMIT',
                                         DATA_VALUE=str(loanLimit), CREATED_BY='UBER_LOGIN_API', compulsory=False, date=False, noIgnor=False,
                                         COMMENTS=('3wkAvg:%.1f, tier:%s, ndrivers:%s, drivingSince:%s' % (threeWeekAverage if threeWeekAverage else 0,
                                                                                                           tier, ndrivers, (timest if timest else '')) +
                                                   ',city:%s' % (city[0]["CURRENT_CITY"] if city else '')),
                                         CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    driver_id = (resp2["driver_id"]
                                 if "driver_id" in resp2 else '')
                    first_name = (resp2["first_name"].encode(
                        "ascii", "ignore") if "first_name" in resp2 else '')
                    last_name = (resp2["last_name"].encode(
                        "ascii", "ignore") if "last_name" in resp2 else '')
                    name = first_name + " " + last_name
                    phNo = (resp2["phone_number"]
                            if "phone_number" in resp2 else '')
                    exist = db.runQuery(Query.from_(cdata).select(functions.Count(cdata.CUSTOMER_ID).as_(
                        "c")).where((cdata.CUSTOMER_ID == custID) & (cdata.DATA_VALUE == phNo[-10:])))
                    loginID = db.runQuery(Query.from_(custcredm).select(
                        custcredm.LOGIN_ID).where(custcredm.CUSTOMER_ID == custID))["data"]
                    ut = utils()
                    respDict = ut.store_customer_data(dataKey="COMPANY_NUMBER", dataValue=phNo, loginId=loginID[0]["LOGIN_ID"], adminId="ADMIN") if (
                        loginID != []) and (exist["data"][0]["c"] == 0) else False
                    email = (resp2["email"].encode(
                        "ascii", "ignore") if "email" in resp2 else '')
                    q = Query.from_(prof).select("DRIVER_ID", "FIRST_NAME", "LAST_NAME", "PHONE_NUMBER").where(
                        (prof.CUSTOMER_ID == str(custID)) & (prof.CONFIRMED_CUSTOMER_ID.isnull()))
                    temp = {"DRIVER_ID": driver_id, "FIRST_NAME": first_name,
                            "LAST_NAME": last_name, "PHONE_NUMBER": phNo}
                    devdb._DbClose_()
                    devdb = db = DB()
                    gen = generate(db)
                    inserted = True
                    exist = devdb.runQuery(q.orderby(prof.AUTO_ID, order=Order.desc))["data"] if (
                        resps[0].status_code == 200) & (custID != "uber_app") else [temp]
                    if (exist[0] != temp if exist else True):
                        inserted = devdb.Insert(db="mw_company_3", table="mw_profile_info", date=False, CUSTOMER_ID=str(custID), TIER=tier,
                                                DRIVER_ID=driver_id, FIRST_NAME=first_name, LAST_NAME=last_name, PHONE_NUMBER=phNo,
                                                RATING=(
                                                    str(resp2["rating"]) if "rating" in resp2 else None),
                                                EMAIL=(
                                                    resp2["email"] if "email" in resp2 else None),
                                                ACTIVATION_STATUS=(
                                                    resp2["activation_status"] if "activation_status" in resp2 else None),
                                                PROMO_CODE=(
                                                    resp2["promo_code"] if "promo_code" in resp2 else None),
                                                CREATED_BY="Admin", CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), compulsory=False)
                        devdb.Insert(db="mw_company_3", table="mw_authorization_dump", ACCESS_TOKEN=resp_json["access_token"], date=False,
                                     CUSTOMER_ID=str(custID), REFRESH_TOKEN=resp_json["refresh_token"], EXPIRES_IN=str(resp_json["expires_in"]),
                                     CREATED_BY="Admin", CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                     compulsory=False)
                    if custID != "uber_app":
                        # if inserted else {"message":"your partner profile already mapped with another customer id", "error":1}
                        payload = {"message": "success", "error": 0, "name": name,
                                   "email": email, "mobNumber": phNo[-10:], "tier": tier}
                    else:  # following section mapps SM customer id if exist for requests that are originating from uber app
                        # devdb._DbClose_()
                        # devdb=db=DB()
                        #gen = generate(db)
                        q = devdb.runQuery(Query.from_(uuidMap).join(uuidm, how=JoinType.left).on(uuidMap.UUID == uuidm.DRIVER_UUID).select(
                            uuidm.CUSTOMER_ID).where(uuidMap.DRIVER_ID == driver_id).where(uuidm.CUSTOMER_ID.notnull()))["data"]
                        if not q:
                            q = devdb.runQuery(Query.from_(prof).select(prof.CONFIRMED_CUSTOMER_ID.as_("CUSTOMER_ID")).where(
                                (prof.DRIVER_ID == driver_id) & ((prof.CONFIRMED_CUSTOMER_ID != 0) & (prof.CONFIRMED_CUSTOMER_ID.notnull()))))["data"]
                        if not q:
                            q = devdb.runQuery(Query.from_(prof).select(prof.CONFIRMED_CUSTOMER_ID.as_("CUSTOMER_ID")).distinct(
                            ).where((prof.DRIVER_ID == driver_id) & (prof.CONFIRMED_CUSTOMER_ID != 0)))["data"]
                        if len(q) != 1:
                            q = devdb.runQuery(Query.from_(inc).select(inc.CUSTOMER_ID).where(
                                (inc.CONTACT_NUMBER == phNo[-10:]) & (inc.CUSTOMER_ID != 0)))["data"]
                        if not q:
                            q = devdb.runQuery(Query.from_(cdata).select(cdata.CUSTOMER_ID).where(
                                (cdata.DATA_VALUE == phNo) & (cdata.DATA_KEY == "COMPANY_NUMBER")))["data"]
                        if not q:
                            q = devdb.runQuery(Query.from_(custcredm).select(custcredm.CUSTOMER_ID).where(
                                (custcredm.LOGIN_ID == phNo) & (custcredm.ACTIVE == 1)))["data"]
                        custIdDerived = q[0]["CUSTOMER_ID"] if q else None
                        loginID = devdb.runQuery(Query.from_(custcredm).select(custcredm.LOGIN_ID).where(
                            (custcredm.CUSTOMER_ID == custIdDerived) & (custcredm.ACTIVE == 1)))["data"]
                        loginID = loginID[0]["LOGIN_ID"] if loginID else "0"
                        loanType = devdb.runQuery(Query.from_(lm).join(lp, how=JoinType.left).on(lm.LOAN_PRODUCT_ID == lp.PRODUCT_ID).select(
                            lp.LIMIT_TYPE).where((lm.CUSTOMER_ID == custIdDerived) & (lm.STATUS == 'ACTIVE')))["data"]
                        loanType = loanType[0]["LIMIT_TYPE"] if loanType else '0'
                        loanType = (loanType.split("LOAN_LIMIT")[0] if loanType.split(
                            "LOAN_LIMIT")[0] else "PERSONAL") if loanType else '0'
                        cust_payments = ((resps[2].json()["payments"] if resps[2].status_code == 200 else [
                        ]) + (resps[3].json()["payments"] if resps[3].status_code == 200 else [])) if len(resps) >= 4 else []
                        urls2 = [baseurl2 + "?offset=%s&limit=1&from_time=%s" % (offset, datetime(2000, 1, 1).strftime("%s"))] + [payUrl21.replace("?", "?offset=%s&" % i) for i in range(50, (resps[2].json(
                        )["count"] if "count" in resps[2].json() else 100), 50)] + [payUrl11.replace("?", "?offset=%s&" % i) for i in range(50, (resps[3].json()["count"] if "count" in resps[3].json() else 100), 50)]
                        rs2 = (grequests.get(u, headers=cust_headers)
                               for u in urls2)
                        resps2 = grequests.map(rs2)
                        resp = resps2[0].json(
                        ) if resps2[0].status_code == 200 else {}
                        timest = ((((resp["trips"][0]["status_changes"][-1] if resp["trips"][0]["status_changes"] else None)
                                    if "status_changes" in resp["trips"][0] else None) if resp["trips"] else 0) if "trips" in resp else None)
                        # default assumes that the driver is driving for 6 months if the data cannot be fetched
                        nDaysDriving = ((datetime.now()-datetime.utcfromtimestamp(
                            timest["timestamp"])).days if "timestamp" in timest else 200) if timest else 200
                        timest = (datetime.utcfromtimestamp(timest["timestamp"]).strftime(
                            "%Y-%m-%d") if "timestamp" in timest else None) if timest else None
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="FIRST TRIP WEEK: " + (timest if timest else ''))
                        utils.logger.debug(
                            "FIRST TRIP WEEK: " + (timest if timest else ''), extra=logInfo)
                        for resp in resps2[1:]:
                            cust_payments += (resp.json()["payments"]
                                              if resp.status_code == 200 else [])
                        threeWeekAverage = 4.25 * \
                            sum(ele["amount"] for ele in cust_payments)/3.
                        lastWeekEpoch = (int((datetime.now(
                        )-timedelta(days=7)).strftime("%s"))), int(datetime.now().strftime("%s"))
                        lastWeekIncome = 4.25*sum(ele["amount"] for ele in [x for x in cust_payments if x["event_time"] < max(
                            lastWeekEpoch) and x["event_time"] > min(lastWeekEpoch)])
                        custData = db.runQuery(Query.from_(custProf).select(
                            custProf.CUSTOMER_DATA).where(custProf.CUSTOMER_ID == custID))["data"]
                        try:
                            custData = json.loads(
                                custData[0]["CUSTOMER_DATA"]) if custData else {}
                        except:
                            custData = {}
                        if custData:
                            custData.update({"oneWeekIncome": int(lastWeekIncome), "driverCount": len(set(
                                [ele["driver_id"] for ele in cust_payments])), "userCategory": tier, "monthlyIncome": int(threeWeekAverage), "experience": int(nDaysDriving/30)})
                            db.Update(db="mint_loan", table="mw_client_profile", conditions={
                                      "CUSTOMER_ID=": str(custID)}, CUSTOMER_DATA=json.dumps(custData))
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="Three week income: " + str(threeWeekAverage))
                        utils.logger.debug(
                            "Three week income: " + str(threeWeekAverage), extra=logInfo)
                        mapp = {"Diamond": 10000, "Platinum": 10000,
                                "Gold": 5000, "Blue": 5000, "": 5000, None: 5000}
                        loanLimit = (mapp[tier] if tier in mapp else 5000) if (threeWeekAverage > 20000 and threeWeekAverage <
                                                                               90000) else 10000 if threeWeekAverage > 90000 else 5000 if threeWeekAverage >= 10000 else 2500 if threeWeekAverage > 1500 else 0
                        url2 = "https://login.uber.com/oauth/v2/authorize?response_type=code&client_id=U4XCFbyEXwwQ0TF0oLveLcXz-Vo_ddkn&"
                        url2 += "scope=partner.accounts+partner.payments+partner.rewards+partner.trips&"
                        url2 += "redirect_uri=https%3A%2F%2Fsmart-backend.mintwalk.com%2FmlGetUberAuth&state=D1MOFF"
                        if custIdDerived:
                            inserted = db.Insert(db="mint_loan", table="mw_company_login_session", compulsory=False, date=False, COMPANY_ID="3",
                                                 CUSTOMER_ID=str(custIdDerived), STATE='D1MOFF', URL_GENERATED=url2, LOGIN_SUCCESS="1",
                                                 CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        exist = devdb.runQuery(Query.from_(unregData).select(functions.Count(
                            unregData.PARTNER_ID).as_("c")).where(unregData.PARTNER_ID == driver_id))["data"]
                        if exist[0]["c"] == 0:
                            junk = devdb.Insert(db="mw_company_3", table="mw_unregistered_data_dump", compulsory=False, date=False, noIgnor=False,
                                                THREE_WEEK_AVERAGE="%.1f" % (threeWeekAverage), PARTNER_ID=driver_id, FIRST_NAME=first_name,
                                                LAST_NAME=last_name, RATING=(str(resp2["rating"]) if "rating" in resp2 else None), TIER=tier,
                                                CUSTOMER_ID=str(custIdDerived) if custIdDerived else None, PHONE_NUMBER=phNo,
                                                EMAIL=(resp2["email"] if "email" in resp2 else None), EXPIRES_IN=str(resp_json["expires_in"]),
                                                ACTIVATION_STATUS=(
                                                    resp2["activation_status"] if "activation_status" in resp2 else None),
                                                PROMO_CODE=(resp2["promo_code"] if "promo_code" in resp2 else None), FIRST_TRIP_WEEK=timest,
                                                ACCESS_TOKEN=resp_json["access_token"], REFRESH_TOKEN=resp_json["refresh_token"],
                                                CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        else:
                            junk = devdb.Update(db="mw_company_3", table="mw_unregistered_data_dump", FIRST_NAME=first_name, LAST_NAME=last_name,
                                                PHONE_NUMBER=phNo, RATING=(str(resp2["rating"]) if "rating" in resp2 else None), TIER=tier,
                                                EMAIL=(resp2["email"] if "email" in resp2 else None), EXPIRES_IN=str(resp_json["expires_in"]),
                                                ACTIVATION_STATUS=(
                                                    resp2["activation_status"] if "activation_status" in resp2 else None),
                                                PROMO_CODE=(resp2["promo_code"] if "promo_code" in resp2 else None), FIRST_TRIP_WEEK=timest,
                                                ACCESS_TOKEN=resp_json["access_token"], REFRESH_TOKEN=resp_json["refresh_token"],
                                                CUSTOMER_ID=str(
                                                    custIdDerived) if custIdDerived else None,
                                                THREE_WEEK_AVERAGE="%.1f" % (
                                                    threeWeekAverage),
                                                MODIFIED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), conditions={"PARTNER_ID=": driver_id})
                        authID = devdb.runQuery(Query.from_(unregData).select(
                            unregData.AUTO_ID).where(unregData.PARTNER_ID == driver_id))["data"]
                        for cp in cust_payments:
                            cp["amount"] = "%i" % (
                                int(100*cp["amount"]) if cp["amount"] else 0)
                            cp["cash_collected"] = "%i" % (
                                int(100*cp["cash_collected"]) if cp["cash_collected"] else 0)
                            cp.update(
                                {"auth_id": str(authID[0]["AUTO_ID"]) if authID else "0"})
                        self.insert_payment_row(cust_payments, devdb)
                        #driversMapped = devdb.runQuery(Query.from_(drivs).select(drivs.star).where(drivs.AUTH_ID==str(authID[0]["AUTO_ID"]) if authID else "0"))["data"]
                        #existingDrivers = [dr["DRIVER_ID"] for dr in driversMapped]
                        #drivers = {dr["driver_id"] for dr in cust_trips if dr["driver_id"] not in existingDrivers}
                        # for dID in drivers:
                        #    devdb.Insert(db="mw_company_3", table="mw_unregistered_driver_id_mapping", AUTH_ID=str(authID[0]["AUTO_ID"]) if authID else "0", DRIVER_ID=dID, CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), date=False, compulsory=False, noIgnor=False)
                        #driversMapped = devdb.runQuery(Query.from_(drivs).select(drivs.star).where(drivs.AUTH_ID==str(authID[0]["AUTO_ID"]) if authID else "0"))["data"]
                        #driversMapped = {dr["DRIVER_ID"]:dr["AUTO_ID"] for dr in driversMapped}
                        # for cp in cust_trips:
                        #    cp.update({"driver_auto_id":driversMapped[cp.pop("driver_id")]})
                        #    cp["fare"]="%i"%(int(100*cp["fare"]) if cp["fare"] else 0)
                        #    cp["distance"]="%i"%(int(100*cp["distance"]) if cp["distace"] else 0)
                        #    cp["duration"]="%i"%(int(100*cp["duration"]) if cp["duration"] else 0)
                        #    cp["surge_multiplier"]="%i"%(int(100*cp["surge_multiplier"]) if cp["surge_multiplier"] else 0)
                        #insert_trip_row(cust_trips, devdb)
                        #loanLimit = ("10000" if tier=="Diamond" else "10000" if tier=="Platinum" else "5000" if tier in ("Gold", "Blue") else "5000") if threeWeekAverage>20000 else "2500" if threeWeekAverage>1000 else "0"
                        payload = {"message": "success", "error": 0, "name": name, "email": email,
                                   "mobNumber": phNo[-10:], "tier": tier, "loanLimit": loanLimit, "loginId": loginID[-10:], "loanType": loanType, "driverID": driver_id}
                    qparams = "".join(("&"+key+"="+str(value)) for key,
                                      value in six.iteritems(payload) if value is not None)[1:]
                    baseurl2 = "https://www.supermoney.in/supermoney_forms/#/uber_loader?" if custID != "uber_app" else "https://www.supermoney.in/uber_connect/#/?" if email not in (
                        'grahul+test+supermoney1@uber.com', 'grahul+test+supermoney2@uber.com', 'grahul+test+supermoney3@uber.com', 'grahul+test+kolkata+gold@uber.com', 'grahul+test+guwahati+blue@uber.com') else "https://www.supermoney.in/uber_connect_test/#/?"  # "http://192.168.5.27:8080/#/?"
                    if resps[0].status_code == 200:
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="Redirection URL: " + baseurl2 + qparams)
                        utils.logger.debug(
                            "Redirection URL: " + baseurl2 + qparams, extra=logInfo)
                        # https%3A%2F%2Fwww.mintwalk.com%2Fsupermoney_forms%2F%23%2Fuber_loader%3F" + qparams)#"https://www.mintwalk.com/supermoney_forms/#/uber_loader?" + qparams)
                        raise falcon.HTTPStatus(falcon.HTTP_302, headers={
                                                "Location": baseurl2+qparams})
                        #utils.logger.debug("Test if the statement gets logged after redirection", extra=logInfo)
                    else:
                        payload = {
                            "message": "You seemed to have entered a non-partner account. Kindly retry.", "error": 1}
                        qparams = "".join(("&"+key+"="+str(value))
                                          for key, value in six.iteritems(payload))[1:]
                        gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                        ).f_lineno, logMessage="Redirection URL: " + baseurl2 + qparams)
                        raise falcon.HTTPStatus(falcon.HTTP_302, headers={
                                                "Location": baseurl2 + qparams})
                    #resp.body = json.dumps({"message":"success", "error":0, "name":name, "email":email, "mobNumber":phNo})
                else:  # this else belongs to scenario when token exchange api gives error with status code other than 200
                    baseurl2 = "https://www.supermoney.in/supermoney_forms/#/uber_loader?" if custID != "uber_app" else "https://www.supermoney.in/uber_connect/#/?" if email not in (
                        'grahul+test+supermoney1@uber.com', 'grahul+test+supermoney2@uber.com', 'grahul+test+supermoney3@uber.com', 'grahul+test+kolkata+gold@uber.com', 'grahul+test+guwahati+blue@uber.com') else "https://www.supermoney.in/uber_connect_test/#/?"
                    resp_json = r.json()  # {"error":"invalid_grant"}#r.json()
                    stdError = "There was an Error in processing your request. Kindly retry in sometime"
                    grantError = "We did not receive permissions to get your data. Kindly grant permissions to Supermoney in Uber app"
                    errorDict = {"invalid_grant": grantError, "invalid_request": stdError, "invalid_client": stdError, "invalid_scope": stdError,
                                 "server_error": stdError, "temporarily_unavailable": stdError}
                    payload = {"message": (errorDict[(resp_json["error"] if resp_json["error"] in errorDict else "server_error")]
                                           if "error" in resp_json else "some error occurred. Please try again later"),
                               "error": 1}
                    qparams = "".join(("&"+key+"="+str(value))
                                      for key, value in six.iteritems(payload))[1:]
                    gen.DBlog(table=table, logFrom="getUberAuth", lineNo=inspect.currentframe(
                    ).f_lineno, logMessage="Redirection URL: " + baseurl2 + qparams)
                    utils.logger.debug("Redirection URL: " +
                                       baseurl2 + qparams, extra=logInfo)
                    raise falcon.HTTPStatus(falcon.HTTP_302, headers={
                                            "Location": baseurl2 + qparams})
                devdb._DbClose_()
        except Exception as ex:
            utils.logger.error("ExecutionError: ",
                               extra=logInfo, exc_info=True)
            # falcon.HTTPStatus(falcon.HTTP_302, headers={"Location":"https://www.supermoney.in/uber_connect/#/?error=1&message=some error occurred. Please try again later."})#falcon.HTTPError(falcon.HTTP_400,'Error', ex.message)
            raise
