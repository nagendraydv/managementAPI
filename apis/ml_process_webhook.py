from __future__ import absolute_import
import falcon
import json
import requests
from mintloan_utils import DB, generate, validate, utils, datetime, timedelta
from pypika import Query, Table, Order, JoinType


class ProcessWebhookResource:

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
            hh = req.headers
            logInfo = {'api': 'webhook'}
            utils.logger.debug("Request url: " +  req.url, extra=logInfo)
            utils.logger.debug("Request headers: " +  json.dumps(req.headers), extra=logInfo)
            # print req.url, req.headers
            input_dict = json.loads(raw_json, encoding='utf-8')
            utils.logger.debug("Request: " + json.dumps(input_dict), extra=logInfo)
            # print input_dict
            # print "-----------XXXXXXXXXXXXX-----------"
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
        try:
            if False:  # not validate.Request(api='', request=input_dict):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
                resp.body = json.dumps(output_dict)
            else:
                db = DB()
                inserted = db.Insert(db="mint_loan", table="mw_webhook_log", compulsory=False, date=False,
                                     AMOUNT=((input_dict["changes"]["transactionAmount"] if "transactionAmount" in input_dict["changes"]
                                              else None) if "changes" in input_dict else None),
                                     RESOURCE_ID_IN=str(
                                         input_dict["resourceId"]) if "resourceId" in input_dict else None,
                                     LOAN_ID_IN=str(
                                         input_dict["loanId"]) if "loanId" in input_dict else None,
                                     LENDER_IN=(hh["FINERACT-PLATFORM-TENANTID"].upper()
                                                if "FINERACT-PLATFORM-TENANTID" in hh else None),
                                     CREATED_DATE=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                     WEBHOOK_ACTION=hh["X-FINERACT-ACTION"] if "X-FINERACT-ACTION" in hh else None)
                if inserted:
                    db.dictcursor.execute("select last_insert_id() as auto_id")
                    auto_id = db.dictcursor.fetchall()
                    auto_id = (
                        auto_id[0]["auto_id"] if "auto_id" in auto_id[0] else 0) if auto_id else 0
                else:
                    auto_id = 0
                if False:  # val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    #            lm = Table("mw_client_loan_master", schema="mint_loan")
                    conf = Table("mw_configuration", schema="mint_loan_admin")
                    conf2 = Table("mw_configuration", schema="mint_loan")
                    lm = Table("mw_client_loan_master", schema="mint_loan")
                    whlog = Table("mw_webhook_log", schema="mint_loan")
                    baseurl = db.runQuery(Query.from_(conf).select(
                        "CONFIG_VALUE").where(conf.CONFIG_KEY == "MIFOS_URL"))
                    baseurl = baseurl["data"][0]["CONFIG_VALUE"]
                    headers = utils.finflux_headers["GETCLARITY"].copy()
                    auth = utils.mifos_auth
                    extID = db.runQuery(Query.from_(lm).select("FUND", "EXTERNAL_LOAN_ID").where(
                        lm.LOAN_REFERENCE_ID == str(input_dict["loanId"])))["data"]
                    loanID = extID[0]["EXTERNAL_LOAN_ID"] if extID else None
                    # print headers, baseurl + "loans/" + str(input_dict["loanId"]), extID, loanID
                    r = requests.get(baseurl + "loans/" + str(input_dict["loanId"]), headers=headers, auth=auth, verify=False) if (hh["X-FINERACT-ENTITY"] != "RESCHEDULELOAN" if "X-FINERACT-ENTITY" in hh else True) else requests.get(
                        baseurl + "rescheduleloans/" + str(input_dict["resourceId"] if "resourceId" in input_dict else 0), headers=headers, auth=auth, verify=False)
                    if (r.status_code == 200) and (loanID):
                        resp = r.json()
                        # (extID[0]["FUND"].lower() if extID[0]["FUND"] else None) if extID else None
                        headers["Fineract-Platform-TenantId"] = ("".join(x.lower(
                        ) for x in extID[0]["FUND"] if not x.isdigit()) if extID[0]["FUND"] else None) if extID else None
                        # print resp
                        posturl = baseurl + "loans/" + loanID
                        payload = {"locale": "en",
                                   "dateFormat": "dd MMMM yyyy"}
                        cmd = ''
                        if (hh["X-FINERACT-ENTITY"] == "RESCHEDULELOAN" if "X-FINERACT-ENTITY" in hh else False) and (hh["X-FINERACT-ACTION"] == "CREATE"):
                            posturl = baseurl + "rescheduleloans"
                            cmd = "reschedule"
                            payload.update({"submittedOnDate": datetime.now().strftime("%d %b %Y"), "rescheduleReasonId": 805,
                                            "rescheduleFromDate": datetime.strptime("-".join(str(x) for x in resp["rescheduleFromDate"]),
                                                                                    "%Y-%m-%d").strftime("%d %b %Y"),
                                            "adjustedDueDate": datetime.strptime("-".join(str(x) for x in resp["loanTermVariationsData"][0]["dateValue"]),
                                                                                 "%Y-%m-%d").strftime("%d %b %Y"),
                                            "rescheduleReasonComment": resp["rescheduleReasonComment"] if "rescheduleReasonComment" in resp else "",
                                            "loanId": str(loanID)})
                        elif (hh["X-FINERACT-ENTITY"] == "RESCHEDULELOAN" if "X-FINERACT-ENTITY" in hh else False) and (hh["X-FINERACT-ACTION"] == "REJECT"):
                            trid = Query.from_(whlog).select("RESOURCE_ID_OUT").where(
                                whlog.RESOURCE_ID_IN == str(input_dict["resourceId"]))
                            trid = db.runQuery(trid)["data"]
                            posturl = baseurl + "rescheduleloans/" + \
                                (str(trid[0]["RESOURCE_ID_OUT"])
                                 if trid else "")
                            cmd = "reject"
                            payload.update(
                                {"rejectedOnDate": input_dict["changes"]["rejectedOnDate"]})
                        elif (hh["X-FINERACT-ENTITY"] == "RESCHEDULELOAN" if "X-FINERACT-ENTITY" in hh else False) and (hh["X-FINERACT-ACTION"] == "APPROVE"):
                            trid = Query.from_(whlog).select("RESOURCE_ID_OUT").where(
                                whlog.RESOURCE_ID_IN == str(input_dict["resourceId"]))
                            trid = db.runQuery(trid)["data"]
                            posturl = baseurl + "rescheduleloans/" + \
                                (str(trid[0]["RESOURCE_ID_OUT"])
                                 if trid else "")
                            cmd = "approve"
                            payload.update(
                                {"approvedOnDate": input_dict["changes"]["approvedOnDate"]})
                        elif (hh["X-FINERACT-ACTION"] == "REPAYMENT") and (extID):
                            posturl += "/transactions"
                            cmd = "repayment"
                            payload.update(input_dict["changes"])
                            # {"transactionDate": input_dict["changes"]["transactionDate"], "checkNumber": input_dict["changes"]["checkNumber"],
                            # "routingCode": input_dict["changes"]["routingCode"], "receiptNumber": input_dict["changes"]["receiptNumber"],
                            # "paymentTypeId": input_dict["changes"]["paymentTypeId"], "transactionAmount": input_dict["changes"]["transactionAmount"]})
                        elif (hh["X-FINERACT-ACTION"] == "APPROVE") and (extID):
                            cmd = "approve"
                            payload.update({"approvedOnDate": input_dict["changes"]["approvedOnDate"],
                                            "expectedDisbursementDate": datetime.strptime("-".join(str(x) for x in
                                                                                                   resp["timeline"]["expectedDisbursementDate"]),
                                                                                          "%Y-%m-%d").strftime("%d %b %Y"),
                                            "note": ""})
                        elif (hh["X-FINERACT-ACTION"] == "DISBURSE") and (extID):
                            cmd = "disburse"
                            payload.update({"actualDisbursementDate": datetime.strptime("-".join(str(x) for x in
                                                                                                 resp["timeline"]["actualDisbursementDate"]),
                                                                                        "%Y-%m-%d").strftime("%d %b %Y")})
                        elif (hh["X-FINERACT-ACTION"] == "ADJUST") and (extID) and ("resourceId" in input_dict):
                            cmd = "undo"
                            trid = Query.from_(whlog).select("RESOURCE_ID_OUT").where(
                                whlog.RESOURCE_ID_IN == str(input_dict["resourceId"]))
                            trid = db.runQuery(trid)["data"]
                            posturl += "/transactions/" + \
                                (str(trid[0]["RESOURCE_ID_OUT"])
                                 if trid else '')
                            payload.update(input_dict["changes"])
                        elif (hh["X-FINERACT-ACTION"] == "REJECT") and (extID):
                            cmd = "reject"
                            payload.update(
                                {"rejectedOnDate": input_dict["changes"]["rejectedOnDate"]})
                        elif (hh["X-FINERACT-ACTION"] == "APPROVALUNDO") and (extID):
                            cmd = "undoapproval"
                            payload = {"note": ""}
                        elif (hh["X-FINERACT-ACTION"] == "DISBURSALUNDO") and (extID):
                            cmd = "undodisbursal"
                            payload = {"note": ""}
                        # print payload, cmd
                        r = requests.post(posturl + "?command=" + cmd, data=json.dumps(
                            payload), headers=headers, auth=auth, verify=False) if cmd != '' else ''
                        outDict = r.json()
                        db.Update(db="mint_loan", table="mw_webhook_log", checkAll=False, conditions={"AUTO_ID=": str(auto_id)},
                                  LOAN_ID_OUT=str(
                                      outDict["loanId"]) if "loanId" in outDict else None,
                                  LENDER_OUT=headers["Fineract-Platform-TenantId"].upper(
                        ) if headers["Fineract-Platform-TenantId"] is not None else None,
                            RESOURCE_ID_OUT=str(outDict["resourceId"]) if "resourceId" in outDict else None)
                        # print r, posturl + "?command=" + cmd
                        # print r.json()
                db._DbClose_()
        except Exception as ex:
            # falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            raise
