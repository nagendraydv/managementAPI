from __future__ import absolute_import
import falcon
import json
import boto3
import botocore
import mimetypes
import string
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
from pypika import Query, Table
from six.moves import range


class S3UploadResource:

    @staticmethod
    def setFilename():
        chars = string.ascii_uppercase + string.digits # python3 remove uppercase so we have to use ascii_uppercase
        return ''.join([choice(chars) for _ in range(7)]) + (datetime.utcnow() + timedelta(seconds=19800)).strftime("%s") + "."

    def on_post(self, req, resp, **kwargs):
        """Handles GET requests"""
        output_dict = {"msgHeader": {"authToken": ""}, "data": {}}
        errors = utils.errors
        success = "success"
        # print req
        try:

            msgHeader = {"authToken": req.get_param("authToken"), "authLoginID": req.get_param("authLoginID"),
                         "timestamp": req.get_param("timestamp"), "ipAddress": req.get_param("ipAddress")}
            data = {"customerID": req.get_param("customerID"), "docTypeID": req.get_param("docTypeID")}
            userimage = req.get_param("userfile")
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Request does not contain required fields.')
            raise
        s3url = "https://s3-ap-southeast-1.amazonaws.com/"
        bucket = "uat.secureasset.mintwalk.com"
        folder = 'mintloan_live_docs/smartdash_uploaded/'
        try:
            db = DB(msgHeader["authLoginID"], dictcursor=True)
            docs = Table("mw_kyc_document_type", schema="mint_loan")
            validDocs = db.runQuery(Query.from_(
                docs).select("DOCUMENT_TYPE_ID"))
            validDocs = [str(list(x.values())[0])
                         for x in validDocs["data"]] if not validDocs["error"] else []
            if (not validate.Request(api='mlUpload', request={"msgHeader": msgHeader, "data": data})) or (str(data["docTypeID"]) not in validDocs):
                output_dict["data"].update(
                    {"error": 1, "message": errors["json"]})
            else:
                try:
                    filename = self.setFilename(
                    ) + userimage.filename.split('.')[-1]
                    s3path = s3url + bucket + '/' + folder + filename
                    session = boto3.Session(aws_secret_access_key="ok6goKsJwAolz9cjjxBYPPhZ2VHzeQHoUeQYJXem",
                                            aws_access_key_id="AKIAJHI6CRQOELVBDCHQ")
                    s3 = session.resource('s3')
                    junk = s3.meta.client.head_bucket(Bucket=bucket)
                except botocore.exceptions.ClientError as e:
                    raise falcon.HTTPError(
                        falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
                val_error = validate(db).basicChecks(
                    token=msgHeader["authToken"])

                if val_error:
                    output_dict["data"].update(
                        {"error": 1, "message": val_error})
                    resp.body = json.dumps(output_dict)
                else:
                    inserted = db.Insert(db="mint_loan", table='mw_cust_kyc_documents', compulsory=False, date=False, DOCUMENT_URL=s3path,
                                         CUSTOMER_ID=data["customerID"],  DOCUMENT_TYPE_ID=data["docTypeID"],
                                         UPLOAD_MODE="SmartDash Admin", DOCUMENT_STATUS="N", CREATED_BY=msgHeader["authLoginID"],
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
                    if inserted:
                        junk = s3.Object(
                            bucket, folder + filename).put(Body=userimage.file.read())
                        token = generate(db).AuthToken()
                        if token["updated"]:
                            output_dict["data"].update(
                                {"error": 0, "message": success})
                            output_dict["msgHeader"]["authToken"] = token["token"]
                        else:
                            output_dict["data"].update(
                                {"error": 1, "message": errors["token"]})
                    else:
                        output_dict["data"].update(
                            {"error": 1, "message": errors["query"]})
                    db._DbClose_()
            resp.body = json.dumps(output_dict)
        except Exception as ex:
            raise  # falcon.HTTPError(falcon.HTTP_400,'Error', ex.message)
