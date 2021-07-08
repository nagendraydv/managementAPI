from __future__ import absolute_import
from __future__ import print_function
import falcon
import json
import boto3
import botocore
import mimetypes
from mintloan_utils import DB
from pypika import Query, Table, functions


class S3ReadResource:

    def on_post(self, req, resp):
        """Handles GET requests"""

        try:
            raw_json = req.stream.read()
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400, 'Error', ex.message)

    def on_get(self, req, resp):
        """Handles POST requests"""
        input_dict = req.params
        #print(input_dict)
        try:
            assert ("customerID" in list(input_dict.keys())) & ("docSeqID" in list(input_dict.keys()))
        except:
            # falcon.HTTPError(falcon.HTTP_400,'Request improper', 'Could not identify customer and/or customer document from the request.')
            raise
        if (str(input_dict["customerID"])!='0') & ("newUat" not in input_dict):
            session = boto3.Session(aws_secret_access_key="ok6goKsJwAolz9cjjxBYPPhZ2VHzeQHoUeQYJXem",aws_access_key_id="AKIAJHI6CRQOELVBDCHQ")
            bucket = "uat.secureasset.mintwalk.com"  # "mintwalk"
            folder = 'mintloan_live_docs/'
        else:
            session = boto3.Session(aws_secret_access_key="0AHYYE4Gu3U+ek5TvQl2vyfk3WlOm5kYIIMgy8eG",aws_access_key_id="AKIAJQIMFTEKKOFALAWQ")  # profile_name='mw')
            bucket = "datafeed.mintwalk.com"
        s3 = session.resource('s3')
        try:
            junk = s3.meta.client.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            raise falcon.HTTPError(
                falcon.HTTP_400, 'Connection error', 'Could not establish S3 connection')
        try:
            db = DB(dictcursor=True)
            if str(input_dict["customerID"]) != '0' and input_dict["docSeqID"]:
                docs = Table("mw_cust_kyc_documents", schema="mint_loan")
                docurl = db.runQuery(Query.from_(docs).select("DOCUMENT_URL").where((docs.DOC_SEQ_ID == input_dict["docSeqID"]) &
                                                                                    (docs.CUSTOMER_ID == input_dict["customerID"])))
                if (not docurl["error"]) & (len(docurl["data"]) > 0):
                    docurl = "/".join(docurl["data"][0]["DOCUMENT_URL"].split("/")[-2:])
                    # print docurl
                    resp.content_type = mimetypes.guess_type(docurl.replace("Mintwalk", ".jpg").replace("Aadhar", ".jpg"))[0]
                    #resp.content_type = mimetypes.guess_type(docurl.replace("Aadhar",".jpg"))[0]
                    if ("uat.secureasset.mintwalk.com" not in docurl):
                        resp.stream = s3.Object(bucket, folder + docurl).get()['Body']
                        resp.stream_len = s3.Object(bucket, folder + docurl).content_length
                    else:
                        resp.stream = s3.Object(bucket, docurl.split("/")[-1]).get()['Body']
                        resp.stream_len = s3.Object(bucket, docurl.split("/")[-1]).content_length
                else:
                    # falcon.HTTPError(falcon.HTTP_400,'Document not found', 'Could not find requested document in s3 bucket')
                    raise
            else:
                docs = Table("mw_other_documents", schema="mint_loan")
                q = Query.from_(docs).select(functions.Max(docs.DOC_SEQ_ID))
                #print(q)
                data = db.runQuery(Query.from_(docs).select("DOCUMENT_URL", "DOCUMENT_FOLDER").where(docs.DOC_SEQ_ID == q))
                if data["data"]:
                    datafolder = data["data"][0]["DOCUMENT_FOLDER"]
                    docurl = data["data"][0]["DOCUMENT_URL"]
                    try:
                        resp.content_type = mimetypes.guess_type(docurl.replace("Mintwalk", ".jpg"))[0]
                        resp.stream = s3.Object(bucket, datafolder + "/" + docurl).get()['Body']
                        resp.stream_len = s3.Object(bucket, datafolder + "/" + docurl).content_length
                    except:
                        raise
        except Exception as ex:
            raise falcon.HTTPError(falcon.HTTP_400,'Invalid JSON', 'The JSON was incorrect.')
            
