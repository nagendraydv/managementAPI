from __future__ import absolute_import
from __future__ import print_function
import base64, bcrypt, MySQLdb, math, re , string, requests, json, logging, pypika
from Crypto.Cipher import Blowfish
from datetime import datetime, timedelta
import numpy as np
from random import choice, randint
from logging.handlers import TimedRotatingFileHandler  
from requests.auth import HTTPBasicAuth
import warnings
import six
from six.moves import range
from codecs import encode,decode

warnings.filterwarnings("ignore", category = MySQLdb.Warning)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# connection closes are not handeled here. Closing of connection of all instances is expected to be handled at api level.

class utils(object):

    formatter = logging.Formatter('%(asctime)s|%(levelname)s|13.126.16.154|%(api)s:%(lineno)d|%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh = TimedRotatingFileHandler('/home/nagendra/centos/mintwalk/mintloanAdmin/logs/python_apis.log', when="midnight", interval=1, backupCount=1000)#logging.FileHandler('apis.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger2 = logging.getLogger('python_api_logs')
    logger2.setLevel(logging.DEBUG)
    logger2.addHandler(fh)
    logger = logger2
    errors = {"no":"User created successfully", 
              "timeout":"sessionTimeout", 
              "userExists":"User already exist", 
              "credentials":"Invalid credentials", 
              "rights":"User rights mismatch", 
              "login":"Invalid login", 
              "json":"Request improper", 
              "query":"Internal error", 
              "token":"Token generation error", 
              "strength":"Password strength low", 
              "session":"UserLoggedIn", 
              "user": "User does not exist", 
              "password": "Incorrect password",
              "document": "No verified document found for the customer"} 

    headers = {'Content-type': 'application/json'}
    finflux_uat_headers = {'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'chaitanya'}#, 
    #                       'Authorization': 'Basic bWludHdhbGs6cGFzc3dvcmQ='}
    #finflux_headers = {'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'chaitanya'}
    finflux_headers = {"CHAITANYA":{'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'chaitanya'},
                       "GETCLARITY":{'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'getclarity'},
                       "PURSHOTTAM":{'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'purshottam'},
                       "POONAWALLA":{'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'poonawalla'},
                       "MINTWALK":{'Content-type': 'application/json', 'Fineract-Platform-TenantId': 'mintwalk'}, 
                       "Content-type": 'application/json', 'Fineract-Platform-TenantId': 'chaitanya'}#mintwalk_uat 
    #finflux_auth = HTTPBasicAuth('mintwalk', 'password')
    finflux_auth = HTTPBasicAuth('SUPERMONEY', 'Super@1234')#'Nikhil', 'Nikhil@2')
    mifos_auth = HTTPBasicAuth('backend', 'mifos@2')
    deviceFPmsgHeader = {"imeiNo":"864587023661318","osName":"Android","osVersion":22,"dualSim":True,"deviceModelNo":"A0001",
                         "deviceManufacturer":"OnePlus","timezone":"Asia/Calcutta","nwProvider":"Vodafone IN","connectionMode":"WIFI",
                         "latitude":"","longitude":"","country":"United States"}
    deviceFootPrintData = {"deviceName":"ASUS Zenfone","dualSim":True,"imeiLine1":"864587023661318","nwProviderLine1":"Vodafone IN",
                           "phoneNoLine1":"","nwTypeLine1":"GSM","imeiLine2":"864587023661318","phoneNoLine2":"","nwProviderLine2":"",
                           "nwTypeLine2":"", "activeSim":"8991212315759828419","osName":"Android", 
                           "osVersion":"android : 5.1.1 | LOLLIPOP_MR1 | SDK=22", "deviceModelNo":"A0001","deviceManufacturer":"ASUS",
                           "orientation":"Portrait","batteryLevel":"83.0","batteryState":"3", "language":"English","country":"IN",
                           "multiTaskingSupport":True,"proximityMonitoringEnabled":True,"proximityState":"", "timezone":"Asia\/Calcutta",
                           "latitude":"","longitude":"","ipAddress":"192.168.2.17","connectionMode":"WIFI", "jailBrokenRooted":False,
                           "emailId":"harilal@gmail.com","wifiMac":"c0:ee:fb:30:3f:04","wifiStationName":"\"GetClarity1\"",
                           "wifiBBSID":"08:86:3b:be:29:88","wifiSignalStrength":"4","mcc":"404","mnc":"20","mobileNetworkCode":"20",
                           "cellTowerId":131840172,"locationAreaCode":33315,"gsmSignalStrength":256,"appVersionId":"100"}

    def __init__(self, ip="172.17.1.200"):#172.17.1.4"):#172.17.1.254"):#"ws.mintwalk.com"):#172.17.2.212
        if ip=="172.17.1.200" or ip=="52.220.32.234":#172.17.1.254":#"ws.mintwalk.com": #172.17.2.212
            self.login = "admin@mintwalk.com"
            self.__password = "Tempuser@2017"
            self.ip = ip
        else:
            self.login = ""
            self.__password = ""
            self.ip = ip

    @classmethod
    def camelCase(self, inputVar=None, modifyNullValues=True):
        if isinstance(inputVar, str):
            return self.camelCaseString(inputVar)
        elif isinstance(inputVar, list):
            return self.camelCaseList(inputVar, modifyNullValues)
        elif isinstance(inputVar, dict):
            return self.camelCaseDict(inputVar, modifyNullValues)            
    
    @classmethod
    def camelCaseString(self, inString=""):
        camelString = ''.join(x for x in inString.title() if x.isalnum())
        result = camelString[0].lower() if camelString not in ["BrokerageAmount"] else camelString[0].upper()
        result += (camelString[1:-1] + camelString[-1].upper()) if ((camelString[-2].isupper()) and 
                                                                    (camelString[-2:].lower() in ["id","ip","no"])) else camelString[1:]
        return result

    @classmethod
    def camelCaseList(self, inList=[], modifyNullValues=True):
        result = []
        for ele in inList:
            if isinstance(ele, dict):
                result.append(self.camelCaseDict(ele, modifyNullValues=modifyNullValues))
            elif isinstance(ele, str):
                result.append(self.camelCaseString(ele))
        return result

    @classmethod
    def camelCaseDict(self, inDict={}, modifyNullValues=True):
        result = {}
        for key, value in inDict.items():
            camelKey = self.camelCaseString(key) if type(key)==str else key
            if isinstance(value,list):
                value = self.camelCaseList(value, modifyNullValues=modifyNullValues)
            if value==None and modifyNullValues:
                value = 0 if "date" in key.lower() else ""
            result.update({camelKey:value})
        return result
    
    @staticmethod
    def mergeDicts(*dict_args):
        '''
        Given any number of dicts, shallow copy and merge into a new dict,
        precedence goes to key value pairs in latter dicts.
        '''
        result = {}
        for dictionary in dict_args:
            result.update(dictionary)
        return result

    def create_customer(self, loginId='sandesh.kulkarni@mintwalk.com', userId=None, pin="1234", language=None):
        if userId:
            payload = {"msgHeader":{"loginId":loginId, "consumerId":"412", "authToken":"", "channelType":"M",
                                    "deviceId":"smartDashAdmin", "source":"smartDashAdmin"},
                       "deviceFPmsgHeader":{"clientIPAddress":"192.168.1.2","connectionMode":"WIFI","country":"","deviceManufacturer":"motorola",
                                            "deviceModelNo":"Moto G (5S) Plus", "dualSim":True,"imeiNo":"351893084234874","latitude":"",
                                            "longitude":"","nwProvider":"Vodafone IN","osName":"Android","osVersion":25,"timezone":"Asia/Kolkata",
                                            "versionCode":"27","versionName":"4.1.1"},
                       "data":{"userId":userId, "pin":pin, "language":""}}
            r = requests.post("http://" + "13.126.98.200" + ":8080/mintLoan/mintloan/createCustomerWPinWOOTPWOSMS", data=json.dumps(payload),
                              headers=self.headers)
            #print payload, r.json()
            return r.json()

    def update_basic_profile(self, loginId=None, authToken=None, companyName=None, companyID=None, division=None, subDevision=None, age=None, currentCity=None, address=None, companyNo=None, dob=None, experience=None, gender=None, monthlyIncome=None, education=None, lifeStage=None, name=None, email=None, placeOfBirth=None, userCategory=None, alternateMobNo=None):
        if name and companyName and currentCity and loginId and authToken:
            payload = {"msgHeader":{"loginId":loginId, "consumerId":"412", "authToken":authToken, "channelType":"M",
                                    "deviceId":"smartDashAdmin", "source":"smartDashAdmin"},
                       "deviceFPmsgHeader":{"clientIPAddress":"192.168.1.2","connectionMode":"WIFI","country":"","deviceManufacturer":"motorola",
                                            "deviceModelNo":"Moto G (5S) Plus", "dualSim":True,"imeiNo":"351893084234874","latitude":"",
                                            "longitude":"","nwProvider":"Vodafone IN","osName":"Android","osVersion":25,"timezone":"Asia/Kolkata",
                                            "versionCode":"27","versionName":"4.1.1"},
                       "data":{"customerData":{"finalUpdate":True, "companyName":companyName, "companyId":companyID, "currentCity":currentCity, "address":address,
                               "companyNo":companyNo, "alternateMobNo":alternateMobNo, "userCategory":userCategory, "dob":dob, "age":age,
                               "experience":experience, "gender":gender, "monthlyIncome":monthlyIncome, "education":education, "lifeStage":lifeStage,
                               "name":name, "email":email, "placeOfBirth":placeOfBirth, "division":division, "subDevision":subDevision}}}
            payload = {k:{k1:(v1 if type(v1)!=dict else {k2:v2 for k2,v2 in six.iteritems(v1) if v2}) for k1,v1 in six.iteritems(v) if v1} for k,v in six.iteritems(payload) if v}
            r = requests.post("http://" + "13.126.98.200" + ":8080/mintLoan/mintloan/updateBasicProfilev2", data=json.dumps(payload),
                              headers=self.headers)
            #print payload, r.json()
            return r.json()


    def add_bank(self, loginId=None, authToken=None, accNo=None, accType=None, defaultAccountFlag=True, ifscCode=None, micrCode=None, personalAccountFlag=None):
        if ifscCode and accNo and loginId and authToken:
            payload = {"msgHeader":{"loginId":loginId, "consumerId":"412", "authToken":authToken, "channelType":"M",
                                    "deviceId":"smartDashAdmin", "source":"smartDashAdmin"},
                       "deviceFPmsgHeader":{"clientIPAddress":"192.168.1.2","connectionMode":"WIFI","country":"","deviceManufacturer":"motorola",
                                            "deviceModelNo":"Moto G (5S) Plus", "dualSim":True,"imeiNo":"351893084234874","latitude":"",
                                            "longitude":"","nwProvider":"Vodafone IN","osName":"Android","osVersion":25,"timezone":"Asia/Kolkata",
                                            "versionCode":"27","versionName":"4.1.1"},
                       "data":{"accNo":accNo, "accType":accType, "defaultAccountFlag":defaultAccountFlag, "ifscCode":ifscCode, "micrCode":micrCode, "personalAccountFlag":personalAccountFlag}}
            payload = {k:{k1:v1 for k1,v1 in six.iteritems(v) if v1} for k,v in six.iteritems(payload) if v}
            r = requests.post("http://" + "13.126.98.200" + ":8080/mintLoan/mintloan/addBank", data=json.dumps(payload),
                              headers=self.headers)
            #print payload, r.json()
            return r.json()


    def store_customer_data(self, dataKey=None, dataValue=None, adminId=None, loginId=None):
        if dataKey and dataValue and adminId and loginId:
            payload = {"msgHeader":{"loginId":loginId, "consumerId":"412", "authToken":"", "channelType":"M",
                                    "deviceId":"smartDashAdmin", "source":"smartDashAdmin"}, 
                       "deviceFPmsgHeader":{"clientIPAddress":"192.168.1.2","connectionMode":"WIFI","country":"","deviceManufacturer":"motorola",
                                            "deviceModelNo":"Moto G (5S) Plus", "dualSim":True,"imeiNo":"351893084234874","latitude":"",
                                            "longitude":"","nwProvider":"Vodafone IN","osName":"Android","osVersion":25,"timezone":"Asia/Kolkata",
                                            "versionCode":"27","versionName":"4.1.1"},
                       "data":{"customerData":[{"dataKey": dataKey, "dataValue": dataValue}], "adminId":adminId}}
            r = requests.post("http://" + "13.126.98.200" + ":8080/mintLoan/mintloan/storeCustomerData", data=json.dumps(payload),
                              headers=self.headers)
            #print(r)
            #print(r.json())
            return r.json() if r.json() else {"data":""}

    def send_sms(self, sender="SUPERMONEY", uniCode=0, numbers=[], message=None):
        if message and numbers:
            payload = {"sender":sender,"numbers":numbers,"message":message, "uniCode":uniCode}
            r = requests.post("http://" + "13.126.28.53" + ":8080/SMS_Service/send/SMS", data=json.dumps(payload), headers=self.headers) #13.126.98.200
            return r.text

class DB(object):

    def __init__(self, id='', cursor=None, mydb='', dictcursor=None, CommitRequired=False, path=".", filename="mysql.config", raiseError=True, debug=False):
        '''New database connection is established if cursor is not provided during the initialisation of class instance.
        '''
        #print path+filename
        if (cursor is None) or (CommitRequired & (mydb=='')):
            self.mydb, self.cursor, self.dictcursor = self.connect(path=path, filename=filename, dictcursor=True, debug=debug)
        else:
            self.mydb, self.cursor, self.dictcursor = mydb, cursor, dictcursor
        self.id = id
        self.db = "mint_loan_admin"
        self._conditionsString = ""
        self._raise = raiseError

    @property
    def get(self):
        conditionString = self._conditionsString
        self._conditionsString = ""
        return conditionString

    def _DbClose_(self):
        '''Closes the database connection if cursor is not externally provided
        '''
        if self.mydb is not '':
            self.mydb.close()

    def _dbQuery_(self, table="mw_admin_user_master", Field = None, id = None):
        '''Queries the database to select a particular field corresponding to the login id passed to the instance of Validate class.
        It takes the field name as input. By defualt queries count(*) which returns number of rows corresponding to the given login id.
        '''
        if id==None:
            id=self.id
        if isinstance(Field,str):
            junk = self.cursor.execute("select " + Field + " from " + self.db + "." + table + " where LOGIN='%s'"%(id))
            dbResp = list(self.cursor)[0][0] if len(list(self.cursor))>0 else None
        elif isinstance(Field, list):
            junk = self.cursor.execute("select " + ", ".join(Field) + " from " + self.db + "." + table + " where LOGIN='%s'"%(id))
            dbResp = list(self.cursor)[0] if len(list(self.cursor))>0 else None
        else:
            junk = self.cursor.execute("select count(*) from " + self.db + "." + table + " where LOGIN='%s'"%(id))
            dbResp = list(self.cursor)[0][0] if len(list(self.cursor))>0 else None
        return dbResp


    @staticmethod
    def pikastr(queryBuilder=None):
        '''
        Changes quote character of a query builder to ` and converts it into string thereafter.
        '''
        if queryBuilder is None:
            return ""
        queryBuilder.quote_char = "`"
        queryBuilder.QUOTE_CHAR = "`"
        return str(queryBuilder)

    def runQuery(self, queryBuilder=None, queryStr=None):
        dbResp = {"data": [], "error":True}
        if isinstance(queryBuilder, pypika.queries.QueryBuilder):
            queryStr = self.pikastr(queryBuilder)
        try:
            if queryStr is not None:
                junk = self.dictcursor.execute(queryStr)
                #self.mydb.commit()
                dbResp = {"data":list(self.dictcursor.fetchall()), "error":False}
            return dbResp
        except:
            return dbResp


    def Query(self, db = "mint_loan", primaryTable = "mw_customer_login_credentials", secondaryTable = None, fields={"A": ["LOGIN_ID"]}, 
              joinType = "outer", conditions = {}, orderBy="", groupBy="", limit = None, eqkey = "CUSTOMER_ID", count=True, Data=True, 
              replace=True, debug=False):
        '''Queries the database to select particular fields using join queries. 
        It takes the field name as input. By defualt queries count(*) which returns number of rows corresponding to the given login id.
        '''
        dbResp = {"data": [], "count":0, "error":True}
        if self.dictcursor==None:
            return dbResp
        if orderBy!="":
            orderBy = " order by " + orderBy
        if groupBy!="":
            groupBy = " group by " + groupBy
        alphabets = list(string.ascii_uppercase)
        secondaryTable = [secondaryTable] if (isinstance(secondaryTable,str)) & (eqkey is not None) else secondaryTable
        eqkey = [eqkey for _ in secondaryTable] if (isinstance(eqkey,str) & (secondaryTable is not None)) else eqkey
        if isinstance(fields,dict):
            if "A" not in list(fields.keys()):
                fields.update({"A":[]})
        limitString = (" limit " + (str(limit) if not isinstance(limit,list) else ", ".join(str(i) for i in limit) )) if limit else ""
        conditionString = ( (" WHERE " + "' and ".join(["'".join(i) for i in conditions.items()]) 
                             + "' ") if conditions!={} else "") if conditions !={} else ""
        conditionString = conditionString.replace("'NULL'", "NULL")
        #if type(conditionString)==str:
        #    conditionString = " WHERE " + conditionString
        #else:
        #    conditionString = ( (" WHERE " + "' and ".join(["'".join(i) for i in conditions.items()]) 
        #                         + "' ") if conditions!={} else "") if conditions !={} else ""
        if replace:
            conditionString = conditionString.replace("''", "")
            conditionString = conditionString.replace("'[", "(")
            conditionString = conditionString.replace("]'", ")")
            conditionString = conditionString.replace(")'", ")")
            conditionString = conditionString.replace("'(", "(")
        exString = "SELECT " + (("A." + ", A.".join(fields["A"]) + (", " if len(fields)>1 else "")) if len(fields["A"])>0 else "" )
        temp = " FROM %s.%s AS A "%(db, primaryTable)
        if isinstance(secondaryTable, list):
            for i in range(len(secondaryTable)):
                exString += " " + alphabets[i+1] + "." + (", " + alphabets[i+1] + ".").join(fields[alphabets[i+1]]) + ","
                temp += "left " + joinType + " join %s.%s as "%(db, secondaryTable[i]) + alphabets[i+1]
                if (isinstance(eqkey[i],tuple)):
                    temp += (" on %s = %s " %(eqkey[i][0], eqkey[i][1]))
                else:
                    temp += (" on A.%s = "%(eqkey[i]) if eqkey[i][1]!="." else " on %s = "%(eqkey[i])) + alphabets[i+1] 
                    temp += ".%s "%(eqkey[i] if eqkey[i][1]!="." else eqkey[i][2:])
            exString = exString[:-1] 
        exString += temp + conditionString + groupBy + orderBy + limitString
        exString2 = "SELECT count(*) " + temp +  conditionString
        if debug:
            print(exString)#, exString2
        try:
            if Data:
                junk = self.dictcursor.execute(exString)
                dbResp.update({"data":list(self.dictcursor.fetchall()), "error":False})
            if count:
                junk = self.cursor.execute(exString2)
                dbResp.update({"count":self.cursor.fetchall()[0][0], "error":False})
            return dbResp 
        except:
            return dbResp 

    
    @classmethod
    def connect(self, filename="mysql.config", path=".", dictcursor=False, changeDefaults=True, debug=False): 

        '''This connects the mysql database inside python. It obtains host, user, password and database name from .mysql.config file to 
        establish the connection. A flag dictcursor can be set to True to return dictcursor object alongside the cursor object.
        Some default formats for datatype conversions are changed (e.g. datetime objects are converted into string and decimal to float) 
        while the connection is established. This can be switched off by setting changeDefaults to False.'''
    
        data=[]
        with open(path + filename,'r') as f:
            for line in f:
                data.append(line.strip().encode().decode("unicode-escape")[1:-1])
        h, u, p, d = decode(data[6].encode()[1:-1],'base-64').decode(), decode(data[11].encode()[1:-1], 'uu').decode(), decode(decode(data[13].encode()[1:-1], 'base-64'), 'uu').decode(), decode(decode(data[19].encode()[1:-1], 'uu'), 'base-64').decode()
        if debug:
            print(h, u, p, d)
        try:
            mydb = MySQLdb.connect(h,u,p,d)#, charset='utf8')
        except:
            try:
                mydb = MySQLdb.connect(h,u,p)#, charset='utf8')
            except:
                raise Exception("Invalid input parameters")
        cursor = mydb.cursor()
        cursor.execute("select @@group_concat_max_len")
        if cursor.fetchall()[0][0]<20000:
            cursor.execute("set group_concat_max_len=20000") #earlier it was set global 
            mydb.commit()
            try:
                mydb = MySQLdb.connect(h,u,p,d)
                cursor = mydb.cursor()
            except:
                try:
                    mydb = MySQLdb.connect(h,u,p)
                    cursor = mydb.cursor()
                except:
                    raise Exception("Invalid input parameters")
        if changeDefaults:
            self.MySQLdefaults = mydb.converter # copying the default converter dictionary 
            mydb.converter[10] = str # converts datetime object into string
            mydb.converter[12] = str # converts datetime object into string
            mydb.converter[246] = float #converts decimal into float
        if not dictcursor:
            return mydb, cursor
        else:
            dictcursor=mydb.cursor(MySQLdb.cursors.DictCursor)
            return mydb,cursor, dictcursor


    def Insert(self, db="mint_loan_admin", table='mw_admin_user_master', compulsory=True, date=True, debug=False, noIgnor=True, **indict):
        '''Inserts values into the table specified by an argument named 'table' (default "mw_admin_user_master") in a database specified by 
        an argument named 'db' (default "mint_walk_admin"). Input data should be in the form of Field_name1=value1, Field_name2=value2, ...
        '''
        indict = {key:value for key, value in indict.items() if type(value)==str or type(value)==six.text_type}
        if date:
            indict.update({"CREATED_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s")})
        Valid = validate(self).compulsory(table, list(indict.keys())) if compulsory else True
        fields = '(`' + '`, `'.join(list(indict.keys())) + '`)'
        values = "'" + "' ,'".join(list(indict.values())) + "'"
        values = values.replace("'LAST_INSERT_ID()'", "LAST_INSERT_ID()")
        if debug:
            print(indict)#, fields, values
        if ('LOGIN' in list(indict.keys())):
            finalCheck = (not validate(self,id=indict['LOGIN']).User()) & Valid
        else:
            finalCheck = Valid
        if finalCheck:
            #if table=="mw_reverse_feed_cams" or table=="mw_reverse_feed_consolidated":
            if debug:
                print(("insert" if noIgnor else "insert ignore") + " into " + db + '.' + table + " %s values (%s)" %(fields, values))
            junk = self.cursor.execute(("insert" if noIgnor else "insert ignore") + " into " + db + '.' + table + " %s values (%s)" %(fields, values))
            self.mydb.commit()
            return True
        else:
            return False

    def InsertMany(self, db="mw_company_3", table='mw_derived_income_data', commit=True, keys=None, values=None, debug=False, Ignore=False):
        '''Inserts values into the table specified by an argument named 'table' (default "mw_admin_user_master") in a database specified by 
        an argument named 'db' (default "mint_walk_admin"). Input data should be in the form of Field_name1=value1, Field_name2=value2, ...
        '''

        fields = ' (`' + '`, `'.join(keys) + '`)'
        if debug:
            print(("insert ignore" if Ignore else "insert") + " into " + db + '.' + table + fields + " values (" + " ,".join(['%s' for _ in keys]) + ")")
        junk = self.dictcursor.executemany(("insert ignore" if Ignore else "insert") + " into " + db + '.' + table + fields + " values (" + " ,".join(['%s' for _ in keys]) + ")",
                                           values)
        if commit:
            self.mydb.commit()
        return True

        
    def Update(self, db="mint_loan_admin", table='mw_admin_user_master', conditions={}, checkAll=False, replace=True, debug=False, **indict):
        '''Updates values of the given columns in the table specified by an argument named 'table' (default "mw_admin_user_master") 
        in a database specified by an argument named 'db' (default "mint_walk_admin").
        If checkAll flag (defualt False) is set True then the module checks if all column names are provided as input parameter or not.
        This module is currently used primarily to update AUTH_TOKEN, PASSWORD and all other admin module user details.
        '''
        indict = {key:value for key, value in indict.items() if type(value)==str or type(value)==six.text_type}
        if ("PASSWORD" in list(indict.keys())) & (table=="mw_admin_user_master"):
            indict.update({"MODIFIED_PASSWORD_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s")})
        if ("MODIFIED_BY" in list(indict.keys())) & ("MODIFIED_DATE" not in list(indict.keys())) & (db=="mint_walk_admin"):
            indict.update({"MODIFIED_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s")})
        if ("FAIL_ATTEMPT" in list(indict.keys())) & (table=="mw_admin_user_master"):
            indict.update({"LAST_LOGIN_DATE":(datetime.utcnow() + timedelta(seconds=19800)).strftime("%s")})
        Valid=validate(self).DBfields(table=table, keys=list(indict.keys()), checkAll=checkAll)
        conditionString = ( (" WHERE " + "' and ".join(["'".join(i) for i in conditions.items()]) + "' ") if conditions!={} else "") 
        items = list(indict.items())
        if replace:
            conditionString = conditionString.replace("''", "")
            conditionString = conditionString.replace("'[", "(")
            conditionString = conditionString.replace("]'", ")")
            conditionString = conditionString.replace(")'", ")")
            conditionString = conditionString.replace("'(", "(")
        if debug:
            print(Valid, indict)
        if Valid:
            exString = "update " + db + "." + table + " set "
            exString += ", ".join( "`%s`='%s'"%items[i] for i in range(len(items)) ) 
            exString += conditionString #+ " where LOGIN='%s'"%self.id if table=="mw_admin_user_master" else ""
            if debug:
                print(exString)
            junk = self.cursor.execute(exString)
            self.mydb.commit()
            return True
        else:
            return False

    def IncrementFailAttempt(self, db="mint_loan_admin", table='mw_admin_user_master', field="FAIL_ATTEMPT"):
        '''Increments the number in FAIL_ATTEMPT column in mw_admin_user_master table. Returns True and value of FAIL_ATTEMPT before 
        incrementing it. Returns False and None if the field FAIL_ATTEMPT does not exist in the given table (default 
        'mw_admin_user_master') fails. Returns True and 0 if increamenting fails.  
        '''
        Valid=validate(self).DBfields(table=table, keys=[field], checkAll=False)
        if Valid:
            exString = "update " + db + "." + table + " set %s= (@cur_value := %s) + 1 where LOGIN='%s'"%(field, field, self.id)
            junk = self.cursor.execute(exString)
            self.mydb.commit()
            junk = self.cursor.execute("select @cur_value")
            failedAttempts = self.cursor.fetchall()[0][0]
            return True, failedAttempts if failedAttempts else 0
        else:
            return False, None






class generate(DB):
    '''This class is used to generate routinely required values such as AUTH_TOKEN, PASSWORD, etc. All such generated values are subsequently 
    updated in respective database field unless specified otherwise. Hence the generate class inherits all modules of the DB class 
    and is initialised by an instance of the db class. 
    '''
    def __init__(self, dbInstance, id=''):
        #DB.__init__(self, id, cursor, mydb, CommitRequired=True) # earlier it was initialising the db class here. 
        # In the new implementation it requires the initialisation of db class to be done outside and only the instance is to be passed 
        self.mydb = dbInstance.mydb
        self.cursor = dbInstance.cursor
        self.dictcursor = dbInstance.dictcursor
        self.id = dbInstance.id if id=='' else id
        self.db = dbInstance.db
        self.secret = 'secret'

    def DBlog(self, table="mw_python_api_log", logLevel="DEBUG", logIP="13.126.16.154", logFrom=None, lineNo=0, logMessage=None):
        if (logFrom is not None) and (lineNo>0) and (logMessage is not None):
            inserted = self.Insert(db="mint_loan", table=table, compulsory=False, date=False, LOG_LEVEL=logLevel, LOG_IP=logIP,
                                   API_NAME=logFrom, LINE_NUMBER=str(lineNo), LOG=logMessage, CREATED_AT=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return 0
        return 1

    def AuthToken(self, exp=60, update=True, saveLoginAuth=False):
        '''Generates AuthToken and inserts it into database against the given login id if update is True (default). Returns a dictionary with keys 
        "token" and "updated" which contains respectively the values of generated AuthToken and a boolean flag corresponding to whether the 
        token is successfully updated in the database or not. One can additionally provide exp as argument to specify the token expiry time 
        in minutes (default is 60 min).
        '''        
        exp_str = (datetime.utcnow() + timedelta(seconds=exp*60)).strftime("Time:%d-%m-%Y %H:%M:%S")
        #print(exp_str)
        obj = Blowfish.new(self.secret)
        tstamp = obj.encrypt(exp_str)
        #tstamp = tstamp.decode("utf-8","ignore")
        #tstamp = tstamp.decode("utf-16")
        #tstamp = tstamp.split("\r\n")
        #print(tstamp)
        tstamp=base64.b64encode(tstamp)
        #print(tstamp)
        tstamp=tstamp.decode("utf-8")
        #print(tstamp)
        gen=bcrypt.gensalt()[7:]
        #token=base64.b64encode(tstamp)
        gen=gen.decode("utf-8")
        token = str(''.join(gen) + "!" + tstamp)
        #token = str(''.join(bcrypt.gensalt()[7:]) + "!" + tstamp)
        #print(token)
        updated = self.Update(AUTH_TOKEN = token, LOGIN_AUTH_TOKEN=(token if saveLoginAuth else None), conditions={"LOGIN = ":self.id}) if update else False 
        # does not update database when update=False
        return {"token": token, "updated": updated}
        
    def PasswordHash(self, password='', saltRounds=12, exp=30, checkStrength=True, update=True):
        '''Generates a password hash and inserts it into database against given login if update is True (default). The module additionally checks
        if the input password has required strength or not (password must contain at least 8 characters, at least one capital character, 
        a small character, a digit and a special character from the set "!@_$"). saltRounds argument (default 12) is passed to the bcrypt.hashpw
        which specifies the security level for hashed password (larger number will generate a more secure hash but validation of password will 
        also then require a larger cpu time). If update is True (default), the module also generates and subsequently updates in the database 
        the AuthToken. exp argument (default 30 min) specifies the expiry time of the generated AuthToken.

        This module returns a dictionary with keys hash, updated and token containing respectively the hashed password and a boolean flag 
        corresponding to whether the password and token are updated in the database or not and the value of the generated AuthToken when update 
        is True. Otherwise it returns a dictionary with only two keys - hash and updated.   
        '''
        strength = validate.Strength(password) if checkStrength else {'password_ok': True}
        if strength['password_ok'] and update:        
            hashed = bcrypt.hashpw(password,bcrypt.gensalt(saltRounds))
            token = self.AuthToken(exp=exp, update=False)
            strength.update({"hash": hashed, "updated":self.Update(AUTH_TOKEN = token["token"], PASSWORD=hashed, conditions={"LOGIN = ":self.id}), 
                             "token":token["token"]})
        elif strength['password_ok']:
            hashed = bcrypt.hashpw(password,bcrypt.gensalt(saltRounds))
            strength.update({"hash": hashed, "updated" : False})
        else:
            strength.update({"hash":"", "updated" : False})
        return strength
    
    @staticmethod
    def Password():
        '''Generates a random password. This is not yet used in the admin module.
        '''
        chars = string.letters + string.digits + string.digits + "!#@$%&*+-./^_~"
        randpw = ""
        while not validate.Strength(randpw)["password_ok"]:
            randpw = ''.join([choice(chars) for _ in range(randint(10,20))])
        return randpw









class validate(DB):    
    '''This class is used to validate various things before processing the api request. The class routinely requires to access database to 
    validate certain attributes such as AuthToken, Password, etc. Hence the validate class inherits all modules of the DB class and  
    is initialised by an instance of the db class. 
    '''
    def __init__(self, dbInstance, id=''):
        #DB.__init__(self, id, cursor) # earlier it was initialising the db class here. 
        # In the new implementation it requires the initialisation of db class to be done outside and only the instance is to be passed 
        self.mydb = dbInstance.mydb
        self.cursor = dbInstance.cursor
        self.dictcursor = dbInstance.dictcursor
        self.id = dbInstance.id if id=='' else id
        self.db = dbInstance.db
        self.secret = 'secret'

    def getID(self):
        return self.id

    def _getTstampFromToken_(self, token=''):
        '''Extracts timestamp information from the token. If extraction fails then sets the timestamp at 99999 days back.
        '''
        obj = Blowfish.new(self.secret)
        #tstamp=datetime.utcnow()
        #print(token)
        #len1=len(token)
        #mod=len1%4
        #print(mod)
        #val=obj.decrypt(base64.b64decode(token))
        #print(val)
        #val=val.decode("utf-8")
        #print(val)
        #tstamp = datetime.strptime(val,"Time:%d-%m-%Y %H:%M:%S")
        try:
            #val=obj.decrypt(base64.b64decode(token.split("!")[0]))
            #val=val.decode("utf-8")
            #tstamp = datetime.strptime(val,"Time:%d-%m-%Y %H:%M:%S")
            tstamp = datetime.strptime(obj.decrypt(base64.b64decode(token.split("!")[1])),"Time:%d-%m-%Y %H:%M:%S")
            #print(tstamp)
        except:
            tstamp = datetime.utcnow() - timedelta(days=99999)
            #print(tstamp)
        return tstamp

    def AuthToken(self, token='', checkTstamp=True, checkToken=True, loginAuth=True):
        '''Validates AuthToken. Returns a dictionary with keys valid, token_ok and tstamp_ok which are the flags set during the validation 
        process. The module additionally accepts checkTstamp (default True) and checkToken (default True) arguments which can be used to
        bypass a part or full validation process. 
        '''
        checkTstamp = checkTstamp if not loginAuth else False
        d=timedelta(hours=5,minutes=30)
        tstamp_ok = datetime.utcnow()+d > self._getTstampFromToken_(token=token + "=") if checkTstamp else True
        #tstamp_ok = datetime.utcnow() < self._getTstampFromToken_(token=token.replace(" ", "+")) if checkTstamp else True
        #print(tstamp_ok)
        #print(self._getTstampFromToken_(token=token.replace(" ", "+")))
        token_ok = token.replace(" ", "+")==self._dbQuery_(Field = 'AUTH_TOKEN') if checkToken else True 
        token_ok = token_ok if token_ok else token.replace(" ", "+")==self._dbQuery_(Field = 'LOGIN_AUTH_TOKEN') if loginAuth else True
        return {'valid': token_ok & tstamp_ok, 'token_ok' : token_ok, 'tstamp_ok':tstamp_ok}

    def Password(self, password=''):
        '''Validates Password. Returns True if password is validated successfully and False if it fails.
        '''
        dbpassword =  self._dbQuery_(Field = 'PASSWORD')
        password=password.encode("utf-8") #password encode
        #print(dbpassword.encode("utf-8"))
        return bcrypt.checkpw(password,dbpassword.encode("utf-8")) if dbpassword is not None else False # encode the dbpassword because
                                                                                                            #it required to encode the unicode before check password 
    @classmethod
    def Strength(self, password = ''):
        '''Checks the password strength. Provides validation for length (minimum 8 characters), at least one digit, one uppercase 
        character, one lower case character and one special character from the set "[ !#$%&'()*+,-./\^_`{|}~]". Returns a dictionary
        with keys length_error, digit_error, uppercase_error, lowercase_error, symbol_error and password_ok which specifies the 
        respective boolean values.
        '''
        length_error = len(password) < 7
        digit_error = re.search(r"\d", password) is None
        uppercase_error = re.search(r"[A-Z]", password) is None
        lowercase_error = re.search(r"[a-z]", password) is None
        symbol_error = re.search(r"[ !@#$%&'()*+,-./[\\\]^_`{|}~"+r'"]', password) is None
        password_ok = not ( length_error or digit_error or uppercase_error or lowercase_error or symbol_error )
        return {'password_ok' : password_ok, 'length_error' : length_error, 'digit_error' : digit_error, 'uppercase_error' : uppercase_error, 
                'lowercase_error' : lowercase_error, 'symbol_error' : symbol_error}

    def Type(self, type='SUPERUSER'): 
        '''Validates account type. Returns True if the account type specified by the type argument (default "SUPERUSER") is equal to the
        account type stored in the database for the respective user. It returns False otherwise.
        '''
        dbtype =  self.Query(db="mint_walk_admin", primaryTable="mw_admin_user_master", secondaryTable="mw_admin_user_account_type", 
                             fields={"B":["ACCOUNT_TYPE"]}, conditions = {"A.LOGIN = ":self.id}, eqkey="UUID", count=False)
        if not dbtype["error"]:
            return type in [datum["ACCOUNT_TYPE"] for datum in dbtype["data"]]
        else:
            return False
    
    def User(self, id=None): 
        '''Validates existance of the login id, specified by the argument "id" in database. Returns True if login id exists in database 
        and False otherwise.
        '''
        if id==None:
            id=self.id
        return self._dbQuery_(id=id)==1
    
    @classmethod
    def Request(self,**indict):

        ''' Checks if the json request was correct by checking that it contains all the required keys. It does not check the data types of their 
        values though. The test is strict in the sense that the test fails even if the json contains some extra information than required
        for the respective api. Hence this code needs to be updated when a new api is developed or even for any small changes in the json 
        request of existing apis.   
        '''
        try:
            assert ('api' in list(indict.keys())) & ('request' in list(indict.keys()))
            assert ('msgHeader' in list(indict['request'].keys()))
            req = indict['request']
            assert (('authLoginID' in req['msgHeader']) & ('authToken' in req['msgHeader']) & ('timestamp' in req['msgHeader']) & 
                    ('ipAddress' in req['msgHeader']))
        except:
            return False
        data = req['data'] if 'data' in req else {}
        assert_dict = {'fetchOnCustomerID':['customerID'], 'login': ['password', 'loginID'], 'logout': ['loginID'], 'viewUser': ['loginID'],  'resetPassword': ['loginID'],
                       'listUser': [{'page':['startIndex','size']}], 'createUser': ['loginID', 'name', 'mobile', 'accountType'], 
                       'changePassword': ['oldPassword', 'newPassword'], 'updateUser': ['loginID', 'name', 'mobile'], 'dashboard': True,
                       'kycStatus': True, 'createTask': ['loginID', 'update', 'taskListID', 'task', 'taskDatetime', 'status'],
                       'showTask': [{'page':['startIndex','size']}, 'days', 'loginID', 'custID', 'loanID', 'fromDate', 'toDate', 'status'],
                       'searchUser': [{'page':['startIndex','size']}, 'days', 'fromDate', 'toDate', 'searchBy', 'searchText', 'accountStatus'], 
                       'clientUpdate': ['customerID', 'update'], 'clientRegister': ['customerID'], 'stageSync': ['custID'],
                       'listLoans':[{'page':['startIndex','size']}, 'days', 'fromDate', 'toDate', 'loanStatus'],
		       'listRepayments':[{'page':['startIndex','size']}, 'days', 'fromDate', 'toDate', 'repayStatus'],'tyreLimit':['customerID'],
                       'clientRegister2': ['customerID','lender'], 'mlUpload': ['customerID', 'docTypeID'], 'appActivityLogs': ["days"],
                       'pushLoansToLender': ['customerID', 'fund'],'fileUploadTest':[],'customerIdCount':['fund'],'searchCustomerId':['ifsc_code','account_no'],
		       'aadharFill': ['customerID', 'aadharNo', 'name', 'dob', 'gender', 'house', 'street', 'careOf', 'landMark',
                                      'locality', 'villageTownCity', 'district', 'subDistrict', 'pinCode', 'postOffice', 'state', 'country'],'changeLogin':['existingId','newId'],
                       'loanApplicationRequest':['customerID', 'loanAmount', 'loanProductID'], 'custDetails': ['customerID'],'approveLoan':['id', 'principal', 'loanID'],
                       'investorTransaction': [{'page': ['startIndex','size']}, 'days','fromDate', 'toDate', 'paymentStatus', 'transactionStatus',
                                               
			'customerID'], 'paymentDisbursalReport': ['loanRefIDs'], 'showSmsExtract':[{'page':['startIndex','size']}, 'customerID'], 
		      'createRepaymentInfo':['repayInfo', 'payMode', 'repayDatetime', 'depositDatetime', 'repayID', 'loanID', 'acceptBy', 'custID', 
		       'amount', 'update'], 'incomeDataUpload':['docType'], 'mandateDataUpload':['docType', 'format'],
                       'insertIfsc':['status', 'ifsc', 'micr', 'address', 'bank', 'city', 'district', 'state', 'contact', 'branch', 'bankcode'],
		       'showRepayInfo': [{'page':['startIndex','size']}, 'days', 'loanID', 'fromDate', 'toDate', 'repayID', 'modeOfPayment'],
		       'showMandateData': [{'page':['startIndex','size']}, 'days', 'fromDate', 'toDate', 'mandateID'], 'sendSms':["numbers", "message"], 'showSmsTemplate': ['language', 'type'], 'loanLimit':['customerID', 'companyName', 'workExp'],
                       'updateRepayInfo':['customerID', 'loanRefID', 'repayID'], 'createCallInfo':['customerID', 'comments', 'reasonID', 'resolutionID', 'id', 'update','contactinfos'], 'getResolutionList': ['reasonID'], 'storeCustomerData':['dataKey', 'dataValue', 'loginID'], 'mapUberAuth':["driverID", "custID"]}
        return self._exists_(data, assert_dict[indict['api']]) if data else True #assert_dict[indict['api']]

    @staticmethod
    def _exists_(data={}, keys=None):
        '''This module is used to check if the dictionary specified by arguent "data" contains all keys specified in a list by argument
        "keys". This also tests whether the length of "keys" list is same as that of "data" dictionary. Returns True if the test passes and
        False otherwise.
        '''
        if isinstance(keys, list) & isinstance(data, dict):
            return all(key in data if isinstance(key,str) else (list(key.values())[0]==list(data[list(key.keys())[0]].keys()) if list(key.keys())[0] in data else False) 
                        if isinstance(key, dict) and len(key)==1 else False for key in keys) #checking if keys exist in data. 
                       #if a particular key is a dictionary then it goes to one level deep in nesting 
        elif isinstance(keys, list) & isinstance(data, list):
            return all(all(key in data[i] if isinstance(key,str) else (list(key.values())[0]==list(data[i][list(key.keys())[0]].keys()) 
                                                                       if list(key.keys())[0] in data[i] else False)
                        if isinstance(key, dict) and len(key)==1 else False for key in keys) for i in range(len(data)))
        else:
            return False
    

    def compulsory(self, table='mw_admin_user_master', keys=[]):
        '''Checks if request contains all the compulsory fields of the table or not. Returns True if test passes and False otherwise.
        '''
        junk = self.cursor.execute("select group_concat(column_name) from information_schema.columns where table_name='%s' and "%table + 
                                   "IS_NULLABLE='NO' and COLUMN_NAME!='AUTO_ID'")
        data = self.cursor.fetchall()[0][0]
        data = data.split(',') if data is not None else []
        data.sort()
        keys.sort()
        if ( set(keys).issubset(data) ) & ( (data == keys) ):#( set(keys).issubset(CompulsoryFields[table]) ) :
            return True
        else:
            return False

    def DBfields(self, table='mw_admin_user_master', keys=[], checkAll=True):
        '''Checks if request contains all the fields of the table or not. Returns True if test passes and False otherwise.
        '''
        junk = self.cursor.execute("select group_concat(column_name) from information_schema.columns where table_name='%s'"%table)
        data = self.cursor.fetchall()[0][0]
        data = data.split(',') if data is not None else []
        if ( set(keys).issubset(data) & (keys!=[])) & ( (data == keys) or (not checkAll) ):
            return True
        else:
            return False
    
    def basicChecks(self, token='', loginID='', checkLogin=False, checkType=False, checkToken=True, negate=True, loginAuth=False):
        '''Performs basic validations. It calls all individual validation modules one by one unless the validation is disabled by
        input flags checkLogin (default False), checkType (default False) and checkToken (Default True) which are used to check if 
        user id specified by the argument "loginID" (default '') exists in database, whether the user type is SUPERUSER' or not 
        and whether or not to check the AuthToken respectively. One additional argument "negate" (default True) to check whether
        user does not exist in database (negating the condition for the login id check). 

        It returns 0 if all checks are passed,
        returns 5 if the authLoginID (set in the instance of DB or Validate class) does not exist in database, 4 if the type
        of the user is not 'SUPERUSER', 3 if AuthToken is incorrect, 1 if AuthToken is expired, 11 if user exists in database
        (when negate is True) and 2 if user does not exist in database.
        '''
        if not self.User(): # validate authLoginID (exist in database)
            return utils.errors["login"]
        elif ((not self.Type()) & (checkType)): # validate whether user is SUPERUSER
            return utils.errors["rights"]
        elif ((not self.AuthToken(token, checkTstamp=True, checkToken=True, loginAuth=loginAuth)["token_ok"]) & checkToken): # validate authToken against authLoginID
            return utils.errors["credentials"]
        elif ((not self.AuthToken(token, checkTstamp=True, checkToken=True, loginAuth=loginAuth)["tstamp_ok"]) & checkToken):# validate expiry of authToken
            return utils.errors["timeout"]
        elif ((self.User(loginID) ^ negate) & (checkLogin)): # validate whether user does not exist (or check if user exists when negate=False)
        # Used xor (^) 
            return utils.errors["user"] if negate else utils.errors["userExists"]
        else: # if everything is valid then return 0
            return False
