#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 12:57:41 2021

@author: nagendra
"""


from mintloan_utils import DB, utils, datetime, timedelta
from pypika import Query, Table, functions
import dateutil.parser
db=DB()
one_cred = Table("mw_customer_login_credentials",schema="one_family")
kyc = Table("mw_cust_kyc_documents",schema="mint_loan")
depend = Table("mw_dependent_profile",schema="mint_loan")
quote = Table("mw_product_quotes",schema="mint_loan")
payment = Table("mw_client_loan_repayment_history_master",schema="mint_loan")
prof = Table("mw_client_profile",schema="mint_loan")
q1=Query.from_(one_cred).select(one_cred.SUPERMONEY_CUSTOMER_ID,one_cred.STAGE).where(one_cred.STAGE.isin (["AWAITING_PH_DOCUMENTS"]))
customer_id=db.runQuery(q1)
#print(customer_id)
if customer_id["data"]!=[]:
    customerID=[(ele["STAGE"],ele["SUPERMONEY_CUSTOMER_ID"]) for ele in customer_id["data"]]
else:
    customerID=None
#print(customerID)
for i in range(len(customerID)):
    print(customerID[i][1])
    if customerID[i][0]=="AWAITING_PH_DOCUMENTS":
        aadharFrontExistPri= db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.CUSTOMER_ID==customerID[i][1])))["data"]
        aadharBackExistPri= db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.CUSTOMER_ID==customerID[i][1])))["data"]
        family_info = db.runQuery(Query.from_(depend).select('*').where(depend.CUSTOMER_ID==customerID[i][1]))["data"]
        quote_info = db.runQuery(Query.from_(quote).select('*').where(quote.CUSTOMER_ID==customerID[i][1]))["data"]
        if quote_info!=[]:
            quote_id=quote_info[0]["AUTO_ID"]
        else:
            quote_id=None
        print(quote_id)
        pay = db.runQuery(Query.from_(payment).select(payment.ID).where((payment.LOAN_ID==quote_id) & (payment.TRANSACTION_STATUS=="SUCCESS") & (payment.PURPOSE=="ONE-FAMILY")))["data"]
        if pay!=[]:
            isPayment=True
        else:
            isPayment=False
        profile = db.runQuery(Query.from_(prof).select(prof.PROFILE_ID).where((prof.CUSTOMER_ID==customerID[i][1]) & (prof.IS_PRIMARY!=1)))["data"]
        if profile!=[]:
            prof_id= [ele["PROFILE_ID"] for ele in profile]
        else:
            prof_id=[]
        #for k in range(len(prof_id)):
        aadhar = db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.PROFILE_ID.isin(prof_id))))["data"]
        if aadhar!=[]:
            aadharExist=True
        else:
            aadharExist=False
        print(aadharExist)
        print(isPayment)
        print(quote_info)
        print(family_info)
        if aadharFrontExistPri!=[] and aadharBackExistPri!=[] and family_info!=[] and quote_info!=[] and isPayment and aadharExist:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")
        elif aadharFrontExistPri!=[] and aadharBackExistPri!=[] and family_info!=[] and quote_info!=[] and isPayment:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_SH_DOCUMENTS")
        elif aadharFrontExistPri!=[] and aadharBackExistPri!=[] and family_info!=[] and quote_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_PAYMENT")
        elif aadharFrontExistPri!=[] and aadharBackExistPri!=[] and family_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_AGREEMENT")
        elif aadharFrontExistPri!=[] and aadharBackExistPri!=[] and family_info!=[]:   
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_FAMILY_INFO")
        else:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_PH_DOCUMENTS")
    elif customerID[i][0]=="AWAITING_FAMILY_INFO":
        family_info = db.runQuery(Query.from_(depend).select('*').where(depend.CUSTOMER_ID==customerID[i][1]))["data"]
        quote_info = db.runQuery(Query.from_(quote).select('*').where(quote.CUSTOMER_ID==customerID[i][1]))["data"]
        if quote_info!=[]:
            quote_id=quote_info[0]["AUTO_ID"]
        else:
            quote_id=None
        print(quote_id)
        pay = db.runQuery(Query.from_(payment).select(payment.ID).where((payment.LOAN_ID==quote_id) & (payment.TRANSACTION_STATUS=="SUCCESS") & (payment.PURPOSE=="ONE-FAMILY")))["data"]
        if pay!=[]:
            isPayment=True
        else:
            isPayment=False
        profile = db.runQuery(Query.from_(prof).select(prof.PROFILE_ID).where((prof.CUSTOMER_ID==customerID[i][1]) & (prof.IS_PRIMARY!=1)))["data"]
        if profile!=[]:
            prof_id= [ele["PROFILE_ID"] for ele in profile]
        else:
            prof_id=[]
        #for k in range(len(prof_id)):
        aadhar = db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.PROFILE_ID.isin(prof_id))))["data"]
        if aadhar!=[]:
            aadharExist=True
        else:
            aadharExist=False
        print(aadharExist)
        print(isPayment)
        print(quote_info)
        print(family_info)
        if family_info!=[] and quote_info!=[] and isPayment and aadharExist:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")
        elif family_info!=[] and quote_info!=[] and isPayment:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_SH_DOCUMENTS")
        elif  family_info!=[] and quote_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_PAYMENT")
        elif family_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_AGREEMENT")
        else:   
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_FAMILY_INFO")
    elif customerID[i][0]=="AWAITING_AGREEMENT":
        quote_info = db.runQuery(Query.from_(quote).select('*').where(quote.CUSTOMER_ID==customerID[i][1]))["data"]
        if quote_info!=[]:
            quote_id=quote_info[0]["AUTO_ID"]
        else:
            quote_id=None
        print(quote_id)
        pay = db.runQuery(Query.from_(payment).select(payment.ID).where((payment.LOAN_ID==quote_id) & (payment.TRANSACTION_STATUS=="SUCCESS") & (payment.PURPOSE=="ONE-FAMILY")))["data"]
        if pay!=[]:
            isPayment=True
        else:
            isPayment=False
        profile = db.runQuery(Query.from_(prof).select(prof.PROFILE_ID).where((prof.CUSTOMER_ID==customerID[i][1]) & (prof.IS_PRIMARY!=1)))["data"]
        if profile!=[]:
            prof_id= [ele["PROFILE_ID"] for ele in profile]
        else:
            prof_id=[]
        #for k in range(len(prof_id)):
        aadhar = db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.PROFILE_ID.isin(prof_id))))["data"]
        if aadhar!=[]:
            aadharExist=True
        else:
            aadharExist=False
        print(aadharExist)
        print(isPayment)
        print(quote_info)
        print(family_info)
        if family_info!=[] and quote_info!=[] and isPayment and aadharExist:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")
        elif family_info!=[] and quote_info!=[] and isPayment:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_SH_DOCUMENTS")
        elif  family_info!=[] and quote_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_PAYMENT")
        else:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_AGREEMENT")
    elif customerID[i][0]=="AWAITING_PAYMENT":
        
        family_info = db.runQuery(Query.from_(depend).select('*').where(depend.CUSTOMER_ID==customerID[i][1]))["data"]
        quote_info = db.runQuery(Query.from_(quote).select('*').where(quote.CUSTOMER_ID==customerID[i][1]))["data"]
        if quote_info!=[]:
            quote_id=quote_info[0]["AUTO_ID"]
        else:
            quote_id=None
        print(quote_id)
        pay = db.runQuery(Query.from_(payment).select(payment.ID).where((payment.LOAN_ID==quote_id) & (payment.TRANSACTION_STATUS=="SUCCESS") & (payment.PURPOSE=="ONE-FAMILY")))["data"]
        if pay!=[]:
            isPayment=True
        else:
            isPayment=False
        profile = db.runQuery(Query.from_(prof).select(prof.PROFILE_ID).where((prof.CUSTOMER_ID==customerID[i][1]) & (prof.IS_PRIMARY!=1)))["data"]
        if profile!=[]:
            prof_id= [ele["PROFILE_ID"] for ele in profile]
        else:
            prof_id=[]
        #for k in range(len(prof_id)):
        aadhar = db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.PROFILE_ID.isin(prof_id))))["data"]
        if aadhar!=[]:
            aadharExist=True
        else:
            aadharExist=False
        print(aadharExist)
        print(isPayment)
        print(quote_info)
        print(family_info)
        if family_info!=[] and quote_info!=[] and isPayment and aadharExist:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")
        elif family_info!=[] and quote_info!=[] and isPayment:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_SH_DOCUMENTS")
        elif  family_info!=[] and quote_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_PAYMENT")
        elif family_info!=[]:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_AGREEMENT")
        else:   
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_FAMILY_INFO")
    elif customerID[i][0]=="AWAITING_AGREEMENT":
        quote_info = db.runQuery(Query.from_(quote).select('*').where(quote.CUSTOMER_ID==customerID[i][1]))["data"]
        if quote_info!=[]:
            quote_id=quote_info[0]["AUTO_ID"]
        else:
            quote_id=None
        print(quote_id)
        pay = db.runQuery(Query.from_(payment).select(payment.ID).where((payment.LOAN_ID==quote_id) & (payment.TRANSACTION_STATUS=="SUCCESS") & (payment.PURPOSE=="ONE-FAMILY")))["data"]
        if pay!=[]:
            isPayment=True
        else:
            isPayment=False
        profile = db.runQuery(Query.from_(prof).select(prof.PROFILE_ID).where((prof.CUSTOMER_ID==customerID[i][1]) & (prof.IS_PRIMARY!=1)))["data"]
        if profile!=[]:
            prof_id= [ele["PROFILE_ID"] for ele in profile]
        else:
            prof_id=[]
        #for k in range(len(prof_id)):
        aadhar = db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.PROFILE_ID.isin(prof_id))))["data"]
        if aadhar!=[]:
            aadharExist=True
        else:
            aadharExist=False
        print(aadharExist)
        print(isPayment)
        print(quote_info)
        print(family_info)
        if quote_info!=[] and isPayment and aadharExist:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")
        elif  quote_info!=[] and isPayment:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_SH_DOCUMENTS")
        else:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_PAYMENT")
    elif customerID[i][0]=="AWAITING_SH_DOCUMENTS":
        aadhar = db.runQuery(Query.from_(kyc).select(kyc.DOCUMENT_TYPE_ID).where((kyc.DOCUMENT_TYPE_ID=="1082") & (kyc.DOCUMENT_TYPE_ID=="1081") & (kyc.PROFILE_ID.isin(prof_id))))["data"]
        if aadhar!=[]:
            aadharExist=True
        else:
            aadharExist=False
        print(aadharExist)
        print(isPayment)
        print(quote_info)
        print(family_info)
        if aadharExist:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")
        else:
            pass
            #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_SH_DOCUMENTS")
    elif customerID[i][0]=="AWAITING_KYC":
        pass
        #updated = db.Update(db="one_family", table="mw_customer_login_credentials", conditions={"SUPERMONEY_CUSTOMER_ID = ":str(customerID[i][1])}, STAGE="AWAITING_KYC")