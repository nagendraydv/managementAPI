from mintloan_utils import DB,utils, datetime, timedelta
from pypika import Query, Table    
db = DB(filename='mysql.config')
lm = Table('mw_client_loan_master',schema='mint_loan')
kycDoc = Table('mw_cust_kyc_documents',schema = 'mint_loan')
cust = db.runQuery(Query.from_(kycDoc).select('CUSTOMER_ID').where(kycDoc.CREATED_DATE>'2019-09-01'))['data']
for i in range(len(cust)):
    custID=(cust[i]['CUSTOMER_ID'])
    #print(custID)
    q = Query.from_(kycDoc).select('CREATED_DATE','DOC_SEQ_ID').where((kycDoc.CREATED_DATE>'2019-09-01') & (kycDoc.DOCUMENT_TYPE_ID==129) & (kycDoc.CUSTOMER_ID==custID))
    kycData = db.runQuery(q)
    lmData = Query.from_(lm).select('CREATED_DATE').where((lm.CUSTOMER_ID ==custID) & (lm.CREATED_DATE>'2019-09-01'))
    lmData = db.runQuery(lmData)
    for i in range(len(lmData['data'])):
        minimum =[]
        if kycData['data']!=[]:
            for j in range(len(kycData['data'])):
                td =(datetime.strptime(kycData['data'][j]['CREATED_DATE'],'%Y-%m-%d %H:%M:%S')-datetime.strptime(lmData['data'][i]['CREATED_DATE'],'%Y-%m-%d %H:%M:%S'))
                docSecID = (abs(td),kycData['data'][j]['DOC_SEQ_ID'])
                minimum.append(docSecID)
            res = [lis[0] for lis in minimum]
            index= res.index(min(res))
            doc_sec_id_list = [lis[1] for lis in minimum]
            doc_sec_id = doc_sec_id_list[index]
            print(doc_sec_id)
            clmDate=datetime.strptime(lmData['data'][i]['CREATED_DATE'],'%Y-%m-%d %H:%M:%S')
            indict = {'LOAN_AGREEMENT_ID':str(doc_sec_id)}
            updated = db.Update(db='mint_loan',table='mw_client_loan_master',conditions={'CREATED_DATE =':str(clmDate)},debug=False,**indict)
            if updated:
                print("true")
        else:
            print("data not found")