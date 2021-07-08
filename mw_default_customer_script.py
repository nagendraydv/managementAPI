from mintloan_utils import DB,utils, datetime, timedelta
from pypika import Query, Table    
db = DB(filename='mysql.config')
lm = Table('mw_client_loan_master',schema='mint_loan')
cust = db.runQuery(Query.from_(lm).select('CUSTOMER_ID').distinct().where(lm.status=='written-off'))['data']
list1=[]
for i in range(len(cust)):
    custID=(cust[i]['CUSTOMER_ID'])
    list1.append(custID)
    inserted = db.Insert(db="mint_loan", table='mw_default_customer', compulsory=False, date=False,
                                         customer_id=str(custID),active=str(1),CREATED_BY="nagendra@supermoney.com",
                                         CREATED_DATE=(datetime.utcnow() + timedelta(seconds=19800)).strftime("%Y-%m-%d %H:%M:%S"))
if inserted:
    print("inserted")
else:
    print("not inserted")