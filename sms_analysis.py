import re
from mintloan_utils import DB, utils, datetime, timedelta
import numpy as np
from pypika import Query, Table, functions
import dateutil.parser
import uuid
from confluent_kafka import  Consumer
db=DB()

c = Consumer({
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'NU32UVCFLKY7NVE6',
    'sasl.password': 'Y1TmlzwJrNlJ9RV6NvIosJ9kvToSvY578A/tpU24k60WXYBC5wqKQ/ehOXYmu3mf',
    'group.id': str(uuid.uuid1()),
    'auto.offset.reset': 'earliest'
})

def RepresentsFloat(s):
    try:
        float(s.replace(",",""))
        return True
    except:
        return False
def validator(date):
    try:
        if dateutil.parser.parse(date):
            return dateutil.parser.parse(date).strftime("%Y-%m-%d")
    except:
        return None
cat = '(?:credit(?:ed)?(?:\ +Card)?)|(?:\ pa[yi][md](?:ent)?)|(?:outstanding)|(?:purchase)|(?:due)|(?:Overdue)|(?:emi)|(?:loan)|'
cat += '(?:debit(?:ed?)?(?:\ +Card)?)|(?:bal(?:[a\ +](?:nce)?))|(?:withdraw(?:n)?)|(?:deposit(?:ed)?)|(?:add(?:ed)?)'
amount = re.compile('((?:(?:rs|inr|balance:)\.?\s?)(-?[\d,]+(?:\.\d+)?)(?:[^/^-^X^x]))|(-?[\d,]+(?:\.\d+)?)\s?(?:(?:inr))', re.IGNORECASE)
account = re.compile('((?:(?:[Xx]+\ {0,})(?:\d+)(?:[Xx]+)?)|(\.\.\.\d{1,5}))')
#txndate = re.compile('\d\d(?:\d\d)[/\\\.-]{1,}(?:\d[12])[/\\\.-]{1,}\d\d(?:\d\d)?')
txndate = re.compile('((19|20)\d\d[/\\\.-]{1,}(0[1-9]|1[012])[/\\\.-]{1,}(0[1-9]|[12][0-9]|3[01]))')
txndate2=re.compile('((0[1-9]|[12][0-9]|3[01])[/\\\.-]{1,}(0[1-9]|1[012])[/\\\.-]{1,}(19|20)\d\d)')
txndate1='(\d\d[/\\\.-]{1,}(?:[Jj][Aa][Nn])[/\\\.-]{1,}\d\d(?:\d\d?\s))|(\d\d[/\\\.-]{1,}(?:[Ff][Ee][Bb]?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:[Mm][Aa][Rr]?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:[Aa][Pp][Rr]?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:[Mm][Aa][Yy]?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:jun?)[/\\\.-]{1,}\d\d(?:\d\d?))|'
txndate1+='(\d\d[/\\\.-]{1,}(?:jul?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:aug?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:sep?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:[Oo][Cc][Tt]?)[/\\\.-]{1,}\d\d(?:\d\d?))|(\d\d[/\\\.-]{1,}(?:nov?)[/\\\.-]{1,}\d\d(?:\d\d?))'
count=0
devFootPrint=Table("mw_device_footprint",schema="mint_loan")
#print(txndate1)
#file=os.listdir("/home/nagendra/Documents/sms")
#file=[ele for ele in file if ele.split("_")[-1]!="proccessed.log"]
#for j in range(len(file)):
    #with open("/home/nagendra/Documents/sms/"+str(file[j])) as f:
        #file_data=f.read()
        #file_data=file_data.split('\n')
c.subscribe(['captureSMS'])

try:
    while True:
        msg = c.poll(0.1)  # Wait for message or event/error
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to `session.timeout.ms` for
            #   the group to rebalance and start consuming.
            continue
        if msg.error():
            # Errors are typically temporary, print error and continue.
            print("Consumer error: {}".format(msg.error()))
            continue

        #print('consumed: {}'.format(msg.value()))
        print((msg.value()).decode('utf-8'))
        file_data=str((msg.value()).decode('utf-8'))
        #sms_no=0
        #deviceIDList=[]
        #unprocessedSmsNo=[]
        #print(file_data[0])
        #amount = re.compile('((?:(?:rs|inr|balance:)\.?\s?)(-?[\d,]+(?:\.\d+)?)(?:[^/^-^X^x]))|(-?[\d,]+(?:\.\d+)?)\s?(?:(?:inr))', re.IGNORECASE)
        #txndate = re.compile('\d\d[/\\\.-]{1,}\d\d[/\\\.-]{1,}\d\d(?:\d\d)?')
        #for i in range(len(file_data)):
        #sms_no+=1
        if (re.findall(cat,file_data,re.IGNORECASE)!=[]):
            mode=re.findall(cat,file_data,re.IGNORECASE)[0]
            if (mode not in ["credit","Credit","paid"])&(len(re.findall(amount,file_data))>0):
                if len(file_data.split("|"))>4:
                    if re.findall(txndate,file_data.split("|")[2])!=[]:
                        tax_date=[re.findall(txndate,file_data.split("|")[2])[0][0]]
                    elif (re.findall(txndate1,file_data.split("|")[2])!=[]):
                        tax_date=re.findall(txndate1,file_data.split("|")[2])[0]
                        tax_date=[ele for ele in tax_date if ele!='']
                    elif re.findall(txndate2,file_data.split("|")[2])!=[]:
                        tax_date=[re.findall(txndate2,file_data.split("|")[2])[0][0]]
                    else:
                        tax_date=[]
                    txn_data=[{"account_no":re.findall(account,file_data)[0][0] if re.findall(account,file_data) else 0,
                               "mode":re.findall(cat,file_data,re.IGNORECASE)[0] if re.findall(cat,file_data,re.IGNORECASE) else None,
                               "amount":re.findall(amount,file_data)[0][1].replace(",","") if len(re.findall(amount,file_data))>0 else 0.0,
                               "total_balance":re.findall(amount,file_data)[1][1].replace(",","") if len(re.findall(amount,file_data))>1 else 0.0,
                               "transaction_date":dateutil.parser.parse(tax_date[0]).strftime("%y-%m-%d") if tax_date!=[] else None,
                               "sms_date":validator(file_data.split("|")[3]) if file_data.split("|")[3]!='' else None,
                               "deviceID":file_data.split("|")[0] if len(file_data.split("|")[0])<20 else None,
                               "vendor":file_data.split("|")[1].split('-')[-1] if len(file_data.split("|")[1])<20 else None}]
                    if txn_data[0]["deviceID"] not in deviceIDList:
                        deviceIDList.append(txn_data[0]["deviceID"]) 
                        q1=Query.from_(devFootPrint).select(devFootPrint.CUSTOMER_ID).where(devFootPrint.DEVICE_ID==txn_data[0]["deviceID"])
                        customer_id=db.runQuery(q1)
                        #print(customer_id)
                        if customer_id["data"]!=[]:
                            customerID=str(customer_id["data"][-1]["CUSTOMER_ID"])
                        else:
                            customerID=None
                    junk = db.Insert(db="sms_analytics", table="sms_data", compulsory=False,date=False,
                                     account_no=txn_data[0]["account_no"] if txn_data[0]["account_no"] else None,
                                     vendor=txn_data[0]["vendor"] if txn_data[0]["vendor"] else None,
                                     mode=txn_data[0]["mode"].strip() if txn_data[0]["mode"]!='' else None,
                                     amount=txn_data[0]["amount"] if txn_data[0]["amount"] else 0.0,
                                     total_balance=txn_data[0]["total_balance"] if txn_data[0]["total_balance"] else 0.0,
                                     transaction_date=txn_data[0]["transaction_date"] if txn_data[0]["transaction_date"] else None,
                                     sms_date=txn_data[0]["sms_date"] if txn_data[0]["sms_date"] else None,
                                     device_id=txn_data[0]["deviceID"],sms_no=str(sms_no),filename=None,customer_id=customerID)
            else:
                unprocessedSmsNo.append(file[j]+str(sms_no))
                with open("/home/nagendra/Documents/sms/unprocessed.txt",'w+') as f:
                    f.write(str(unprocessedSmsNo))
        #print(unprocessedSmsNo)
        #print(len(unprocessedSmsNo))
except KeyboardInterrupt:
    pass

finally:
    # Leave group and commit final offsets
    c.close()                
        #print(deviceIDList)
    #os.rename("/home/nagendra/Documents/sms/"+str(file[j]),"/home/nagendra/Documents/sms/"+str(file[j]).split(".")[0]+str("_proccessed.log"))