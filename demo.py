
from mintloan_utils import DB, generate, validate, datetime, timedelta, utils, choice
import json
ut=utils()
cid="0"
logInfo={"api":"bulkupload"}
try:
    response = ut.update_basic_profile(loginId="+918691945293",
                                        authToken='', companyName='BIJNIS',
                                        companyID="3",
                                        currentCity="mumbai",
                                        name="nagendra")
                #if cid != "0" else {"data": {"successFlag": False}})
    #utils.logger.info("api response: " + json.dumps(response, extra=logInfo))
    #profileres=response
    profileUpdated = "1" if not (response["data"]["successFlag"]) else "0"
except:
    profileUpdated = "0"
print(response)
print(type(response))
print(profileUpdated)
'''
import json
response=json.dumps({
"header":{
"loginId":"+918927839742",
"authToken":"49dc25b44fdfce13b7ag3fcgc34g372c133fa93d",
"timeStamp":"2021-01-21T02:14:47.269+0000",
"txnRefNum":"9035671935124508",
"hostStatus":"S",
"error":{
"errorCode":"",
"errorDesc":"",
"errorMessage":""
}
},
"data":{
"successFlag":"true",
"errorMsg":""
}
}
)
print(response)
'''