import requests

url = "https://dev.supermoney.in:8443/python/oauth2/token/"

payload = "{\n  \"client_id\": \"admin@mintloan.com\",\n  \"client_secret\": \"$2b$12$.V6Y7d2frFjSvD7nbbO6Hugj0QfLEG0Y57fSxdo32tE0Z36bEH.YS\",\n  \"grant_type\": \"password\",\n  \"provision_key\": \"utTRWQf2eyAT2OWLW4Jh8F7USbIbCXid\",\n  \"authenticated_userid\": \"admin@mintloan.com\",\n  \"scope\": \"email\"\n}"
headers = {
    'Content-Type': "application/json",
    'Cache-Control': "no-cache",
    'Postman-Token': "e272817a-dbcc-4210-8c43-f674eb114321"
    }

response = requests.request("POST", url, data=payload, headers=headers,verify=False)

print(response.text)