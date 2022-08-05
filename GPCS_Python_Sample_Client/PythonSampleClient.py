
import array
import base64
import certifi
import datetime
import time
import uuid
from pprint import pprint
import json
import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes



def createAuthToken():
    keypath = ('C:/Users/andrew.gustafson/Desktop/3M_Grouper_API/cert/'
               '2420151_SiteCoversAllMillimanSharedDataSources_Cert/PEM/')
    certname = '2420151_SiteCoversAllMillimanSharedDataSources_Cert'
    private_key_path = keypath + certname + '_key.pem'
    public_key_path = keypath + certname + '_cert.pem'
    public_key_file = open(public_key_path, mode='rb')
    public_key = public_key_file.read()
    private_key_file = open(private_key_path, mode='rb')
    private_key = private_key_file.read()
    cert = x509.load_pem_x509_certificate(public_key)
    fingerprint = cert.fingerprint(hashes.SHA1())
    kid = fingerprint.hex()
    x5t = base64.urlsafe_b64encode(fingerprint).decode('utf-8')

    # load client public and private key
    payload = {
            'jti' : str(uuid.uuid4()),
            'iat' : datetime.datetime.utcnow(),
            'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
            'aud' : 'http://gpcs.3m.com'
            }
    headers = {
            'x5t' : x5t,
            'kid' : kid
            }
    authToken = jwt.encode(payload, private_key, algorithm='RS256', headers=headers)
    return authToken
#Generates OAuth Token
def generateAccessToken():
    authToken = createAuthToken()
    response = requests.post('https://gpcs.3m.com/GpcsSts/oauth/token', json={
        'grant_type' : 'jwt-bearer',
        'assertion' : authToken
        })
    if response.status_code != 200:
        raise Exception('STS authentication error', response.status_code)
    accessToken = response.json()['access_token']
    return accessToken

#Build request and serialize into JSON
def setupRequest():
    procedure = {"code": "5A1955Z"}
    procedureList = [procedure]

    diagnosis1 = {"code": "A4189"}
    diagnosis2 = {"code": "E876"}
    diagnosis3 = {"code": "F79"}
    diagnosis4 = {"code": "I10"}
    diagnosis5 = {"code": "J1289"}
    diagnosis6 = {"code": "J9601"}
    diagnosis7 = {"code": "N179"}
    diagnosis8 = {"code": "U071"}
    diagnosis9 = {"code": "Z79899"}
    diagnosis10 = {"code": "Z8673"}

    diagnosisList = [diagnosis1, diagnosis2, diagnosis3, diagnosis4, diagnosis5, diagnosis6, diagnosis7, diagnosis8, diagnosis9, diagnosis10]
    

    claimInputList = {
     "icdVersionQualifier": "0" ,
     "admitDate": "2020-11-25" ,
     "ageInYears": 51 ,
     "diagnosisList": diagnosisList,
     "dischargeDate": "2021-01-02" ,
     "dischargeStatus": "01" ,
     "procedureList": procedureList,
     "sex": "F" ,
     "totalCharges": 0 
    }

    processingOptions = {
    "contentVersion": '2022.2.1', 
    "grouperType": "APR", 
    "grouperVersion": "390" 
    }


    claimInputListArray = [claimInputList]


    request = {
    "claimInputList": claimInputListArray, 
    "processingOptions": processingOptions
    }

    requestListArray = [request]

    requestList = {"requestList": requestListArray}

    requestBody = json.dumps(requestList)
    
    return requestBody


#call GPCS REST service and print response
class main():
    accessToken = generateAccessToken()

    requestBody = setupRequest()

    headers = {"Authorization": "Bearer "+accessToken, "Content-Type": "application/json", "accept": "application/json"}

    endpoint = "https://gpcs.3m.com/Gpcs/rest/2022.2.1/claims/process"

    claimRequest = requests.post(endpoint, data=requestBody, headers=headers, timeout=60)

    if claimRequest.status_code != 200:
        raise Exception('Claims Processing Error', claimRequest.status_code, claimRequest.content)
        
    print(json.dumps(claimRequest.json(), indent=1))
    