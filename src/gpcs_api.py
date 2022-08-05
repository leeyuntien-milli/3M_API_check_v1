import base64
import datetime
import json
import math
import uuid
from typing import Dict, List, Optional, Set, Tuple
from cryptography import x509
from cryptography.hazmat.primitives import hashes
import jwt
import requests

def convert_format_to_csv(input_file: str,
                          output_file: str,
                          input_format: str = 'A',
                          return_lines: bool = False) -> Optional[List[str]]:
    flines: List[str] = list()
    with open(input_file, 'r') as file_in:
        flines = file_in.readlines()
    result: List[str] = list()
    if input_format == 'A':
        diagnosis_width: int = 8
        diagnosis_start: int = 94
        diagnosis_count: int = 50
        diagnosis_poa_width: int = 1
        diagnosis_poa_start: int = (
            diagnosis_start + diagnosis_width*diagnosis_count
        )
        diagnosis_poa_count: int = 50
        procedure_width: int = 7
        procedure_start: int = (
            diagnosis_poa_start + diagnosis_poa_width*diagnosis_poa_count
        )
        procedure_count: int = 50
        end_start: int = procedure_start + procedure_width*procedure_count
        boundaries: List[int] = [
            0,
            40,
            50,
            60,
            70,
            72,
            82,
            85,
            86
        ] + [
            diagnosis_start + diagnosis_width*i
            for i in range(diagnosis_count)
        ] + [
            diagnosis_poa_start + diagnosis_poa_width*i
            for i in range(diagnosis_poa_count)
        ] + [
            procedure_start + procedure_width*i
            for i in range(procedure_count)
        ] + [
            end_start,
            end_start + 1,
            end_start + 2
        ]
        blim: int = len(boundaries) - 1
        result = [
            ','.join([
                line[boundaries[i]:boundaries[i+1]].strip()
                for i in range(0, blim)
            ])
            for line in flines
        ]
    else:
        print('Unknown input_format.')
    with open(output_file, 'w') as file_out:
        file_out.write('\n'.join(result))
    return (result if return_lines else None)

def generate_gpcs_auth_token(
    cert_basedir: str,
    cert_basename: str,
    duration_minutes: int
) -> str:
    private_key_file = open(
        cert_basedir + '/' + cert_basename + '_key.pem',
        mode='rb'
    )
    private_key = private_key_file.read()
    private_key_file.close()
    public_key_file = open(
        cert_basedir + '/' + cert_basename + '_cert.pem',
        mode='rb'
    )
    public_key = public_key_file.read()
    public_key_file.close()

    cert = x509.load_pem_x509_certificate(public_key)
    fingerprint = cert.fingerprint(hashes.SHA1())
    kid = fingerprint.hex()
    x5t = base64.urlsafe_b64encode(fingerprint).decode('utf-8')

    base_gpcs_url: str = 'http://gpcs.3m.com'
    now_time = datetime.datetime.utcnow()
    payload: dict = {
        'jti' : str(uuid.uuid4()),
        'iat' : now_time,
        'exp' : now_time + datetime.timedelta(minutes=duration_minutes),
        'aud' : base_gpcs_url
    }
    headers: dict = {
        'x5t' : x5t,
        'kid' : kid
    }
    auth_token = jwt.encode(
        payload,
        private_key,
        algorithm='RS256',
        headers=headers
    )

    return auth_token

def get_gpcs_oath_access_token(auth_token: str) -> str:
    auth_token_request_url: str = 'https://gpcs.3m.com/GpcsSts/oauth/token'
    response = requests.post(
        auth_token_request_url,
        json={
            'grant_type' : 'jwt-bearer',
            'assertion' : auth_token
        }
    )
    if response.status_code != 200:
        raise Exception('STS authentication error', response.status_code)
    access_token = response.json()['access_token']
    return access_token

def claims_file_to_list(input_file: str,
                        input_format: str = 'A') -> List[dict]:
    claim_lines: List[str] = list()
    with open(input_file, 'r') as file_in:
        claim_lines = file_in.readlines()
    result: List[dict] = [dict()]
    if input_format == 'A':
        result = [
            {
                'patientId': claim[0],
                'claimId': claim[1],
                'admitDate': claim[2],
                'dischargeDate': claim[3],
                'dischargeStatus': claim[4],
                'birthDate': claim[5],
                'ageInYears': int(claim[6]),
                'sex': claim[7],
                'diagnosisList': [
                    {'code': claim[i]} | (
                        {'poa': claim[i + 50]} if claim[i + 50] else dict()
                    )
                    for i in range(9, 59)
                    if claim[i]
                ],
                'icdVersionQualifier': claim[160][0]
            } | (
                {'admitDiagnosis': claim[8]} if claim[8] else dict()
            ) | (
                {
                    'procedureList': [
                        {'code': claim[i]}
                        for i in range(109, 159)
                        if claim[i]
                    ]
                } if claim[109] else dict()
            )
            for claim_str in claim_lines
            for claim in [claim_str.split(',')]
        ]
    return result

def get_gpcs_result(
    access_token: str,
    claims_list: List[dict],
    content_version: str = '2022.2.1',
    grouper_type: str = 'APR',
    grouper_version: str = '390',
    sub_requests: int = 1,
    claims_per_sub_request: Optional[int] = None,
    timeout_seconds: int = 60
) -> dict:
    """Gets claim grouping results from the 3M gpcs remote API.

    Retrieves 3M gpcs claim grouping results for a a list of claims.
    The function needs to be able to contact https://gpcs.3m.com/

    Args:
        access_token: An Oauth access token valid for accessing the gpcs URL.
        claims_list: A list of claims represented as info dictionaries.
        content_version: The version of the gpcs API to call.
        grouper_type: The type of grouper to use.
        grouper_version: The version of the grouper to use.
        sub_requests: The number of sub-requests to make within the request.
        claims_per_sub_request: The number of claims to put in each
          sub-request.  If this is not None it overrides the sub_requests
          argument.
        timeout_seconds: The number of seconds to wait before considering the
          gpcs request to have timed out.

    Returns:
        A string representation of the JSON response of the 3M gpcs grouping
        API for the claims.
    """
    #This block of code checks for errors in the input parameters.
    assert isinstance(access_token, str), 'access_token is not a string.'
    assert isinstance(claims_list, list), 'claims_list is not a list.'
    assert isinstance(content_version, str), 'content_version is not a string.'
    assert isinstance(grouper_type, str), 'grouper_type is not a string.'
    assert isinstance(grouper_version, str), 'grouper_version is not a string.'
    assert isinstance(sub_requests, int) and (sub_requests > 0), \
        'sub_requests is not an int or is less than 1.'
    assert isinstance(claims_per_sub_request, type(None)) or \
        (isinstance(claims_per_sub_request, int) and 
         (claims_per_sub_request > 0)), 'claims_per sub_request invalid value.'
    assert isinstance(timeout_seconds, int) and (timeout_seconds > 0), \
        'timeout_seconds is not an int or is less than 1.'
    if not claims_list:
        print('Empty claims_list, nothing to process.')
        return ''

    gpcs_claims_url: str = (
        'https://gpcs.3m.com/Gpcs/rest/' + content_version + '/claims/process'
    )
    headers: Dict[str, str] = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }
    claims_count: int = len(claims_list)
    claims_per_subrequest_count: int = (
        claims_per_sub_request if claims_per_sub_request else (
            int(math.ceil(float(claims_count) / float(sub_requests)))
        )
    )

    processing_options: Dict[str, str] = {
        'contentVersion': content_version,
        'grouperType': grouper_type,
        'grouperVersion': grouper_version
    }
    request_list: List[dict] = [
        {
            'claimInputList': claims_list[i:i + claims_per_subrequest_count],
            'processingOptions': processing_options
        }
        for i in range(0, claims_count, claims_per_subrequest_count)
    ]
    request_body: str = json.dumps({'requestList': request_list})

    gpcs_response = requests.post(
        gpcs_claims_url,
        data=request_body,
        headers=headers,
        timeout=timeout_seconds
    )
    if gpcs_response.status_code != 200:
        raise Exception(
            'Claims Processing Error',
            gpcs_response.status_code,
            gpcs_response.content
        )

    return gpcs_response.json()


def format_json_to_text(
    json_input: dict,
    output_format: str,
    output_file: Optional[str]
) -> Optional[List[str]]:
    lines: List[str] = list()
    if output_format == 'A':
        lines = [
            ','.join([
                claim_out['fields']['patientIdUsed'],
                claim_out['fields']['claimId'],
                claim_out['fields']['drg'],
                claim_out['fields']['returnCode'],
                claim_out['fields']['soi'],
                claim_out['fields']['rom']
            ])
            for resp in json_input['responseList']
            for claim_out in resp['claimOutputList']
        ]
    result: Optional[List[str]] = None if output_file else lines
    return result


def get_gpcs_result_for_file(
    input_file: str,
    input_format: str,
    output_format: str,
    cert_basedir: str,
    cert_basename: str,
    auth_duration_minutes: int = 5,
    content_version: str = '2022.2.1',
    grouper_type: str = 'APR',
    grouper_version: str = '390',
    sub_requests: int = 1,
    claims_per_sub_request: Optional[int] = None,
    timeout_seconds: int = 60
):
    auth_token: str = generate_gpcs_auth_token(
        cert_basedir=cert_basedir,
        cert_basename=cert_basename,
        duration_minutes=auth_duration_minutes
    )
    access_token: str = get_gpcs_oath_access_token(auth_token)
    claims_list: List[dict] = claims_file_to_list(input_file, input_format)
    result: dict = get_gpcs_result(
        access_token=access_token,
        claims_list=claims_list,
        content_version=content_version,
        grouper_type=grouper_type,
        grouper_version=grouper_version,
        sub_requests=sub_requests,
        claims_per_sub_request=claims_per_sub_request,
        timeout_seconds=timeout_seconds
    )
    return result
