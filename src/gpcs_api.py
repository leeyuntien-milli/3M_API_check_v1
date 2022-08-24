import base64
import datetime
import json
import math
import uuid
from collections import OrderedDict
from itertools import islice
from typing import Dict, List, Optional, Set, Tuple
import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from pyspark.sql.types import StringType, StructField, StructType


def get_format_for_usecase(usecase: str) -> Optional[dict]:
    format_dict: Optional[dict] = None
    field_info: Tuple[str, str, str] = (
        'type',
        'nullable',
        'text_left_col'
    )
    if usecase == 'HRT_CaseA':
        diagnosis_code_count: int = 50
        diagnosis_code_lim: int = diagnosis_code_count + 1
        diagnosis_width: int = 8
        diagnosis_start: int = 94
        diagnosis_poa_width: int = 1
        diagnosis_poa_start: int = (
            diagnosis_start + diagnosis_width*diagnosis_code_count
        )
        procedure_count: int = 50
        procedure_lim: int = procedure_count + 1
        procedure_width: int = 7
        procedure_start: int = (
            diagnosis_poa_start + diagnosis_poa_width*diagnosis_code_count
        )
        end_start: int = procedure_start + procedure_width*procedure_count
        format_dict = {
            'input_fields': OrderedDict(
                [
                    ('patientId', dict(zip(
                        field_info,
                        ('STRING', False, 0)
                    ))),
                    ('claimId', dict(zip(
                        field_info,
                        ('STRING', False, 40)
                    ))),
                    ('admitDate', dict(zip(
                        field_info,
                        ('STRING', True, 50)
                    ))),
                    ('dischargeDate', dict(zip(
                        field_info,
                        ('STRING', True, 60)
                    ))),
                    ('dischargeStatus', dict(zip(
                        field_info,
                        ('STRING', True, 70)
                    ))),
                    ('birthDate', dict(zip(
                        field_info,
                        ('STRING', True, 72)
                    ))),
                    ('ageInYears', dict(zip(
                        field_info,
                        ('INT', True, 82)
                    ))),
                    ('sex', dict(zip(
                        field_info,
                        ('STRING', True, 85)
                    ))),
                    ('admitDiagnosis', dict(zip(
                        field_info,
                        ('STRING', True, 86)
                    ))),
                    ('diagnosisCode1', dict(zip(
                        field_info,
                        ('STRING', False, diagnosis_start)
                    )))
                ] + [
                    ('diagnosisCode' + str(i), dict(zip(
                        field_info,
                        (
                            'STRING',
                            True,
                            diagnosis_start + diagnosis_width*(i - 1)
                        )
                    )))
                    for i in range(2, diagnosis_code_lim)
                ] + [
                    ('diagnosisPOA' + str(i), dict(zip(
                        field_info,
                        (
                            'STRING',
                            True,
                            diagnosis_poa_start + diagnosis_poa_width*(i - 1)
                        )
                    )))
                    for i in range(1, diagnosis_code_lim)
                ] + [
                    ('procedure' + str(i), dict(zip(
                        field_info,
                        (
                            'STRING',
                            True,
                            procedure_start + procedure_width*(i - 1)
                        )
                    )))
                    for i in range(1, procedure_lim)
                ] + [
                    ('disableHac', dict(zip(
                        field_info,
                        ('STRING', False, end_start)
                    ))),
                    ('icdVersionQualifier', dict(zip(
                        field_info,
                        ('STRING', False, end_start + 1)
                    )))
                ]
            ),
            'diagnosis_code_count': diagnosis_code_count,
            'diagnosis_poas': True,
            'procedure_count': procedure_count,
            'date_convert_fields': {'admitDate', 'dischargeDate', 'birthDate'},
            'line_final_boundary': end_start + 2
        }
    return format_dict

def get_schema_for_usecase(usecase: str) -> Optional[StructType]:
    format_dict: Optional[dict] = get_format_for_usecase(usecase)
    schema: Optional[StructType] = None
    if format_dict:
        schema = StructType([
            StructField(item[0], StringType(), item[1]['nullable'])
            for item in (format_dict['input_fields']).items()
        ])
    else:
        print('Unknown usecase.')
    return schema

def convert_format_to_csv(input_file: str,
                          output_file: str,
                          usecase: str,
                          lines_chunk: int = 1024) -> None:
    format_dict: Optional[dict] = get_format_for_usecase(usecase)
    if not format_dict:
        print('Unknown usecase.')
        return
    bounds: List[int] = [
        entry['text_left_col']
        for entry in format_dict['input_fields'].values()
    ] + [format_dict['line_final_boundary']]
    blim: int = len(bounds) - 1
    date_convert_indices: Set[int] = {
        i for i, key in enumerate(format_dict['input_fields'])
        if key in format_dict['date_convert_fields']
    }
    with open(input_file, 'r') as file_in:
        with open(output_file, 'w') as file_out:
            while True:
                flines = islice(file_in, lines_chunk)
                result: List[str] = [
                    ','.join([
                        ('-'.join([lline[-4:], lline[:2], lline[3:5]])
                        if i in date_convert_indices else lline)
                        for i in range(0, blim)
                        for lline in [line[bounds[i]:bounds[i+1]].strip()]
                    ])
                    for line in flines
                ]
                if not result:
                    break
                file_out.write('\n'.join(result))
    return

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
    disable_hac: Optional[str] = '1',
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
        return dict()

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
    } | (
        {'disableHac': disable_hac} if disable_hac else dict()
    )
    print(processing_options)
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


def json_subset_to_text(
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
    result: Optional[List[str]] = None
    if output_file:
        with open(output_file, 'w') as file_out:
            file_out.write('\n'.join(lines))
    else:
        result = lines
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

def build_struct_array_null_filter_subquery(
    field_names: List[str],
    field_count: int,
    filter_field_index: int = 0,
    indent: str = '',
    field_types: Optional[List[str]] = None
) -> str:
    fnames_count: int = len(field_names)
    assert (filter_field_index > -1) and (filter_field_index < fnames_count),\
        'filter_field_index out of range error.'
    if (field_count < 1) or (
        field_types and (len(field_types) != fnames_count)
    ):
        return ''
    struct_type: str = (
        'STRUCT<' + (
            ', '.join([
                field_names[i] + ': ' + field_types[i]
                for i in range(fnames_count)
            ])
            if field_types else
            ': STRING, '.join(field_names) + ': STRING'
         ) + '>'
    )
    delim: str = ',\n    ' + indent
    field_lim: int = field_count + 1
    query: str = (
        indent + 'FILTER(\n  ' + indent +
        'ARRAY(\n    ' + indent +
        delim.join([
            'CAST((' +
            ', '.join([
                fname + str(i)
                for fname in field_names
            ]) +
            ') AS ' + struct_type + ')'
            for i in range(1, field_lim)
        ]) +
        '\n  ' + indent + ')' +
        ',\n  ' + indent +
        'x -> x.' + field_names[filter_field_index] + ' IS NOT NULL' +
        '\n' + indent + ')'
    )
    return query

def build_array_null_filter_subquery(
    field_name: str,
    field_count: int,
    indent: str = '',
    cast_to: str = 'STRING',
) -> str:
    if field_count < 1:
        return ''
    delim: str = ',\n    ' + indent
    field_lim: int = field_count + 1
    query: str = (
        indent + 'FILTER(\n  ' + indent +
        'ARRAY(\n    ' + indent +
        delim.join([
            'CAST(' + field_name + str(i) + ' AS ' + cast_to + ')'
            for i in range(1, field_lim)
        ]) +
        '\n' + indent + '  ),\n' +
        indent + '  x -> x IS NOT NULL\n' +
        indent + ')'
    )
    return query

def build_collect_list_subquery(
    fields_dict: Dict[str, str],
    diag_code_count: int,
    diag_poas: bool,
    procedure_count: int,
    indent: str = '',
    include_patientId: bool = True
) -> str:
    delim1: str = ',\n  ' + indent
    subquery_indent: str = indent + '  '
    query: str = (
        indent + 'collect_list(named_struct(\n' +
        (indent + "  'patientId', CAST(t2.patientId AS STRING)" + delim1
         if include_patientId else '') +
        delim1.join([
            "'" + item[0] + "', CAST(" + item[0] + ' AS ' + item[1] +')'
            for item in fields_dict.items()
        ]) + (
            delim1 + "'diagnosisCodes'," + (
                (
                    build_struct_array_null_filter_subquery(
                        field_names=['diagnosisCode', 'diagnosisPOA'],
                        field_count=diag_code_count,
                        indent=subquery_indent
                    )
                ) if diag_poas else (
                    build_array_null_filter_subquery(
                        field_name='diagnosisCode',
                        field_count=diag_code_count,
                        indent=subquery_indent
                    )
                )
            )
            if diag_code_count > 0 else ''
        ) + (
            delim1 + "'procedures'," +
            build_array_null_filter_subquery(
                field_name='procedure',
                field_count=procedure_count,
                indent=subquery_indent
            )
            if procedure_count > 0 else ''
        ) +
        indent + '))'
    )
    return query

def build_grouped_dataframe_query(
    table_name: str,
    group_count: int,
    input_format: str = 'A',
    patient_id_column: str = 'patientId'
):
    fields: Dict[str, str] = dict()
    diag_code_count: int = 0
    diag_poas: bool = False
    procedure_count: int = 0
    if input_format == 'A':
        diag_code_count = 50
        diag_poas = True
        procedure_count = 50
        fields = {
            'claimId': 'STRING',
            'admitDate': 'STRING',
            'dischargeDate': 'STRING',
            'dischargeStatus': 'STRING',
            'birthDate': 'STRING',
            'ageInYears': 'INT',
            'sex': 'STRING',
            'admitDiagnosis': 'STRING'
        }
    collect_query: str = build_collect_list_subquery(
        fields_dict=fields,
        diag_code_count=diag_code_count,
        diag_poas=diag_poas,
        procedure_count=procedure_count
    )
    df_grouped_query = """
        SELECT
          Group_Index,\n""" + collect_query + """ AS Data
        FROM
          """ + table_name + """ t1
        INNER JOIN (
          SELECT
            patientId,
            ROUND(""" + str(group_count) + """*RAND()) AS Group_Index
          FROM (
            SELECT
                DISTINCT """ + patient_id_column + """ AS patientId
            FROM
                """ + table_name + """
          )
        ) t2
        ON t1.""" + patient_id_column + """ = t2.patientId
        GROUP BY
          Group_Index
    """
    return df_grouped_query

def grouped_data_to_json(partition_data):
    for row in partition_data:
        request_list: List[dict] = [
            {
                'patientId': entry['patientId'],
                'claimId': entry['claimId'],
                'admitDate': entry['admitDate'],
                'dischargeDate': entry['dischargeDate'],
                'birthDate': entry['birthDate'],
                'ageInYears': int(entry['ageInYears']),
                'sex': entry['sex'],
                #'icdVersionQualifier': entry['icdVersionQualifier']
                'diagnosisList': [
                {'code': code.diagnosisCode} | (
                    {'poa': code.diagnosisPOA}
                    if code.diagnosisPOA else dict()
                )
                for code in entry['diagnosisCodes']
                if code.diagnosisCode
                ]
            } | (
                {
                'procedureList': [
                    {'code': procedure}
                    for procedure in entry['procedures']
                ]
                }
                if entry['procedures'] else dict()
            ) | (
                {'admitDiagnosis': entry['admitDiagnosis']}
                if entry['admitDiagnosis'] else dict()
            )
            for entry in row['Data']
        ]
        yield [json.dumps({'requestList': request_list})]
