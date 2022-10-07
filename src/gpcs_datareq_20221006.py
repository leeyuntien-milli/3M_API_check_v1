import json
import requests
import xmltodict
from itertools import islice
from typing import Any, Dict, List, Optional, Set, Tuple
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StringType, StructField, StructType

# list of constants
CLAIM_URL_PRE_3M_GPCS: str = 'https://gpcs.3m.com/Gpcs/rest/'
CLAIM_URL_SUF_3M_GPCS: str = '/claims/process'
HTTP_APP_HEADER: str = 'application/json'
CLAIM_EXCEPTION_MSG: str = 'Claims Processing Error'
AUTH_CLAIM_PRE: str = 'Bearer '
HTTP_RES_SUCC_CODE: int = 200
CLAIM_HEADER_FIELD_3M_GPCS: Tuple[str, str, str] = ('Authorization', 'Content-Type', 'accept')
CLAIM_PROC_FIELD_3M_GPCS: Tuple[str, str, str, str] = ('contentVersion', 'grouperType', 'grouperVersion', 'disableHac')
CLAIM_REQ_FIELD_3M_GPCS: Tuple[str, str, str] = ('claimInputList', 'processingOptions', 'scheduleList')
CLAIM_REQ_BODY_FIELD_3M_GPCS: str = 'requestList'
HRT_CASEA_FORMAT_PATH: str = '/dbfs/tmp/py/hrt_casea_format_dict.xml'
HRT_CASEA_FORMAT_DICT_NAME: str = 'format_dict'
HRT_CASEA_CONVERT_FIELD: Tuple[str, str] = ('core_fields', 'date_convert_fields')
SCHEMA_SCH_FIELD: str = 'schedule_fields'
SCHEMA_INP_FIELD: str = 'input_fields'
RESPONSE_FIELD: str = 'response_fields'
USECASE_EXCEPTION_MSG: str = 'Unknown usecase'

def get_format_for_usecase(usecase: str,
                           diagnosis_code_count: Optional[int] = None,
                           procedure_count: Optional[int] = None,
                           item_count: Optional[int] = None) -> dict:
    if usecase == 'HRT_CaseA':
        format_dict: dict = xmltodict.parse(open(HRT_CASEA_FORMAT_PATH).read())
        for i in range(0, len(HRT_CASEA_CONVERT_FIELD)):
            format_dict[HRT_CASEA_FORMAT_DICT_NAME][HRT_CASEA_CONVERT_FIELD[i]] = set(map(lambda x: x.replace("'", ""), format_dict[HRT_CASEA_FORMAT_DICT_NAME][HRT_CASEA_CONVERT_FIELD[i]].split(", ")))
    elif usecase == 'Chicago_CaseA':
        pass
    # else # better to add an exception if usecase not recognized
    #   raise Exception(USECASE_EXCEPTION_MSG + ' ' + usecase)

    return format_dict['format_dict']

def get_schema_for_usecase(
    usecase: str,
    schedule_schema: bool = False
) -> Optional[StructType]:
    """This method returns the schema corresponding to either schedule or input fields.
    
    Input Arguments: usecase, string specifying which usecase
                     [optional] schedule_schema, default to False (input_fields) or True (schedule_fields)
    Returns: a schema of StructType with all specified StructFields
    """
    format_dict: Optional[dict] = get_format_for_usecase(usecase)
    schema: Optional[StructType] = None
    ffield: str = SCHEMA_SCH_FIELD if schedule_schema else SCHEMA_INP_FIELD
    if format_dict:
        schema = StructType([
            StructField(item[0], StringType(), eval(item[1]['nullable']))
            for item in (format_dict[ffield]).items()
        ])
    else: # better to put under format_dict
        #print('Unknown usecase.')
        raise Exception(USECASE_EXCEPTION_MSG)
    return schema

def get_dict_for_usecase(
    usecase: str
) -> Optional[dict]:
    """This method returns the format dictionary of a usecase.
    
    Input Arguments: usecase, string specifying which usecase
    Returns: a format dictionary of the corresponding usecase
    """
    format_dict: dict = xmltodict.parse(open(HRT_CASEA_FORMAT_PATH).read())
    for i in range(0, len(HRT_CASEA_CONVERT_FIELD)):
        format_dict[HRT_CASEA_FORMAT_DICT_NAME][HRT_CASEA_CONVERT_FIELD[i]] = set(map(lambda x: x.replace("'", ""), format_dict[HRT_CASEA_FORMAT_DICT_NAME][HRT_CASEA_CONVERT_FIELD[i]].split(", ")))
    return format_dict['format_dict']

def convert_format_to_csv(input_file: str,
                          output_file: str,
                          usecase: str,
                          lines_chunk: int = 16384) -> None:
    format_dict: Dict[str, Any] = get_format_for_usecase(usecase)
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
    #The below works, but should be refactored to be nicer.
    with open(input_file, 'r') as file_in:
        with open(output_file, 'w') as file_out:
            result: List[str] = []
            if date_convert_indices and format_dict['periods_are_nulls']:
                while True:
                    flines = islice(file_in, lines_chunk)
                    result = [
                        ','.join([
                            (
                                '-'.join([lline[-4:], lline[:2], lline[3:5]])
                                if ((i in date_convert_indices) and lline)
                                else lline
                            )
                            for i in range(0, blim)
                            for lline in [
                                lcheck if lcheck != '.' else ''
                                for lcheck in [
                                    line[bounds[i]:bounds[i+1]].strip()
                                ]
                            ]
                        ])
                        for line in flines
                    ]
                    if not result:
                        break
                    file_out.write('\n'.join(result) + '\n')
            elif date_convert_indices:
                while True:
                    flines = islice(file_in, lines_chunk)
                    result = [
                        ','.join([
                            (
                                '-'.join([lline[-4:], lline[:2], lline[3:5]])
                                if ((i in date_convert_indices) and lline)
                                else lline
                            )
                            for i in range(0, blim)
                            for lline in [line[bounds[i]:bounds[i+1]].strip()]
                        ])
                        for line in flines
                    ]
                    if not result:
                        break
                    file_out.write('\n'.join(result) + '\n')
            elif format_dict['periods_are_nulls']:
                while True:
                    flines = islice(file_in, lines_chunk)
                    result = [
                        ','.join([
                            lline
                            for i in range(0, blim)
                            for lline in [
                                lcheck if lcheck != '.' else ''
                                for lcheck in [
                                    line[bounds[i]:bounds[i+1]].strip()
                                ]
                            ]
                        ])
                        for line in flines
                    ]
                    if not result:
                        break
                    file_out.write('\n'.join(result) + '\n')
            else:
                while True:
                    flines = islice(file_in, lines_chunk)
                    result = [
                        ','.join([
                            lline
                            for i in range(0, blim)
                            for lline in [line[bounds[i]:bounds[i+1]].strip()]
                        ])
                        for line in flines
                    ]
                    if not result:
                        break
                    file_out.write('\n'.join(result) + '\n')
    return

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
    usecase: str,
    patient_id_column: str = 'patientId'
):
    fields: Dict[str, str] = {}
    diag_code_count: int = 0
    diag_poas: bool = False
    procedure_count: int = 0
    if usecase == 'HRT_CaseA':
        diag_code_count = 50
        diag_poas = True
        procedure_count = 50
        format_dict: dict = get_format_for_usecase(usecase)
        #Fix this
        fields = {
            key: format_dict['input_fields'][key]['type']
            for key in format_dict['input_fields'].keys()
            if key in format_dict['core_fields']
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
            FLOOR(""" + str(group_count) + """*RAND()) AS Group_Index
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

def create_grouped_dataframe(
    spark: SparkSession,
    input_table: str,
    group_count: int,
    usecase: str,
    patient_id_column: str = 'patientId'
) -> DataFrame:
    query: str = build_grouped_dataframe_query(
        table_name=input_table,
        group_count=group_count,
        usecase=usecase,
        patient_id_column=patient_id_column
    )
    df: DataFrame = spark.sql(query)
    return df

def grouped_claims_to_structured_list(
    row,
    usecase: str
) -> List[dict]:
    claim_input_list: List[dict] = []
    if usecase == 'HRT_CaseA':
        claim_input_list = [
            {
                'patientId': entry['patientId'],
                'claimId': entry['claimId'],
                'admitDate': entry['admitDate'],
                'dischargeDate': entry['dischargeDate'],
                'sex': entry['sex'],
                'icdVersionQualifier': entry['icdVersionQualifier']
            } | (
                {'birthDate': entry['birthDate']}
                if (entry['birthDate'] and
                    (len(entry['birthDate']) == 10)) else {}
            ) | (
                {'ageInYears': int(entry['ageInYears'])}
                if entry['ageInYears'] else {}
            ) | (
                {'admitDiagnosis': entry['admitDiagnosis']}
                if entry['admitDiagnosis'] else {}
            ) | (
                {'diagnosisList': [
                    {'code': code.diagnosisCode} | (
                        {'poa': code.diagnosisPOA}
                        if code.diagnosisPOA else {}
                    )
                    for code in entry['diagnosisCodes']
                ]}
                if entry['diagnosisCodes'] else {}
            ) | (
                {'procedureList': [
                    {'code': procedure}
                    for procedure in entry['procedures']
                ]}
                if entry['procedures'] else {}
            )
            for entry in row['Data']
        ]
    #yield [json.dumps({'claimInputList': claim_input_list})]
    return claim_input_list

def get_json_result_subset(
    json_input: dict,
    response_fields: Tuple[str, ...]
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = [
        {
            fname: fields.get(fname)
            for fname in response_fields
        }
        for resp in json_input['responseList']
        for claim_out in resp['claimOutputList']
        for fields in [claim_out['fields']]
    ]
    return result

def get_gpcs_result_for_partition(
    partition_data,
    access_token: str,
    usecase: str,
    content_version: str = '2022.2.1',
    grouper_type: str = 'APR',
    grouper_version: str = '390',
    disable_hac: Optional[str] = None,
    schedule_list: Optional[List[Dict[str, Any]]] = None,
    sub_requests: int = 1,
    claims_per_sub_request: Optional[int] = None,
    timeout_seconds: int = 60
):
    """Gets claim grouping results from the 3M gpcs remote API.

    Retrieves 3M gpcs claim grouping results for a list of claims.
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
    if (usecase == 'HRT_CaseA') and (not disable_hac):
        disable_hac = '1'

    gpcs_claims_url: str = CLAIM_URL_PRE_3M_GPCS + content_version + CLAIM_URL_SUF_3M_GPCS
    headers: dict = dict(zip(
        CLAIM_HEADER_FIELD_3M_GPCS,
        (AUTH_CLAIM_PRE + access_token, HTTP_APP_HEADER, HTTP_APP_HEADER)
    ))
    processing_options: dict = dict(zip(
        CLAIM_PROC_FIELD_3M_GPCS,
        (content_version, grouper_type, grouper_version, (disable_hac if disable_hac else {}))
    ))
    format_dict: Dict[str, Any] = get_format_for_usecase(usecase)
    response_field_names: Tuple[str, ...] = (
        tuple((format_dict[RESPONSE_FIELD]).keys())
    )
    for row in partition_data:
        claim_input_list: List[dict] = grouped_claims_to_structured_list(
            row,
            usecase
        )
        request_list: List[dict] = [dict(zip(
            CLAIM_REQ_FIELD_3M_GPCS,
            (claim_input_list, processing_options, schedule_list if schedule_list else {})
        ))]
        request_body: str = json.dumps({CLAIM_REQ_BODY_FIELD_3M_GPCS: request_list})
        gpcs_response = requests.post(
            gpcs_claims_url,
            data=request_body,
            headers=headers,
            timeout=timeout_seconds
        )
        if gpcs_response.status_code != HTTP_RES_SUCC_CODE:
            raise Exception(
                CLAIM_EXCEPTION_MSG,
                gpcs_response.status_code,
                gpcs_response.content
            )
        result = get_json_result_subset(
            gpcs_response.json(),
            response_field_names
        )
        #yield [json.dumps(gpcs_response.json())]
        yield [result]

def postprocess_grouped_result(
    result_dataframe: DataFrame,
    usecase: str
) -> DataFrame:
    format_dict: Dict[str, Any] = get_format_for_usecase(usecase)
    col_list: List[str] = [
        'col.' + key
        for key in format_dict[RESPONSE_FIELD].keys()
    ]
    df_final = (
        result_dataframe.select(explode(result_dataframe.Data))
    ).select(col_list)
    return df_final

def get_gpcs_result_for_grouped_data(
    spark: SparkSession,
    grouped_dataframe: DataFrame,
    access_token: str,
    usecase: str,
    new_partitions: Optional[int] = None
) -> DataFrame:
    df_in: DataFrame = (
        grouped_dataframe.repartition(new_partitions)
    ) if new_partitions else (
        grouped_dataframe
    )
    result_rdd: RDD = df_in.rdd.mapPartitions(
        lambda x: get_gpcs_result_for_partition(x, access_token, usecase)
    )
    result_df: DataFrame = spark.createDataFrame(result_rdd).toDF("Data")
    final_df: DataFrame = postprocess_grouped_result(result_df, usecase)
    return final_df
