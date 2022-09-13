import base64
import datetime
import json
import math
from re import S
import uuid
from collections import OrderedDict
from itertools import islice
from typing import Any, Dict, List, Optional, Set, Tuple
import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StringType, StructField, StructType


def get_format_for_usecase(usecase: str,
                           diagnosis_code_count: Optional[int] = None,
                           procedure_count: Optional[int] = None,
                           item_count: Optional[int] = None) -> dict:
    format_dict: Dict[str, Any] = {}
    field_info: Tuple[str, str, str] = (
        'type',
        'nullable',
        'text_left_col'
    )
    sfield_info: Tuple[str, str, str] = (
        'type',
        'nullable',
        'schedule_part'
    )
    boolean_str: str = 'BOOLEAN'
    double_str: str = 'DOUBLE'
    int_str: str = 'INT'
    string_str: str = 'STRING'
    diagnosis_code_lim: int = 0
    diagnosis_width: int = 0
    diagnosis_start: int = 0
    if usecase == 'HRT_CaseA':
        diagnosis_code_count = (
            50 if not diagnosis_code_count else diagnosis_code_count
        )
        diagnosis_code_lim = diagnosis_code_count + 1
        diagnosis_width = 8
        diagnosis_start = 94
        diagnosis_poa_width: int = 1
        diagnosis_poa_start: int = (
            diagnosis_start + diagnosis_width*diagnosis_code_count
        )
        procedure_count = 50 if not procedure_count else procedure_count
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
                        (string_str, False, 0)
                    ))),
                    ('claimId', dict(zip(
                        field_info,
                        (string_str, False, 40)
                    ))),
                    ('admitDate', dict(zip(
                        field_info,
                        (string_str, False, 50)
                    ))),
                    ('dischargeDate', dict(zip(
                        field_info,
                        (string_str, False, 60)
                    ))),
                    ('dischargeStatus', dict(zip(
                        field_info,
                        (string_str, True, 70)
                    ))),
                    ('birthDate', dict(zip(
                        field_info,
                        (string_str, True, 72)
                    ))),
                    ('ageInYears', dict(zip(
                        field_info,
                        (int_str, True, 82)
                    ))),
                    ('sex', dict(zip(
                        field_info,
                        (string_str, False, 85)
                    ))),
                    ('admitDiagnosis', dict(zip(
                        field_info,
                        (string_str, True, 86)
                    )))
                ] + [
                    ('diagnosisCode' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            diagnosis_start + diagnosis_width*(i - 1)
                        )
                    )))
                    for i in range(1, diagnosis_code_lim)
                ] + [
                    ('diagnosisPOA' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            diagnosis_poa_start + diagnosis_poa_width*(i - 1)
                        )
                    )))
                    for i in range(1, diagnosis_code_lim)
                ] + [
                    ('procedure' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            procedure_start + procedure_width*(i - 1)
                        )
                    )))
                    for i in range(1, procedure_lim)
                ] + [
                    ('disableHac', dict(zip(
                        field_info,
                        (string_str, False, end_start)
                    ))),
                    ('icdVersionQualifier', dict(zip(
                        field_info,
                        (string_str, False, end_start + 1)
                    )))
                ]
            ),
            'diagnosis_code_count': diagnosis_code_count,
            'diagnosis_poas': True,
            'procedure_count': procedure_count,
            'item_count': 0,
            'core_fields': {
                'claimId', 'admitDate', 'dischargeDate', 'dischargeStatus',
                'birthDate', 'ageInYears', 'sex', 'admitDiagnosis',
                'icdVersionQualifier'
            },
            'date_convert_fields': {'admitDate', 'dischargeDate', 'birthDate'},
            'periods_are_nulls': True,
            'line_final_boundary': end_start + 2,
            'response_fields': OrderedDict([
                ('patientIdUsed', string_str),
                ('claimId', string_str),
                ('drg', string_str),
                ('drgDescription', string_str),
                ('returnCode', string_str),
                ('soi', string_str),
                ('soiDescription', string_str),
                ('rom', string_str),
                ('romDescription', string_str),
                ('admitDrg', string_str),
                ('admitDrgDescription', string_str),
                ('admitMdc', string_str),
                ('admitMdcDescription', string_str),
                ('admitMedicalSurgicalDrgFlag', string_str),
                ('admitReturnCode', string_str),
                ('admitRom', string_str),
                ('admitRomDescription', string_str),
                ('admitSoi', string_str),
                ('admitSoiDescription', string_str),
                ('icuIntensityMarker', string_str),
                ('los', int_str),
                ('mdc', string_str),
                ('mdcDescription', string_str),
                ('medicalSurgicalDrgFlag', string_str),
                ('prePostPaymentErrors', string_str),
                ('proceduresUsedCount', int_str),
                ('secondaryDiagnosesUsedCount', string_str)
            ])
        }
    elif usecase == 'Chicago_CaseA':
        common_str: str = 'common'
        grouper_eapgs_str: str = 'grouper_eapgs'
        reimbursement_wi_medicaid_eapgs_str: str = (
            'reimbursement_wi_medicaid_eapgs'
        )
        processing_options_str: str = 'processing_options'
        string_required_common: Dict[str, Any] = dict(zip(
            sfield_info,
            (string_str, True, common_str)
        ))
        string_nullable_reimbursement: Dict[str, Any] = dict(zip(
            sfield_info,
            (string_str, True, reimbursement_wi_medicaid_eapgs_str)
        ))
        double_nullable_reimbursement: Dict[str, Any] = dict(zip(
            sfield_info,
            (double_str, True, reimbursement_wi_medicaid_eapgs_str)
        ))
        string_nullable_grouper: Dict[str, Any] = dict(zip(
            sfield_info,
            (string_str, True, grouper_eapgs_str)
        ))
        string_nullable_options: Dict[str, Any] = dict(zip(
            sfield_info,
            (string_str, True, processing_options_str)
        ))
        string_nullable_unused: Dict[str, Any] = dict(zip(
            sfield_info,
            (string_str, True, None)
        ))
        diagnosis_code_count = (
            25 if not diagnosis_code_count else diagnosis_code_count
        )
        diagnosis_code_lim = diagnosis_code_count + 1
        diagnosis_width = 7
        item_count = 427 if not item_count else item_count
        item_lim: int = item_count + 1
        item_procedure_start: int = 213
        item_procedure_width: int = 6
        item_modifier_width: int = 2
        item_modifier1_start: int = (
            item_procedure_start + item_procedure_width*item_count
        )
        item_modifier2_start: int = (
            item_modifier1_start + item_modifier_width*item_count
        )
        item_modifier3_start: int = (
            item_modifier2_start + item_modifier_width*item_count
        )
        item_modifier4_start: int = (
            item_modifier3_start + item_modifier_width*item_count
        )
        item_revenue_code_start: int = (
            item_modifier4_start + item_modifier_width*item_count
        )
        item_revenue_code_width: int = 4
        item_units_of_service_start: int = (
            item_revenue_code_start + item_revenue_code_width*item_count
        )
        item_units_of_service_width: int = 8
        item_service_date_start: int = (
            item_units_of_service_start + item_units_of_service_width*item_count
        )
        item_service_date_width: int = 8
        item_ndc_start: int = (
            item_service_date_start + item_service_date_width*item_count
        )
        item_ndc_width: int = 11
        item_billing_note_start: int = (
            item_ndc_start + item_ndc_width*item_count
        )
        item_billing_note_width: int = 3
        format_dict = {
            'input_fields': OrderedDict(
                [
                    ('claimId', dict(zip(
                        field_info,
                        (string_str, False, 0)
                    ))),
                    ('admitDate', dict(zip(
                        field_info,
                        (string_str, False, 13)
                    ))),
                    ('dischargeDate', dict(zip(
                        field_info,
                        (string_str, False, 21)
                    ))),
                    ('sex', dict(zip(
                        field_info,
                        (string_str, False, 29)
                    ))),
                    ('birthDate', dict(zip(
                        field_info,
                        (string_str, True, 30)
                    )))
                ] + [
                    ('diagnosisCode' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            diagnosis_start + diagnosis_width*(i - 1)
                        )
                    )))
                    for i in range(1, diagnosis_code_lim)
                ] + [
                    ('itemProcedure' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            item_procedure_start + item_procedure_width*(i - 1)
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemModifier1_' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            item_modifier1_start + item_modifier_width*(i - 1)
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemModifier2_' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            item_modifier2_start + item_modifier_width*(i - 1)
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemModifier3_' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            item_modifier3_start + item_modifier_width*(i - 1)
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemModifier4_' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            item_modifier4_start + item_modifier_width*(i - 1)
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemRevenueCode' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            (item_revenue_code_start +
                             item_revenue_code_width*(i - 1))
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemUnitsOfService' + str(i), dict(zip(
                        field_info,
                        (
                            double_str,
                            True,
                            (item_units_of_service_start +
                             item_units_of_service_width*(i - 1))
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemServiceDate' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            (item_service_date_start +
                             item_service_date_width*(i - 1))
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemNdc' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            (item_ndc_start +
                             item_ndc_width*(i - 1))
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('itemBillingNote' + str(i), dict(zip(
                        field_info,
                        (
                            string_str,
                            True,
                            (item_billing_note_start +
                             item_billing_note_width*(i - 1))
                        )
                    )))
                    for i in range(1, item_lim)
                ] + [
                    ('patientId', dict(zip(
                        field_info,
                        (string_str, False, 20709)
                    ))),
                    ('typeOfBill', dict(zip(
                        field_info,
                        (string_str, True, 20721)
                    ))),
                    ('reasonForVisitDiagnosis', dict(zip(
                        field_info,
                        (string_str, True, 20725)
                    ))),
                    ('itemPlaceOfService', dict(zip(
                        field_info,
                        (string_str, True, 20732)
                    ))),
                    ('userKey2', dict(zip(
                        field_info,
                        (string_str, True, 20734)
                    ))),
                    ('userKey1', dict(zip(
                        field_info,
                        (string_str, True, 20745)
                    ))),
                    ('icdVersionQualifier', dict(zip(
                        field_info,
                        (string_str, False, 20759)
                    )))
                ]
            ),
            'schedule_format_dict': OrderedDict([
                ('userKey1', string_nullable_unused),
                ('userKey2', string_nullable_unused),
                ('BEGIN_DATE', string_required_common),
                ('END_DATE', string_required_common),
                ('reimbursementScheme', string_nullable_unused),
                ('KEYED_BY', string_required_common),
                ('grouperVersion', dict(zip(
                    sfield_info,
                    string_nullable_options
                ))),
                ('DESCRIPTION', dict(zip(
                    sfield_info,
                    (string_str, True, common_str)
                ))),
                ('statsFeesOnly', string_nullable_unused),
                ('ppcVersion', dict(zip(
                    sfield_info,
                    string_nullable_options
                ))),
                ('modifiedDate', string_nullable_unused),
                ('medicalNecessityEditor', string_nullable_unused),
                ('medicalNecessityInsuranceId', string_nullable_unused),
                ('defaultsDate', string_nullable_unused),
                ('AUTO_DETERMINED_GROUPER_SETTING', dict(zip(
                    sfield_info,
                    (boolean_str, True, reimbursement_wi_medicaid_eapgs_str)
                ))),
                ('viewAutoDeterminedSettingForEffectiveDate',
                 string_nullable_unused),
                ('AUTO_DETERMINED_REIMBURSEMENT_SETTING', dict(zip(
                    sfield_info,
                    (boolean_str, True, reimbursement_wi_medicaid_eapgs_str)
                ))),
                ('EAPGSWIMCAID_BASE_RATE', double_nullable_reimbursement),
                ('EAPGSWIMCAID_STATISTICS', string_nullable_reimbursement),
                ('EAPGSWIMCAID_FEES', string_nullable_reimbursement),
                ('EAPGSWIMCAID_AGENCY_EFFECTIVE_DATE',
                 string_nullable_reimbursement),
                ('EAPGSWIMCAID_USER_DEFINED_ADJUSTMENT_FACTOR', 
                 double_nullable_reimbursement),
                ('EAPGSWIMCAID_ACCESS_PAYMENT', double_nullable_reimbursement),
                ('EAPGSWIMCAID_P4P_ADJUSTMENT', double_nullable_reimbursement),
                ('EAPGS_GRPR_VISITS_PER_CLAIM', string_nullable_grouper),
                ('EAPGS_GRPR_DIRECT_ADMIT_OBSERVATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_SAME_SIGNIFICANT_PROCEDURE_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_CLINICAL_SIGNIFICANT_PROCEDURE_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_MULTIPLE_SIGNIFICANT_PROCEDURE_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_REPEAT_ANCILLARY_PROCEDURE_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_BILATERAL_DISCOUNTING', string_nullable_grouper),
                ('EAPGS_GRPR_TERMINATED_PROCEDURE_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_IGNORE_ALL_MODIFIERS', string_nullable_grouper),
                ('EAPGS_GRPR_USE_MODIFIER_25', string_nullable_grouper),
                ('EAPGS_GRPR_USE_MODIFIER_27', string_nullable_grouper),
                ('EAPGS_GRPR_USE_MODIFIER_59', string_nullable_grouper),
                ('EAPGS_GRPR_USE_THERAPY_MODIFIERS', string_nullable_grouper),
                ('EAPGS_GRPR_PER_DIEM_INDIRECT_OPTION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_PER_DIEM_DIRECT_OPTION', string_nullable_grouper),
                ('EAPGS_GRPR_ADDITIONAL_INPATIENT_CODES',
                 string_nullable_grouper),
                ('inpatientTo994', string_nullable_unused),
                ('EAPGS_GRPR_NEVER_PAY_CODES', string_nullable_grouper),
                ('EAPGS_GRPR_NEVER_PAY_EAPGS', string_nullable_grouper),
                (('EAPGS_GRPR_SAME_SIGNIFICANT_PROCEDURE'
                  '_CONSOLIDATION_EXCLUSION_EAPGS'),
                 string_nullable_grouper),
                ('EAPGS_GRPR_ADD_PACKAGED_EAPGS', string_nullable_grouper),
                ('EAPGS_GRPR_DELETE_PACKAGED_EAPGS', string_nullable_grouper),
                ('payerExceptions', string_nullable_unused),
                ('EAPGS_GRPR_ALLOW_MEDICAL_VISIT_WITH_SPT_EAPGS',
                 string_nullable_grouper),
                ('conditionalOnDiagnosis', string_nullable_unused),
                ('EAPGS_GRPR_USE_ANATOMICAL_MODIFIERS',
                 string_nullable_grouper),
                ('EAPGS_GRPR_REPEAT_ANCILLARY_DISCOUNTING_FOR_DRUG',
                 string_nullable_grouper),
                ('EAPGS_GRPR_REPEAT_ANCILLARY_DISCOUNTING_FOR_DME',
                 string_nullable_grouper),
                ('EAPGS_GRPR_SINGLE_VISIT_PER_CLAIM_REVENUE_CODES',
                 string_nullable_grouper),
                (('EAPGS_GRPR_SAME_PHYSICAL_THERAPY_'
                  'REHABILITATION_CONSOLIDATION'),
                 string_nullable_grouper),
                ('EAPGS_GRPR_SAME_MENTAL_HEALTH_COUNSELING_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_SAME_DENTAL_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_SAME_RADIOLOGIC_PROCEDURE_CONSOLIDATION',
                 string_nullable_grouper),
                ('diagnosticOrTherapeuticSignificantProcedure',
                 string_nullable_unused),
                (('EAPGS_GRPR_CLINICAL_PHYSICAL_'
                  'THERAPY_REHABILITATION_CONSOLIDATION'),
                 string_nullable_grouper),
                ('EAPGS_GRPR_CLINICAL_MENTAL_HEALTH_COUNSELING_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_CLINICAL_DENTAL_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_CLINICAL_RADIOLOGIC_PROCEDURE_CONSOLIDATION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_CLINICAL_OTHER_DIAGNOSTIC_PROCEDURE_CONSOLIDATION',
                 string_nullable_grouper),
                (('EAPGS_GRPR_MULTIPLE_PHYSICAL_THERAPY_'
                  'REHABILITATION_DISCOUNTING'),
                 string_nullable_grouper),
                ('EAPGS_GRPR_MULTIPLE_MENTAL_HEALTH_COUNSELING_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_MULTIPLE_DENTAL_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_MULTIPLE_RADIOLOGIC_PROCEDURE_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_MULTIPLE_OTHER_DIAGNOSTIC_PROCEDURE_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_PROCESS_MEDICAL_VISIT_WITH_SIGNIFICANT_PROCEDURE',
                 string_nullable_grouper),
                (('EAPGS_GRPR_PROCESS_MEDICAL_VISIT_WITH_PHYSICAL_'
                  'THERAPY_REHABILITATION_PROCEDURE'),
                 string_nullable_grouper),
                (('EAPGS_GRPR_PROCESS_MEDICAL_VISIT_WITH_'
                  'MENTAL_HEALTH_COUNSELING_PROCEDURE'),
                 string_nullable_grouper),
                ('EAPGS_GRPR_PROCESS_MEDICAL_VISIT_WITH_DENTAL_PROCEDURE',
                 string_nullable_grouper),
                ('EAPGS_GRPR_PROCESS_MEDICAL_VISIT_WITH_RADIOLOGIC_PROCEDURE',
                 string_nullable_grouper),
                (('EAPGS_GRPR_PROCESS_MEDICAL_VISIT_'
                  'WITH_OTHER_DIAGNOSTIC_PROCEDURE'),
                 string_nullable_grouper),
                ('EAPGS_GRPR_ACUITY_EAPGS', string_nullable_grouper),
                ('EAPGS_GRPR_ACUITY_SECONDARY_DIAGNOSIS_CODES',
                 string_nullable_grouper),
                ('EAPGS_GRPR_USE_NEVER_EVENT_MODIFIERS_PA_PB_PC',
                 string_nullable_grouper),
                ('EAPGS_GRPR_OBSERVATION_HOURS_OPTION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_CROSS_TYPE_MULTIPLE_DISCOUNTING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_RADIOLOGY_PROCEDURE_PACKAGING',
                 string_nullable_grouper),
                ('EAPGS_GRPR_USER_DEFINED_340B_DRUG_LIST',
                 string_nullable_grouper),
                ('EAPGS_GRPR_USE_MODIFIER_57', string_nullable_grouper),
                ('EAPGS_GRPR_USE_DISTINCT_PROCEDURE_MODIFIERS',
                 string_nullable_grouper),
                ('EAPGS_GRPR_CONDITIONAL_EAPG_DIAGNOSIS_REQUIRED_LIST',
                 string_nullable_grouper),
                ('EAPGS_GRPR_MODIFIER_JW_OPTION', string_nullable_grouper),
                ('EAPGS_GRPR_MULTIPLE_MEDICAL_VISIT_OPTION',
                 string_nullable_grouper),
                ('EAPGS_GRPR_PER_DIEM_DIRECT_OPTION', string_nullable_grouper),
                ('EAPGS_GRPR_PER_DIEM_INDIRECT_OPTION',
                 string_nullable_grouper),
                ('modifierDistinctMedicalVisit', string_nullable_unused),
                ('productVersion', string_nullable_unused)
            ]),
            'diagnosis_code_count': diagnosis_code_count,
            'diagnosis_poas': False,
            'procedure_count': 0,
            'item_count': item_count,
            'core_fields': {
                'claimId', 'admitDate', 'dischargeDate', 'sex', 'birthDate',
                'typeOfBill', 'reasonForVisitDiagnosis', 'itemPlaceOfService',
                'icdVersionQualifier'
            },
            'date_convert_fields': {'admitDate', 'dischargeDate', 'birthDate'},
            'periods_are_nulls': True,
            'line_final_boundary': 20760
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
                            ('-'.join([lline[-4:], lline[:2], lline[3:5]])
                            if i in date_convert_indices else lline)
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
                            ('-'.join([lline[-4:], lline[:2], lline[3:5]])
                            if i in date_convert_indices else lline)
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

def generate_gpcs_auth_token(
    public_key: bytes,
    private_key: bytes,
    duration_minutes: int
) -> str:
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
    auth_token: str = jwt.encode(
        payload,
        private_key,
        algorithm='RS256',
        headers=headers
    )

    return auth_token

def generate_gpcs_auth_token_from_cert_files(
    cert_basedir: str,
    cert_basename: str,
    duration_minutes: int
) -> str:
    cert_prefix: str = cert_basedir + '/' + cert_basename
    private_key: bytes = bytes()
    public_key: bytes = bytes()
    with open(cert_prefix + '_key.pem', mode='rb') as private_key_file:
        private_key = private_key_file.read()
    with open(cert_prefix + '_cert.pem', mode='rb') as public_key_file:
        public_key = public_key_file.read()

    auth_token: str = generate_gpcs_auth_token(
        public_key,
        private_key,
        duration_minutes
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
    result: List[dict] = [{}]
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
        {'disableHac': disable_hac} if disable_hac else {}
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
    auth_token: str = generate_gpcs_auth_token_from_cert_files(
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
    disable_hac: Optional[str] = '1',
    sub_requests: int = 1,
    claims_per_sub_request: Optional[int] = None,
    timeout_seconds: int = 60
):
    gpcs_claims_url: str = (
        'https://gpcs.3m.com/Gpcs/rest/' + content_version + '/claims/process'
    )
    headers: Dict[str, str] = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }
    processing_options: Dict[str, str] = {
        'contentVersion': content_version,
        'grouperType': grouper_type,
        'grouperVersion': grouper_version
    } | (
        {'disableHac': disable_hac} if disable_hac else {}
    )
    format_dict: Dict[str, Any] = get_format_for_usecase(usecase)
    response_field_names: Tuple[str, ...] = (
        tuple((format_dict['response_fields']).keys())
    )
    for row in partition_data:
        claim_input_list: List[dict] = grouped_claims_to_structured_list(
            row,
            usecase
        )
        request_list: List[dict] = [
            {
                'claimInputList': claim_input_list,
                'processingOptions': processing_options
            }
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
        for key in format_dict['response_fields'].keys()
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



