# type: ignore
from schema import (
    schema_CBMT160_O2_ASY_BOM_V_practice, #changed
    schema_DATES_APPLICABILITY_practice #changed
  
)

dates_app_config = {
    "dates_applicability": {
        "schema": schema_DATES_APPLICABILITY_practice,
        "dataset": "vdm_data_loading",
        "write_disposition": "WRITE_APPEND",
    }
}

task_config = {
    "CBMT160_O2_ASY_BOM_V_practice": {
        "schema": schema_CBMT160_O2_ASY_BOM_V_practice,
        "dataset": "vdm_raw_data_practice", #changing
        "write_disposition": "WRITE_APPEND",
        "tests": {
            "no_nulls_CD_SFI_ASSEMBLY": {
                "test_type": "null_count_test",
                "tested_fields": "CD_SFI_ASSEMBLY"
            },
            "regex_CD_SFI_ASSEMBLY": {
                "test_type": "regex_test",
                "tested_fields": "CD_SFI_ASSEMBLY",
                "pattern": "[A-Z0-9]+"
            },
            "no_nulls_IN_INCORP_IN": {
                "test_type": "null_count_test",
                "tested_fields": "IN_INCORP_IN"
            },
            "regex_IN_INCORP_IN": {
                "test_type": "regex_test",
                "tested_fields": "IN_INCORP_IN",
                "pattern": "Y|N"
            },
            "no_nulls_DT_EFFECTIVE_IN": {
                "test_type": "null_count_test",
                "tested_fields": "DT_EFFECTIVE_IN"
            },
            "no_nulls_DT_EFFECTIVE_OUT": {
                "test_type": "null_count_test",
                "tested_fields": "DT_EFFECTIVE_OUT"
            },
            "no_nulls_NO_PART_BASE": {
                "test_type": "null_count_test",
                "tested_fields": "NO_PART_BASE"
            },
            "regex_NO_PART_BASE": {
                "test_type": "regex_test",
                "tested_fields": "NO_PART_BASE",
                "pattern": "[A-Z0-9]+"
            },
            "no_nulls_NO_PART_PREFIX": {
                "test_type": "null_count_test",
                "tested_fields": "NO_PART_PREFIX"
            },
            "regex_NO_PART_PREFIX": {
                "test_type": "regex_test",
                "tested_fields": "NO_PART_PREFIX",
                "pattern": "[A-Z0-9]+|^$"
            },
            "no_nulls_NO_PART_SUFFIX": {
                "test_type": "null_count_test",
                "tested_fields": "NO_PART_SUFFIX"
            },
            "regex_NO_PART_SUFFIX": {
                "test_type": "regex_test",
                "tested_fields": "NO_PART_PREFIX",
                "pattern": "[A-Z0-9]+|^$"
            },
            "no_nulls_CD_PLANT": {
                "test_type": "null_count_test",
                "tested_fields": "CD_PLANT"
            },
            "interval_thresholds": {
                "COUNT(*)": 1.05
            }
        }
    },
    
}