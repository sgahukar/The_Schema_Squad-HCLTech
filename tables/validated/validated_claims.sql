CREATE OR REPLACE TABLE VALIDATED_CLAIMS AS
SELECT
    claim_id,
    policy_number,
    customer_id,

    CASE
        WHEN TRIM(UPPER(claim_payload:address.state::STRING)) IN ('UP','UTTAR PRADESH')
            THEN 'Uttar Pradesh'
        WHEN TRIM(UPPER(claim_payload:address.state::STRING)) = 'DELHI'
            THEN 'Delhi'
        WHEN TRIM(UPPER(claim_payload:address.state::STRING)) = 'MAHARASHTRA'
            THEN 'Maharashtra'
        WHEN TRIM(UPPER(claim_payload:address.state::STRING)) = 'RAJASTHAN'
            THEN 'Rajasthan'
        WHEN TRIM(UPPER(claim_payload:address.state::STRING)) = 'KARNATAKA'
            THEN 'Karnataka'
        ELSE INITCAP(TRIM(claim_payload:address.state::STRING))
    END AS state,

    TRIM(claim_payload:address.city::STRING) AS city,

    CASE
        WHEN LENGTH(REGEXP_REPLACE(claim_payload:address.zip::STRING,'[^0-9]','')) BETWEEN 5 AND 6
            THEN REGEXP_REPLACE(claim_payload:address.zip::STRING,'[^0-9]','')
        ELSE NULL
    END AS zip,

    claim_payload:claim_type::STRING AS claim_type,

    TRY_TO_DATE(claim_payload:incident_date::STRING) AS incident_date,
    TRY_TO_TIMESTAMP(claim_payload:fnol_datetime::STRING) AS fnol_datetime,

    claim_payload:status::STRING AS status,
    claim_payload:report_channel::STRING AS report_channel,

    TRY_TO_NUMBER(REPLACE(claim_payload:total_incurred::STRING, ',', '')) AS total_incurred,
    TRY_TO_NUMBER(REPLACE(claim_payload:total_paid::STRING, ',', '')) AS total_paid,

    CASE
        WHEN TRY_TO_NUMBER(REPLACE(claim_payload:total_paid::STRING, ',', ''))
           > TRY_TO_NUMBER(REPLACE(claim_payload:total_incurred::STRING, ',', ''))
        THEN 'PAID_GT_INCURRED'
        ELSE 'OK'
    END AS anomaly_flag,

    load_timestamp,
    source_file_name

FROM RAWSCHEMA.CLAIMS_RAW
WHERE policy_number IS NOT NULL
  AND customer_id IS NOT NULL;