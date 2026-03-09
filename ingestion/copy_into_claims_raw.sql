REMOVE @CLAIMS_STAGE/claims_master.txt;

COPY INTO CLAIMS_RAW
(
    claim_id,
    policy_number,
    customer_id,
    claim_payload,
    source_file_name
)
FROM (
    SELECT
        $1:claim_id::STRING,
        $1:policy_number::STRING,
        $1:customer_id::STRING,
        $1,
        METADATA$FILENAME
    FROM @CLAIMS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = JSON_CLAIM_FORMAT);

SELECT * FROM CLAIMS_RAW;