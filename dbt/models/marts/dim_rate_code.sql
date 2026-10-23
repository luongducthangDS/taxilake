select rate_code_id, rate_code from {{ ref('rate_codes') }}
