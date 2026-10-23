select payment_type_id, payment_type from {{ ref('payment_types') }}
