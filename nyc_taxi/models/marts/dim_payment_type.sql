select
    payment_type,
    payment_type_desc,
    is_paid
from {{ ref('stg_payment_types') }}