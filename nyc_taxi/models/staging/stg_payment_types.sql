select
    cast(payment_type as integer) as payment_type,
    payment_type_desc,
    is_paid
from {{ ref('payment_types') }}