select 
    t.transaction_id,
    t.user_id,
    t.product_id,
    t.quantity,
    t.price,
    t.transaction_date,
    t.payment_method,
    t.shipping_address,
    t.order_status,
    t.discount_code
from
    transactions as t;