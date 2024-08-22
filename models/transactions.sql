select 
    transactions.transaction_id,
    transactions.user_id,
    transactions.product_id,
    transactions.quantity,
    transactions.price,
    transactions.transaction_date,
    transactions.payment_method,
    transactions.shipping_address,
    transactions.order_status,
    transactions.discount_code,
    t1.transaction_id as transaction_id1,
    t2.transaction_id as transaction_id2
from
    transactions
inner join
    transactions as t1
on 
    transactions.transaction_id = t1.transaction_id
inner join
    transactions as t2
on
    transactions.transaction_id = t2.transaction_id
where
    transactions.price < 100;