with all_users as (
	select * from pg1.public.users
	union all
	select * from pg2.public.users
	union all
	select * from pg3.public.users
), 
all_transactions as (
	select * from pg1.public.transactions
	union all
	select * from pg2.public.transactions
	union all
	select * from pg3.public.transactions
)

select
	count(*)
from
	all_users
left join
	all_transactions
on
	all_users.user_id = all_transactions.user_id;
