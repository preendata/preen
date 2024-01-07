create table users (
	user_id int primary key,
	first_name varchar(50),
	last_name varchar(50),
	email varchar(50),
	gender varchar(50),
	ip_address varchar(20),
	is_active boolean
);

copy users from '/home/data/mock-user-data.csv' ( format csv, delimiter(',') );

create table transactions (
	transaction_id varchar(50) primary key,
	user_id int,
	product_id varchar(50),
	quantity int,
	price decimal(6,2),
	transaction_date date,
	payment_method varchar(11),
	shipping_address varchar(50),
	order_status varchar(10),
	discount_code varchar(8)
);
copy transactions from '/home/data/mock-transaction-data.csv' ( format csv, delimiter(',') );
