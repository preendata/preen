create table users (
	user_id varchar(50) primary key,
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
	user_id varchar(50),
	product_id varchar(50),
	quantity int,
	price decimal(6,2),
	transaction_date date,
	payment_method varchar(50),
	shipping_address varchar(50),
	order_status varchar(50),
	discount_code varchar(50)
);
copy transactions from '/home/data/mock-transaction-data.csv' ( format csv, delimiter(',') );
