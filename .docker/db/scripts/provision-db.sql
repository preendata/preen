create table users (
	id int primary key,
	first_name varchar(50),
	last_name varchar(50),
	email varchar(50),
	gender varchar(50),
	ip_address varchar(20),
	is_active boolean
);

copy users from '/home/data/mock-user-data.csv' ( format csv, delimiter(',') );
