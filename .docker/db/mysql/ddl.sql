CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    is_active BOOLEAN
);

LOAD DATA INFILE '/home/data/mock-user-data.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(user_id, first_name, last_name, email, gender, ip_address, @is_active)
SET is_active = CASE
                   WHEN LOWER(@is_active) = 'true' THEN 1
                   WHEN LOWER(@is_active) = 'false' THEN 0
                   ELSE NULL
                END;

CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    price DECIMAL(6,2),
    transaction_date DATE,
    payment_method VARCHAR(50),
    shipping_address VARCHAR(50),
    order_status VARCHAR(50),
    discount_code VARCHAR(50)
);

LOAD DATA INFILE '/home/data/mock-transaction-data.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
