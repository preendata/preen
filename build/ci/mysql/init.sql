create table test_data_types ( 
    mysql_id int auto_increment primary key,
    mysql_field_tinyint tinyint,
    mysql_field_smallint smallint,
    mysql_field_mediumint mediumint,
    mysql_field_int int,
    mysql_field_bigint bigint,
    mysql_field_decimal decimal(10, 2),
    mysql_field_float float,
    mysql_field_double double,
    mysql_field_char char(10),
    mysql_field_varchar varchar(255),
    mysql_field_tinytext tinytext,
    mysql_field_text text,
    mysql_field_mediumtext mediumtext,
    mysql_field_longtext longtext,
    mysql_field_boolean boolean,
    mysql_field_date date,
    mysql_field_time time,
    mysql_field_datetime datetime,
    mysql_field_timestamp timestamp,
    mysql_field_year year,
    mysql_field_binary binary(50),
    mysql_field_varbinary varbinary(255),
    mysql_field_tinyblob tinyblob,
    mysql_field_blob blob,
    mysql_field_mediumblob mediumblob,
    mysql_field_longblob longblob,
    mysql_field_enum enum('small', 'medium', 'large'),
    mysql_field_set set('option1', 'option2', 'option3'),
    mysql_field_json json,
    mysql_field_generated bigint generated always as (mysql_field_int * 2) stored
) engine = innodb row_format = default;

insert into test_data_types (
    mysql_field_tinyint, mysql_field_smallint, mysql_field_mediumint, mysql_field_int, mysql_field_bigint, mysql_field_decimal, mysql_field_float, mysql_field_double,
    mysql_field_char, mysql_field_varchar, mysql_field_tinytext, mysql_field_text, mysql_field_mediumtext, mysql_field_longtext,
    mysql_field_boolean,
    mysql_field_date, mysql_field_time, mysql_field_datetime, mysql_field_timestamp, mysql_field_year,
    mysql_field_binary, mysql_field_varbinary, mysql_field_tinyblob, mysql_field_blob, mysql_field_mediumblob, mysql_field_longblob,
    mysql_field_enum, mysql_field_set,
    mysql_field_json
) values (
    127, 32767, 8388607, 2147483647, 9223372036854775807, 1234.56, 3.14159, 2.71828,
    'CHAR(10)  ', 'VARCHAR(255)', 'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT',
    TRUE,
    '2024-09-12', '15:30:00', '2024-09-12 15:30:00', '2024-09-12 15:30:00', 2024,
    'BINARY', 'VARBINARY', 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB',
    'medium', 'option1,option3',
    '{"key": "value"}'
);
