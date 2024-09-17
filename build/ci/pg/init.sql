create table test_data_types (
    pg_id serial primary key,
    pg_field_smallint smallint,
    pg_field_integer integer,
    pg_field_bigint bigint,
    pg_field_decimal decimal(10, 2),
    pg_field_numeric numeric(10, 2),
    pg_field_real real,
    pg_field_double double precision,
    pg_field_char char(10),
    pg_field_varchar varchar(255),
    pg_field_text text,
    pg_field_boolean boolean,
    pg_field_date date,
    pg_field_time time,
    pg_field_timestamp timestamp,
    pg_field_interval interval,
    pg_field_bytea bytea,
    pg_field_inet inet,
    pg_field_cidr cidr,
    pg_field_macaddr macaddr,
    pg_field_json json,
    pg_field_jsonb jsonb,
    pg_field_integer_array integer[],
    pg_field_text_array text[],
    pg_field_uuid uuid,
    pg_field_xml xml
);

insert into test_data_types (
    pg_field_smallint, pg_field_integer, pg_field_bigint, pg_field_decimal, pg_field_numeric, pg_field_real, pg_field_double,
    pg_field_char, pg_field_varchar, pg_field_text,
    pg_field_boolean,
    pg_field_date, pg_field_time, pg_field_timestamp, pg_field_interval,
    pg_field_bytea,
    pg_field_inet, pg_field_cidr, pg_field_macaddr,
    pg_field_json, pg_field_jsonb,
    pg_field_integer_array, pg_field_text_array,
    pg_field_uuid,
    pg_field_xml
) values (
    32767, 2147483647, 9223372036854775807, 1234.56, 9876.54, 3.14159, 2.71828,
    'CHAR(10)  ', 'VARCHAR(255)', 'This is a text field',
    TRUE,
    '2024-09-12', '15:30:00', '2024-09-12 15:30:00', '1 year 2 months 3 days 4 hours 5 minutes 6 seconds',
    E'\\xDEADBEEF',
    '192.168.1.1', '192.168.1.0/24', '08:00:2b:01:02:03',
    '{"key": "value"}', '{"key": "value"}',
    array[1, 2, 3], array['a', 'b', 'c'],
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '<root><element>XML data</element></root>'
);