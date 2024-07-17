CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE TABLE IF NOT EXISTS BL_3NF.CE_COUNTRIES(
	country_id SERIAL PRIMARY KEY ,
	country_name VARCHAR(100) NOT NULL,
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS BL_3NF.CE_CITIES(
	city_id SERIAL PRIMARY KEY ,
	city_name VARCHAR(100) NOT NULL,
	country_id INT REFERENCES BL_3NF.CE_COUNTRIES(country_id),
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);



CREATE TABLE IF NOT EXISTS BL_3NF.CE_ADDRESSES(
	store_address_id SERIAL PRIMARY KEY ,
	store_address VARCHAR(100) NOT NULL,
	store_state VARCHAR(50) NOT NULL,
	city_id INT REFERENCES BL_3NF.CE_CITIES(city_id),
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORE_TYPES(
	storetype_id SERIAL PRIMARY KEY ,
	storetype_name VARCHAR(50) NOT NULL,
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);



CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORES(
	store_id SERIAL PRIMARY KEY ,
	store_name VARCHAR(100) NOT NULL,
	store_size INT NOT NULL,
	shop_website VARCHAR (100) NOT NULL,
	storetype_id INT REFERENCES BL_3NF.CE_STORE_TYPES(storetype_id),
	store_address_id INT REFERENCES BL_3NF.CE_ADDRESSES(store_address_id),
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);



CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMO_TYPE_1(
	promo_type1_id SERIAL PRIMARY KEY ,
	promo_type1 VARCHAR(100) NOT NULL,
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMO_BIN_1(
	promo_bin1_id SERIAL PRIMARY KEY ,
	promo_bin1 VARCHAR(100) NOT NULL,
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMO_TYPE_2(
	promo_bin2_id SERIAL PRIMARY KEY ,
	promo_bin2 VARCHAR(100) NOT NULL,
	insert_dt DATE NOT NULL,
	update_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);




CREATE TABLE IF NOT EXISTS BL_3NF.CE_EMPLOYEES_SCD(
	employee_id SERIAL PRIMARY KEY ,
	employee_name VARCHAR(50) NOT NULL,
	employee_last_name VARCHAR(50) NOT NULL,
	employee_email VARCHAR(100) NOT NULL,
	start_dt DATE NOT NULL,
	end_dt DATE NOT NULL,
	is_active VARCHAR(10) NOT NULL,
	insert_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);



CREATE TABLE IF NOT EXISTS BL_3NF.CE_PRODUCTS_SCD(
	product_id SERIAL PRIMARY KEY ,
	product_name VARCHAR(100) NOT NULL,
	product_length DECIMAL(5,2) NOT NULL,
	product_depth DECIMAL(5,2) NOT NULL,
	product_width DECIMAL(5,2) NOT NULL,
	product_price DECIMAL(10,2) NOT NULL,
	product_cost DECIMAL(10,2) NOT NULL,
	product_stock INT NOT NULL,
	hierarchy1_id VARCHAR(30) NOT NULL,
	hierarchy2_id VARCHAR(30) NOT NULL,
	start_dt DATE NOT NULL,
	end_dt DATE NOT NULL,
	is_active VARCHAR(10) NOT NULL,
	insert_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);


CREATE TABLE IF NOT EXISTS BL_3NF.CE_CUSTOMERS_SCD(
	customer_id SERIAL PRIMARY KEY ,
	f_name VARCHAR(50) NOT NULL,
	l_name VARCHAR(50) NOT NULL,
	email VARCHAR(100) NOT NULL,
	cust_phone VARCHAR(30) NOT NULL,
	start_dt DATE NOT NULL,
	end_dt DATE NOT NULL,
	is_active VARCHAR(10) NOT NULL,
	insert_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS BL_3NF.CE_SALES_SCD(
	sale_id SERIAL PRIMARY KEY,
	date DATE NOT NULL,
	product_id INT REFERENCES BL_3NF.CE_PRODUCTS_SCD(product_id),
	employee_id INT REFERENCES BL_3NF.CE_EMPLOYEES_SCD(employee_id),
	store_id INT REFERENCES BL_3NF.CE_STORES(store_id),
	customer_id INT REFERENCES BL_3NF.CE_CUSTOMERS_SCD(customer_id),
	quantity INT NOT NULL,
	stock INT NOT NULL,
	price DECIMAL(10,2) NOT NULL,
	cost DECIMAL(10,2) NOT NULL,
	sales_channel VARCHAR(50) NOT NULL,
	start_dt DATE NOT NULL,
	end_dt DATE NOT NULL,
	is_active VARCHAR(10) NOT NULL,
	insert_dt DATE NOT NULL,
	source_id VARCHAR(100) NOT NULL,
	source_entity VARCHAR(100) NOT NULL,
	source_system VARCHAR(100) NOT NULL
);



INSERT INTO BL_3NF.CE_COUNTRIES(country_id, country_name, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT country_id, country_name, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE , '-1', 'MANUAL', 'MANUAL')
) AS default_row(country_id, country_name, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_COUNTRIES 
    WHERE BL_3NF.CE_COUNTRIES.country_id = default_row.country_id
)
RETURNING *;



INSERT INTO BL_3NF.CE_CITIES(city_id, city_name, country_id, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT city_id, city_name, country_id, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', -1, '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(city_id, city_name, country_id, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_CITIES
    WHERE BL_3NF.CE_CITIES.city_id = default_row.city_id
)
RETURNING *;


INSERT INTO BL_3NF.CE_ADDRESSES(store_address_id, store_address, store_state, city_id, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT store_address_id, store_address, store_state, city_id, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', 'n.a.', -1, '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(store_address_id, store_address, store_state, city_id, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_ADDRESSES
    WHERE BL_3NF.CE_ADDRESSES.store_address_id = default_row.store_address_id
)
RETURNING *;



INSERT INTO BL_3NF.CE_STORE_TYPES(storetype_id, storetype_name, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT storetype_id, storetype_name, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(storetype_id, storetype_name, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_STORE_TYPES
    WHERE BL_3NF.CE_STORE_TYPES.storetype_id = default_row.storetype_id
)
RETURNING *;


INSERT INTO BL_3NF.CE_STORES(store_id, store_name, store_size, shop_website, storetype_id, store_address_id, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT store_id, store_name, store_size, shop_website, storetype_id, store_address_id, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', -1, 'n.a.', -1, -1, '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(store_id, store_name, store_size, shop_website, storetype_id, store_address_id, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_STORES
    WHERE BL_3NF.CE_STORES.store_id = default_row.store_id
)
RETURNING *;



INSERT INTO BL_3NF.CE_PROMO_TYPE_1(promo_type1_id, promo_type1, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT promo_type1_id, promo_type1, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(promo_type1_id, promo_type1, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_PROMO_TYPE_1
    WHERE BL_3NF.CE_PROMO_TYPE_1.promo_type1_id = default_row.promo_type1_id
)
RETURNING *;



INSERT INTO BL_3NF.CE_PROMO_BIN_1(promo_bin1_id, promo_bin1, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT promo_bin1_id, promo_bin1, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(promo_bin1_id, promo_bin1, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_PROMO_BIN_1
    WHERE BL_3NF.CE_PROMO_BIN_1.promo_bin1_id = default_row.promo_bin1_id
)
RETURNING *;


INSERT INTO BL_3NF.CE_PROMO_TYPE_2(promo_bin2_id, promo_bin2, insert_dt, update_dt, source_id, source_entity, source_system)
SELECT promo_bin2_id, promo_bin2, insert_dt, update_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(promo_bin2_id, promo_bin2, insert_dt, update_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_PROMO_TYPE_2
    WHERE BL_3NF.CE_PROMO_TYPE_2.promo_bin2_id = default_row.promo_bin2_id
)
RETURNING *;


INSERT INTO BL_3NF.CE_EMPLOYEES_SCD(employee_id, employee_name, employee_last_name, employee_email, start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system)
SELECT employee_id, employee_name, employee_last_name, employee_email, start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
FROM (
    VALUES
    (-1, 'n.a.', 'n.a.', 'n.a.', '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', '1900-01-01'::DATE, '-1', 'MANUAL', 'MANUAL')
) AS default_row(employee_id, employee_name, employee_last_name, employee_email, start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_EMPLOYEES_SCD
    WHERE BL_3NF.CE_EMPLOYEES_SCD.employee_id = default_row.employee_id
)
RETURNING *;


INSERT INTO BL_3NF.CE_PRODUCTS_SCD(
    product_id, product_name, product_length, product_depth, product_width,
    product_price, product_cost, product_stock, hierarchy1_id, hierarchy2_id,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
)
SELECT 
    product_id, product_name, product_length, product_depth, product_width,
    product_price, product_cost, product_stock, hierarchy1_id, hierarchy2_id,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
FROM (
    VALUES (
        -1, 'n.a.', -1.00, -1.00, -1.00,
        -1.00, -1.00, -1, 'n.a.', 'n.a.',
        '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', '1900-01-01'::DATE,
        '-1', 'MANUAL', 'MANUAL'
    )
) AS default_row(
    product_id, product_name, product_length, product_depth, product_width,
    product_price, product_cost, product_stock, hierarchy1_id, hierarchy2_id,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_PRODUCTS_SCD
    WHERE BL_3NF.CE_PRODUCTS_SCD.product_id = default_row.product_id
)
RETURNING *;


INSERT INTO BL_3NF.CE_CUSTOMERS_SCD(
    customer_id, f_name, l_name, email, cust_phone,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
)
SELECT 
    customer_id, f_name, l_name, email, cust_phone,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
FROM (
    VALUES (
        -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.',
        '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', '1900-01-01'::DATE,
        '-1', 'MANUAL', 'MANUAL'
    )
) AS default_row(
    customer_id, f_name, l_name, email, cust_phone,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_CUSTOMERS_SCD
    WHERE BL_3NF.CE_CUSTOMERS_SCD.customer_id = default_row.customer_id
)
RETURNING *;



INSERT INTO BL_3NF.CE_SALES_SCD(
    sale_id, date, product_id, employee_id, store_id, customer_id,
    quantity, stock, price, cost, sales_channel,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
)
SELECT 
    sale_id, date, product_id, employee_id, store_id, customer_id,
    quantity, stock, price, cost, sales_channel,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
FROM (
    VALUES (
        -1, '1900-01-01'::DATE, -1, -1, -1, -1,
        -1, -1, -1.00, -1.00, 'n.a.',
        '1900-01-01'::DATE, '1900-01-01'::DATE, 'n.a.', '1900-01-01'::DATE,
        '-1', 'MANUAL', 'MANUAL'
    )
) AS default_row(
    sale_id, date, product_id, employee_id, store_id, customer_id,
    quantity, stock, price, cost, sales_channel,
    start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system
)
WHERE NOT EXISTS (
    SELECT 1
    FROM BL_3NF.CE_SALES_SCD
    WHERE BL_3NF.CE_SALES_SCD.sale_id = default_row.sale_id
)
RETURNING *;
