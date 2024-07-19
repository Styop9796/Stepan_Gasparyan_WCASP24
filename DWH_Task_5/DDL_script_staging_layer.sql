CREATE SCHEMA IF NOT EXISTS sa_offline_sales;

CREATE EXTENSION IF NOT EXISTS file_fdw;  -- Ensure file_fdw extension is installed

CREATE SERVER IF NOT EXISTS sa_offline_sales_server FOREIGN DATA WRAPPER file_fdw;

CREATE FOREIGN TABLE IF NOT EXISTS sa_offline_sales.ext_offline_sales (
  invoice_number VARCHAR(100),
  date VARCHAR(100),
  product_id VARCHAR(100),
  product_name VARCHAR(100),
  quantity VARCHAR(100),
  stock VARCHAR(100),
  price VARCHAR(100),
  cost VARCHAR(100),
  promo_type_1 VARCHAR(100),
  promo_bin_1 VARCHAR(100),
  promo_type_2 VARCHAR(100),
  product_length VARCHAR(100),
  product_depth VARCHAR(100),
  product_width VARCHAR(100),
  employee_id VARCHAR(100),
  hierarchy1_id VARCHAR(100),
  hierarchy2_id VARCHAR(100),
  sales_channel VARCHAR(100),
  store_id VARCHAR(100),
  store_name VARCHAR(100),
  storetype_id VARCHAR(100),
  storetype_name VARCHAR(100),
  store_size VARCHAR(100),
  city_id VARCHAR(100),
  city_name VARCHAR(100),
  customer_id VARCHAR(100),
  f_name VARCHAR(100),
  l_name VARCHAR(100),
  email VARCHAR(100),
  cust_phone VARCHAR(100),
  store_state VARCHAR(100),
  country_id VARCHAR(100),
  country_name VARCHAR(100),
  store_address VARCHAR(100),
  product_price VARCHAR(100),
  product_cost VARCHAR(100),
  product_stock VARCHAR(100)
)
SERVER pglog
OPTIONS ( filename '/home/styop/Desktop/EPAM_STAGE_2/offline.csv', format 'csv', header 'true' );

SELECT * FROM sa_offline_sales.ext_offline_sales
limit 1;


CREATE SCHEMA IF NOT EXISTS sa_online_sales;


-- Create a server for file_fdw
CREATE SERVER IF NOT EXISTS sa_online_sales_server
    FOREIGN DATA WRAPPER file_fdw;

-- Define the foreign table
CREATE FOREIGN TABLE IF NOT EXISTS sa_online_sales.ext_online_sales (
    invoice_number VARCHAR(100),
    date VARCHAR(100),
    store_id VARCHAR(100),
    shop_website VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(100),
    quantity VARCHAR(100),
    stock VARCHAR(100),
    price VARCHAR(100),
    cost VARCHAR(100),
    promo_type_1 VARCHAR(100),
    promo_bin_1 VARCHAR(100),
    product_length VARCHAR(100),
    product_depth VARCHAR(100),
    product_width VARCHAR(100),
    employee_id VARCHAR(100),
    employee_name VARCHAR(100),
    hierarchy1_id VARCHAR(100),
    hierarchy2_id VARCHAR(100),
    sales_channel VARCHAR(100),
    customer_id BIGINT,
    f_name VARCHAR(100),
    l_name VARCHAR(100),
    email VARCHAR(100),
    employee_last_name VARCHAR(100),
    employee_email VARCHAR(100),
    product_price VARCHAR(100),
    product_cost VARCHAR(100),
    product_stock VARCHAR(100)
)
SERVER sa_online_sales_server
OPTIONS ( filename '/home/styop/Desktop/EPAM_STAGE_2/online.csv', format 'csv', header 'true' );

SELECT * FROM sa_online_sales.ext_online_sales
LIMIT 10;





CREATE TABLE IF NOT EXISTS sa_offline_sales.src_offline_sales (
  invoice_number VARCHAR(100),
  date VARCHAR(100),
  product_id VARCHAR(100),
  product_name VARCHAR(100),
  quantity VARCHAR(100),
  stock VARCHAR(100),
  price VARCHAR(100),
  cost VARCHAR(100),
  promo_type_1 VARCHAR(100),
  promo_bin_1 VARCHAR(100),
  promo_type_2 VARCHAR(100),
  product_length VARCHAR(100),
  product_depth VARCHAR(100),
  product_width VARCHAR(100),
  employee_id VARCHAR(100),
  hierarchy1_id VARCHAR(100),
  hierarchy2_id VARCHAR(100),
  sales_channel VARCHAR(100),
  store_id VARCHAR(100),
  store_name VARCHAR(100),
  storetype_id VARCHAR(100),
  storetype_name VARCHAR(100),
  store_size VARCHAR(100),
  city_id VARCHAR(100),
  city_name VARCHAR(100),
  customer_id VARCHAR(100),
  f_name VARCHAR(100),
  l_name VARCHAR(100),
  email VARCHAR(100),
  cust_phone VARCHAR(100),
  store_state VARCHAR(100),
  country_id VARCHAR(100),
  country_name VARCHAR(100),
  store_address VARCHAR(100),
  product_price VARCHAR(100),
  product_cost VARCHAR(100),
  product_stock VARCHAR(100)
);





CREATE TABLE IF NOT EXISTS sa_online_sales.src_online_sales (
    invoice_number VARCHAR(100),
    date VARCHAR(100),
    store_id VARCHAR(100),
    shop_website VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(100),
    quantity VARCHAR(100),
    stock VARCHAR(100),
    price VARCHAR(100),
    cost VARCHAR(100),
    promo_type_1 VARCHAR(100),
    promo_bin_1 VARCHAR(100),
    product_length VARCHAR(100),
    product_depth VARCHAR(100),
    product_width VARCHAR(100),
    employee_id VARCHAR(100),
    employee_name VARCHAR(100),
    hierarchy1_id VARCHAR(100),
    hierarchy2_id VARCHAR(100),
    sales_channel VARCHAR(100),
    customer_id VARCHAR(100),
    f_name VARCHAR(100),
    l_name VARCHAR(100),
    email VARCHAR(100),
    employee_last_name VARCHAR(100),
    employee_email VARCHAR(100),
    product_price VARCHAR(100),
    product_cost VARCHAR(100),
    product_stock VARCHAR(100)
);






