CREATE SCHEMA IF NOT EXISTS sa_offline_sales;
CREATE SCHEMA IF NOT EXISTS sa_online_sales;
CREATE EXTENSION IF NOT EXISTS file_fdw;  

CREATE OR REPLACE PROCEDURE public.create_foreign_tables_procedure()
LANGUAGE plpgsql
AS $$
BEGIN


DROP FOREIGN TABLE IF EXISTS sa_offline_sales.ext_offline_sales;

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
SERVER sa_offline_sales_server
OPTIONS ( filename '/home/styop/Desktop/EPAM_STAGE_2/1111updated_file.csv', format 'csv', header 'true' );


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


    

    RAISE NOTICE 'Foreign tables created' ;

EXCEPTION
    WHEN OTHERS THEN
   
        -- Raise the exception to propagate the error
        RAISE NOTICE 'Foreign tables are not created';
END;
$$;

--------------------

CALL public.create_foreign_tables_procedure();


CREATE OR REPLACE PROCEDURE public.create_src_tables_procedure()
LANGUAGE plpgsql
AS $$
BEGIN
	
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
  product_stock VARCHAR(100),
	is_processed VARCHAR(5)
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
    product_stock VARCHAR(100),
	is_processed VARCHAR(5)

);



    RAISE NOTICE 'Source tables are created' ;

EXCEPTION
    WHEN OTHERS THEN
   
        -- Raise the exception to propagate the error
        RAISE NOTICE 'Source tables are not created';
END;
$$;




CALL public.create_src_tables_procedure();


CREATE SCHEMA IF NOT EXISTS sa_offline_sales;
CREATE SCHEMA IF NOT EXISTS sa_online_sales;


--- LOGGIN TABLE 


CREATE TABLE IF NOT EXISTS  public.logging (
    log_id SERIAL PRIMARY KEY,
    log_datetime DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procedure_name VARCHAR(255) NOT NULL,
    rows_affected INT,
	message VARCHAR(255)
);




------------need to be fixed

CREATE OR REPLACE PROCEDURE sa_online_sales.insert_data_into_src_online_procedure()
LANGUAGE plpgsql
AS $$
DECLARE 
	inserted_count INTEGER;
BEGIN
	WITH inserted AS (INSERT INTO sa_online_sales.src_online_sales(
	invoice_number,employee_id, date, quantity, stock, price, 
	   cost, promo_type_1, promo_bin_1,  
	   product_id, product_name, product_length, 
	   product_depth, product_width, hierarchy1_id, 
	   hierarchy2_id, sales_channel, store_id,
	    customer_id, 
	   f_name, l_name, email,  
	     
	   product_price, product_cost, product_stock,is_processed)
SELECT invoice_number ,
	  employee_id,
	  date ,
	  quantity,
	  stock ,
	  price ,
	  cost ,
	  promo_type_1 ,
	  promo_bin_1 ,
	  product_id ,
	  product_name ,
	  product_length ,
	  product_depth ,
	  product_width ,
	  hierarchy1_id ,
	  hierarchy2_id ,
	  sales_channel ,
	  store_id ,
	  customer_id ,
	  f_name ,
	  l_name ,
	  email ,
	  product_price ,
	  product_cost ,
	  product_stock,
	  'N'
FROM sa_online_sales.ext_online_sales s
WHERE NOT EXISTS( SELECT 1 FROM sa_online_sales.src_online_sales src WHERE src.invoice_number = s.invoice_number) 
					  RETURNING 1)
					  
	SELECT COUNT(*) INTO inserted_count FROM inserted;
	
	INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_data_into_src_online_procedure', inserted_count);
	



    RAISE NOTICE 'Inserted rows into src_online_sales: %', inserted_count;
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_data_into_src_online_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


----------------------------------------------

CREATE OR REPLACE PROCEDURE sa_offline_sales.insert_data_into_src_offline_procedure()
LANGUAGE plpgsql
AS $$
DECLARE 
    inserted_count INTEGER;
BEGIN
    INSERT INTO sa_offline_sales.src_offline_sales(
       invoice_number, date, quantity, stock, price, 
       cost, promo_type_1, promo_bin_1, promo_type_2, 
       product_id, product_name, product_length, 
       product_depth, product_width,employee_id, hierarchy1_id, 
       hierarchy2_id, sales_channel, store_id,
       store_name, storetype_id, storetype_name, 
       store_size, city_id, city_name, customer_id, 
       f_name, l_name, email, cust_phone, store_state,
       country_id, country_name, store_address,
       product_price, product_cost, product_stock,is_processed)
    SELECT invoice_number ,
          date ,
          quantity,
          stock ,
          price ,
          cost ,
          promo_type_1 ,
          promo_bin_1 ,
          promo_type_2 ,
          product_id ,
          product_name ,
          product_length ,
          product_depth ,
          product_width ,
          employee_id::TEXT,
          hierarchy1_id ,
          hierarchy2_id ,
          sales_channel ,
          store_id ,
          store_name ,
          storetype_id ,
          storetype_name ,
          store_size ,
          city_id ,
          city_name ,
          customer_id ,
          f_name ,
          l_name ,
          email ,
          cust_phone ,
          store_state ,
          country_id ,
          country_name ,
          store_address ,
          product_price ,
          product_cost ,
          product_stock,
          'N'
    FROM sa_offline_sales.ext_offline_sales s
    WHERE NOT EXISTS( SELECT 1 FROM sa_offline_sales.src_offline_sales src WHERE src.invoice_number = s.invoice_number);
    
    GET DIAGNOSTICS inserted_count = ROW_COUNT;

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_data_into_src_offline_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows into src_offline_sales : %', inserted_count;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_data_into_src_offline_procedure', -1);

        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




CREATE INDEX IF NOT EXISTS inv_num_ind_for_offline ON sa_offline_sales.src_offline_sales(invoice_number);
CREATE INDEX IF NOT EXISTS inv_num_ind_for_online ON sa_online_sales.src_online_sales(invoice_number);



--CALL sa_online_sales.insert_data_into_src_online_procedure();
--CALL sa_offline_sales.insert_data_into_src_offline_procedure();


CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE OR REPLACE PROCEDURE public.create_3nf_and_tables_procedure()
LANGUAGE plpgsql
AS $$
BEGIN

		

		CREATE TABLE IF NOT EXISTS BL_3NF.CE_COUNTRIES(
			    country_id integer NOT NULL ,
			    country_name character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_countries_pkey PRIMARY KEY (country_id)
		);


		CREATE TABLE IF NOT EXISTS BL_3NF.CE_CITIES(
			    city_id integer NOT NULL,
			    city_name character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    country_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_cities_pkey PRIMARY KEY (city_id)
		);



		CREATE TABLE IF NOT EXISTS BL_3NF.CE_ADDRESSES(
			    store_address_id integer NOT NULL,
			    store_address character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    store_state character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    city_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_addresses_pkey PRIMARY KEY (store_address_id)
		);


		CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORE_TYPES(
			    store_type_id integer NOT NULL ,
			    store_type_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    update_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_store_types_pkey PRIMARY KEY (store_type_id)
		);



		CREATE TABLE IF NOT EXISTS BL_3NF.CE_STORES(
			    store_id integer NOT NULL,
			    store_name character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    store_size integer NOT NULL,
			    shop_website character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    store_type_id character varying(100) COLLATE pg_catalog."default",
			    store_address_id character varying(100) COLLATE pg_catalog."default",
			    insert_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_stores_pkey PRIMARY KEY (store_id)
		);



		CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMO_TYPE_1(
			    promo_type_1_id integer NOT NULL ,
			    promo_type_1 character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    update_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_promo_type_1_pkey PRIMARY KEY (promo_type_1_id)
		);

		CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMO_BIN_1(
			    promo_bin_1_id integer NOT NULL ,
			    promo_bin_1 character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    update_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_promo_bin_1_pkey PRIMARY KEY (promo_bin_1_id)
		);

		CREATE TABLE IF NOT EXISTS BL_3NF.CE_PROMO_TYPE_2(
			    promo_type_2_id integer NOT NULL ,
			    promo_type_2 character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    update_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_promo_type_2_pkey PRIMARY KEY (promo_type_2_id)
		);




		CREATE TABLE IF NOT EXISTS BL_3NF.CE_EMPLOYEES(
			    employee_id integer NOT NULL,
			    employee_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    employee_last_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    employee_email character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_employees_scd_pkey PRIMARY KEY (employee_id)
		);



		CREATE TABLE IF NOT EXISTS BL_3NF.CE_PRODUCTS(
			    product_id integer NOT NULL,
			    product_name character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    product_length numeric(5,2) NOT NULL,
			    product_depth numeric(5,2) NOT NULL,
			    product_width numeric(5,2) NOT NULL,
			    product_price numeric(10,2) NOT NULL,
			    product_cost numeric(10,2) NOT NULL,
			    product_stock integer NOT NULL,
			    hierarchy1_id character varying(30) COLLATE pg_catalog."default" NOT NULL,
			    hierarchy2_id character varying(30) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_products_scd_pkey PRIMARY KEY (product_id)
		);


		CREATE TABLE IF NOT EXISTS BL_3NF.CE_CUSTOMERS_SCD(
			    customer_id integer NOT NULL ,
			    f_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    l_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
			    email character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    cust_phone character varying(30) COLLATE pg_catalog."default" NOT NULL,
			    start_dt date NOT NULL,
			    end_dt date NOT NULL,
			    is_active character varying(10) COLLATE pg_catalog."default" NOT NULL,
			    insert_dt date NOT NULL,
			    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system character varying(100) COLLATE pg_catalog."default" NOT NULL,
			    CONSTRAINT ce_customers_scd_pkey PRIMARY KEY (customer_id)
		);

		CREATE TABLE IF NOT EXISTS BL_3NF.ce_sales (
			    sale_id INTEGER NOT NULL,
			    date DATE NOT NULL,
			    product_id CHARACTER VARYING(50) COLLATE pg_catalog."default" NOT NULL,
			    employee_id INTEGER,
			    store_id CHARACTER VARYING(50) COLLATE pg_catalog."default" NOT NULL,
			    customer_id INTEGER,
			    quantity INTEGER NOT NULL,
			    stock INTEGER NOT NULL,
			    price NUMERIC(10,2) NOT NULL,
			    cost NUMERIC(10,2) NOT NULL,
			    sales_channel CHARACTER VARYING(50) COLLATE pg_catalog."default" NOT NULL,
			    promo_type_1 CHARACTER VARYING(50) COLLATE pg_catalog."default",
			    promo_bin_1 CHARACTER VARYING(50) COLLATE pg_catalog."default",
			    promo_type_2 CHARACTER VARYING(50) COLLATE pg_catalog."default",
			    insert_dt DATE NOT NULL,
			    source_id CHARACTER VARYING(100) COLLATE pg_catalog."default" NOT NULL,
			    source_entity CHARACTER VARYING(100) COLLATE pg_catalog."default" NOT NULL,
			    source_system CHARACTER VARYING(100) COLLATE pg_catalog."default" NOT NULL
			)
			PARTITION BY RANGE (date);




    RAISE NOTICE '3NF tables are created' ;

EXCEPTION
    WHEN OTHERS THEN
   
        -- Raise the exception to propagate the error
        RAISE NOTICE '3NF tables are not created';
END;
$$;


---------------


CREATE OR REPLACE PROCEDURE BL_3NF.create_sequencies_for_3nf_tables_procedure()
LANGUAGE plpgsql
AS $$
BEGIN
	CREATE SEQUENCE IF NOT EXISTS bl_3nf.country_id_sec;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.city_id_sec;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.address_id_sec;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_store_types_store_type_id_seq;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.stores_id_sec;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_promo_type_1_promo_type_1_id_seq;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_promo_bin_1_promo_bin_1_id_seq;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_promo_type_2_promo_type_2_id_seq;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.ce_employees_employee_id_seq;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.products_id_sec;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.customer_id_sec;

	CREATE SEQUENCE IF NOT EXISTS bl_3nf.sales_id_sec;

    RAISE NOTICE '3NF sequencies are created' ;

EXCEPTION
    WHEN OTHERS THEN
   
        -- Raise the exception to propagate the error
        RAISE NOTICE '3NF sequencies are not created';
END;
$$;


----------------------------------------------------------


--procedure for partitioning 
CREATE OR REPLACE PROCEDURE BL_3NF.create_sales_partitions(start_date DATE, end_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    partition_start DATE := start_date;
    partition_end DATE;
BEGIN
    WHILE partition_start <= end_date LOOP
        -- Calculate the end date of the current partition
        partition_end := partition_start + INTERVAL '2 months';

        -- Define the partition name based on the start date
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS BL_3NF.ce_sales_%s_to_%s PARTITION OF BL_3NF.ce_sales FOR VALUES FROM (%L) TO (%L)',
            to_char(partition_start, 'YYYYMM'),
            to_char(partition_end, 'YYYYMM'),
            partition_start,
            partition_end
        );

        -- Move to the next 2-month range
        partition_start := partition_end;
    END LOOP;
END;
$$;



------------------------------------------


CREATE OR REPLACE PROCEDURE BL_3NF.create_partitions_on_sales()
LANGUAGE plpgsql
AS $$
DECLARE 
max_date DATE;
BEGIN
			SELECT max(date) FROM sa_offline_sales.src_offline_sales INTO max_date;
			CALL BL_3NF.create_sales_partitions('2022-01-01'::DATE, max_date);   
		
			RAISE NOTICE 'Partitions are created for fact sales';
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('create_partitions_on_sales', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;

-------






CREATE OR REPLACE PROCEDURE BL_3NF.insert_default_rows_procedure()
LANGUAGE plpgsql
AS $$
BEGIN


	-- Insert default row into CE_COUNTRIES
	INSERT INTO BL_3NF.CE_COUNTRIES (country_id, country_name, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_CITIES
	INSERT INTO BL_3NF.CE_CITIES (city_id, city_name, country_id, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_ADDRESSES
	INSERT INTO BL_3NF.CE_ADDRESSES (store_address_id, store_address, store_state, city_id, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', 'n.a.', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_STORE_TYPES
	INSERT INTO BL_3NF.CE_STORE_TYPES (store_type_id, store_type_name, insert_dt, update_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_STORES
	INSERT INTO BL_3NF.CE_STORES (store_id, store_name, store_size, shop_website, store_type_id, store_address_id, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', -1, 'n.a.', 'n.a.', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_PROMO_TYPE_1
	INSERT INTO BL_3NF.CE_PROMO_TYPE_1 (promo_type_1_id, promo_type_1, insert_dt, update_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_PROMO_BIN_1
	INSERT INTO BL_3NF.CE_PROMO_BIN_1 (promo_bin_1_id, promo_bin_1, insert_dt, update_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_PROMO_TYPE_2
	INSERT INTO BL_3NF.CE_PROMO_TYPE_2 (promo_type_2_id, promo_type_2, insert_dt, update_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_EMPLOYEES
	INSERT INTO BL_3NF.CE_EMPLOYEES (employee_id, employee_name, employee_last_name, employee_email, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', 'n.a.', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_PRODUCTS
	INSERT INTO BL_3NF.CE_PRODUCTS (product_id, product_name, product_length, product_depth, product_width, product_price, product_cost, product_stock, hierarchy1_id, hierarchy2_id, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', -1, -1, -1, -1, -1, -1, 'n.a.', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_CUSTOMERS_SCD
	INSERT INTO BL_3NF.CE_CUSTOMERS_SCD (customer_id, f_name, l_name, email, cust_phone, start_dt, end_dt, is_active, insert_dt, source_id, source_entity, source_system)
	VALUES (-1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', CURRENT_TIMESTAMP, '9999-12-31', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;

	-- Insert default row into CE_SALES
	INSERT INTO BL_3NF.CE_SALES (sale_id, date, product_id, employee_id, store_id, customer_id, quantity, stock, price, cost, sales_channel, promo_type_1, promo_bin_1, promo_type_2, insert_dt, source_id, source_entity, source_system)
SELECT -1, '2023-12-31', '-1', -1, '-1', -1, -1, -1, -1, -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM BL_3NF.CE_SALES WHERE sale_id = -1
);



	RAISE NOTICE 'Default rows are inserted' ;

EXCEPTION
    WHEN OTHERS THEN
   
        -- Raise the exception to propagate the error
        RAISE NOTICE 'Default rows are not inserted';
END;
$$;


CALL BL_3NF.insert_default_rows_procedure();
CALL public.create_3nf_and_tables_procedure();
CALL BL_3NF.create_sequencies_for_3nf_tables_procedure();
CALL BL_3NF.create_partitions_on_sales();







--- LOGGIN TABLE 


CREATE TABLE IF NOT EXISTS  public.logging (
    log_id SERIAL PRIMARY KEY,
    log_datetime DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procedure_name VARCHAR(255) NOT NULL,
    rows_affected INT,
	message VARCHAR(255)
);



---------------------------------------------------------


-- CE_COUNTRIES

CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_countries_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT  DISTINCT
            country_id, 
            country_name 
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_countries (
            country_id,
            country_name,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.country_id_sec'),
            dsr.country_name,
            current_timestamp,
            dsr.country_id,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_countries c
            WHERE c.source_id = dsr.country_id
              AND c.country_name = dsr.country_name
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

        RAISE NOTICE 'Inserted rows: % in ce_countries', inserted_count;

		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_countries_procedure', inserted_count,'success');
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO public.logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_countries', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




----------------------------------------------------

-- CE_CITIES 


CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_cities_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            city_id, 
            city_name,
            country_id
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_cities (
            city_id,
            city_name,
            country_id,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.city_id_sec'),
            dsr.city_name,
            (SELECT country_id FROM BL_3NF.ce_countries c WHERE c.source_id=dsr.country_id::TEXT),
            current_timestamp,
            dsr.city_id,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_cities c
            WHERE c.source_id = dsr.city_id
              AND c.city_name = dsr.city_name 
              AND c.country_id::TEXT = dsr.country_id::TEXT
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_cities', inserted_count;
	
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_cities_procedure', inserted_count,'success');
		
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_cities_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




------------------------------------------------------------------------


-- CE_ ADDRESSES

CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_addreses_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            store_address, 
            store_state,
            city_id
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_addresses (
            store_address_id,
			store_address,
			store_state,
            city_id,           
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.address_id_sec'),
            dsr.store_address,
            dsr.store_state,
			COALESCE((SELECT city_id FROM BL_3NF.ce_cities c WHERE c.source_id=dsr.city_id::TEXT),-1),
            current_timestamp,
            dsr.store_address,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_addresses c
            WHERE c.source_id = dsr.store_address
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_addreses', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_addreses_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_addreses_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;

-------------------------------------------------------------




-- CE_PRODUCTS

CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_products_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN


UPDATE sa_offline_sales.SRC_OFFLINE_SALES
SET product_length = -1
WHERE product_length = 'NULL';

UPDATE sa_offline_sales.SRC_OFFLINE_SALES
SET product_depth = -1 
WHERE product_depth = 'NULL';


UPDATE sa_offline_sales.SRC_OFFLINE_SALES
SET product_width = -1
WHERE product_width = 'NULL';

WITH RankedProducts AS (
    SELECT 
        product_id, 
        product_name,
        product_length,
        product_depth,
        product_width,
        product_price,
        product_cost,
        product_stock,
        hierarchy1_id,
        hierarchy2_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM sa_offline_sales.SRC_OFFLINE_SALES
),
		 distinct_source_rows AS (
			SELECT 
		product_id, 
		product_name,
		product_length,
		product_depth,
		product_width,
		product_price,
		product_cost,
		product_stock,
		hierarchy1_id,
		hierarchy2_id
	FROM RankedProducts
	WHERE rn = 1),
			
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_products (
            product_id,
			product_name,
			product_length,
			product_depth,
			product_width,
			product_price,
			product_cost,
			product_stock,
			hierarchy1_id,
			hierarchy2_id,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.products_id_sec'),
            dsr.product_name,
            COALESCE(dsr.product_length::NUMERIC,-1),
			COALESCE(dsr.product_depth::NUMERIC,-1),
			COALESCE(dsr.product_width::NUMERIC,-1),
			COALESCE(dsr.product_price::NUMERIC,-1),
			COALESCE(dsr.product_cost::NUMERIC,-1),
			COALESCE(dsr.product_stock::NUMERIC,-1),
			dsr.hierarchy1_id,
			dsr.hierarchy2_id,
            current_timestamp,
            dsr.product_id,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_products c
            WHERE c.source_id = dsr.product_id
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

	RAISE NOTICE 'Inserted rows: % in ce_products', inserted_count;
	
	
	INSERT INTO logging (procedure_name, rows_affected,message)
	VALUES ('insert_ce_products', inserted_count,'success');
		
		
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_products', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;
   

------------------------------------------------------



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_store_types_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            storetype_id, 
            storetype_name
            
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_store_types (
            store_type_id,
			store_type_name,
            insert_dt,
			update_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.ce_store_types_store_type_id_seq'),
            dsr.storetype_name,
            current_timestamp,
            current_timestamp,
			dsr.storetype_id,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_store_types c
            WHERE c.source_id = dsr.storetype_id
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_store_types', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_store_types_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_store_types_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


--------------------------------------------------------------------

-- CE_STORES 

CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_stores_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            store_id, 
            store_name,
			storetype_id,
			store_state,
			store_size,
			store_address,
			city_id
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_stores (
            store_id,
			store_name,
			store_size,
			shop_website,
            store_type_id,
			store_address_id,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.stores_id_sec'),
            dsr.store_name,
            dsr.store_size::INT,
			'n.a.',
			(SELECT store_type_id FROM BL_3NF.ce_store_types c WHERE c.source_id=dsr.storetype_id::TEXT),
			(SELECT store_address_id FROM BL_3NF.CE_ADDRESSES a 
			 LEFT JOIN  BL_3NF.CE_CITIES ci ON ci.city_id::TEXT = a.city_id::TEXT
			 WHERE a.source_id=dsr.store_address::TEXT AND a.store_state = dsr.store_state AND ci.source_id=dsr.city_id),
            current_timestamp,
            dsr.store_id,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_stores c
            WHERE c.source_id = dsr.store_id
        )
        RETURNING *
    )
        SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

		RAISE NOTICE 'Inserted rows: % in ce_stores', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_stores_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_stores_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


----------------------------------------------------------------------



-- CE_CUSTOMERS_SCD
	
	
CREATE OR REPLACE PROCEDURE BL_3NF.insert_or_update_ce_customers_scd_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INT := 0;
    updated_count INT := 0;
    v_record RECORD;
BEGIN
	CREATE INDEX IF NOT EXISTS idx_ce_customers_source_id ON BL_3NF.ce_customers_scd(source_id);
	CREATE INDEX IF NOT EXISTS idx_src_offline_sales_customer_id ON sa_offline_sales.SRC_OFFLINE_SALES(customer_id);

    FOR v_record IN (SELECT DISTINCT customer_id, f_name, l_name, email, cust_phone FROM sa_offline_sales.src_offline_sales)
    LOOP

        -- Check if the record exists in the target table
        IF EXISTS (
            SELECT 1
            FROM BL_3NF.ce_customers_scd cs
            WHERE cs.source_id::TEXT = v_record.customer_id::TEXT
        ) THEN

			IF NOT EXISTS (
                SELECT 1
                FROM BL_3NF.ce_customers_scd cs
                WHERE cs.source_id::INT = v_record.customer_id::INT AND 
                	cs.f_name = v_record.f_name AND 
                     cs.l_name = v_record.l_name AND
                     cs.email = v_record.email AND 
                     cs.cust_phone = v_record.cust_phone)
             THEN
                UPDATE BL_3NF.ce_customers_scd
                SET 
					end_dt = current_timestamp,
					is_active = 'N'
                WHERE source_id::INT = v_record.customer_id::INT AND is_active = 'Y';
				
				INSERT INTO BL_3NF.ce_customers_scd (customer_id, f_name, l_name, email, cust_phone, start_dt, end_dt, is_active, insert_dt,
                                                 source_id, source_entity, source_system)
				VALUES (nextval('bl_3nf.customer_id_sec'), v_record.f_name, v_record.l_name, v_record.email, v_record.cust_phone,
						current_timestamp, '9999-01-01', 'Y', current_timestamp, v_record.customer_id, 'SRC_OFFLINE_SALES', 'BL_CL');
				updated_count := updated_count + 1;
				
				
				
            END IF;
        ELSE

            -- Record does not exist, insert it
            INSERT INTO BL_3NF.ce_customers_scd (customer_id, f_name, l_name, email, cust_phone, start_dt, end_dt, is_active, insert_dt,
                                                 source_id, source_entity, source_system)
            VALUES (nextval('bl_3nf.customer_id_sec'), v_record.f_name, v_record.l_name, v_record.email, v_record.cust_phone,
                    current_timestamp, '9999-01-01', 'Y', current_timestamp, v_record.customer_id, 'SRC_OFFLINE_SALES', 'BL_CL');
            inserted_count := inserted_count + 1;
        END IF;
    END LOOP;

    	RAISE NOTICE 'Inserted rows: % in ce_customers_scd , updated %', inserted_count,updated_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_or_update_ce_customers_scd_procedure', inserted_count,updated_count);
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_or_update_ce_customers_scd_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


--------------------------------------------------


CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_employees_procedure_online()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            employee_id, 
            employee_name,
            employee_last_name,
			employee_email
        FROM sa_online_sales.SRC_ONLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_employees(
            employee_id,
			employee_name,
			employee_last_name,
            employee_email,           
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.ce_employees_employee_id_seq'),
            dsr.employee_name,
            dsr.employee_last_name,
			dsr.employee_email,
            current_timestamp,
            dsr.employee_id,
            'SRC_ONLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_employees c
            WHERE c.source_id = dsr.employee_id
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_employees', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_employees_procedure_online', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_employees_procedure_online', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




---------------------------------------------
--PRRMO TYPE 1

CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_promo_type_1_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            promo_type_1
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_promo_type_1(
            promo_type_1_id,
			promo_type_1,
			insert_dt,
			update_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.ce_promo_type_1_promo_type_1_id_seq'),
            dsr.promo_type_1,
			current_timestamp,
            current_timestamp,
            dsr.promo_type_1,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_promo_type_1 c
            WHERE c.source_id = dsr.promo_type_1
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_promo_type_1', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_promo_type_1_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_promo_type_1_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;

----------------------------------------------
-- PROMO_TYPE 2



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_promo_type_2_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            promo_type_2
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_promo_type_2(
            promo_type_2_id,
			promo_type_2,
			insert_dt,
			update_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.ce_promo_type_2_promo_type_2_id_seq'),
            dsr.promo_type_2,
			current_timestamp,
            current_timestamp,
            dsr.promo_type_2,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_promo_type_2 c
            WHERE c.source_id = dsr.promo_type_2
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_promo_type_2', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_promo_type_2_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_promo_type_2_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




---------------------------------------------------------
-- PROMO BIN 1 



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_promo_bin_1_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            promo_bin_1
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_promo_bin_1(
            promo_bin_1_id,
			promo_bin_1,
			insert_dt,
			update_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.ce_promo_bin_1_promo_bin_1_id_seq'),
            dsr.promo_bin_1,
			current_timestamp,
            current_timestamp,
            dsr.promo_bin_1,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_promo_bin_1 c
            WHERE c.source_id = dsr.promo_bin_1
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RAISE NOTICE 'Inserted rows: % in ce_promo_bin_1', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_promo_bin_1_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_promo_bin_1_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


--------------------------------------------------------------------

--CE_SALES TABLE 

--procedure for partitioning 
CREATE OR REPLACE PROCEDURE BL_3NF.create_sales_partitions(start_date DATE, end_date DATE)
LANGUAGE plpgsql AS $$
DECLARE
    partition_start DATE := start_date;
    partition_end DATE;
BEGIN
    WHILE partition_start <= end_date LOOP
        -- Calculate the end date of the current partition
        partition_end := partition_start + INTERVAL '2 months';

        -- Define the partition name based on the start date
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS BL_3NF.ce_sales_%s_to_%s PARTITION OF BL_3NF.ce_sales FOR VALUES FROM (%L) TO (%L)',
            to_char(partition_start, 'YYYYMM'),
            to_char(partition_end, 'YYYYMM'),
            partition_start,
            partition_end
        );

        -- Move to the next 2-month range
        partition_start := partition_end;
    END LOOP;
END;
$$;





CREATE OR REPLACE PROCEDURE BL_3NF.insert_fct_sales_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INT := 0;
    v_record RECORD;
	max_date DATE;
BEGIN
	CREATE INDEX IF NOT EXISTS idx_is_proc ON sa_offline_sales.src_offline_sales(is_processed);
	CREATE INDEX IF NOT EXISTS idx_ce_products_source_id ON BL_3NF.CE_PRODUCTS(source_id);
	CREATE INDEX IF NOT EXISTS idx_ce_employees_source_id ON BL_3NF.CE_EMPLOYEES(source_id);
	CREATE INDEX IF NOT EXISTS idx_ce_stores_source_id ON BL_3NF.CE_STORES(source_id);
	CREATE INDEX IF NOT EXISTS idx_ce_customers_scd_source_id_is_active
	ON BL_3NF.CE_CUSTOMERS_SCD(source_id, is_active);
		
	SELECT max(date) FROM sa_offline_sales.src_offline_sales INTO max_date;
	CALL create_sales_partitions('2022-01-01', max_date);

	FOR v_record IN (SELECT  
					 			invoice_number,
								date,
								product_id,
								employee_id,
								store_id,
								customer_id,
								quantity,
								stock,
								price,
								cost,
								promo_type_1,
								promo_bin_1,
								promo_type_2,
								sales_channel 
					 		FROM sa_offline_sales.src_offline_sales
							WHERE is_processed ='N'
)
    LOOP
			
				INSERT INTO BL_3NF.ce_sales (
            sale_id,
			date,
			product_id,
			employee_id,
			store_id,
			customer_id,
			quantity,
			stock,
			price,
			cost,
			promo_type_1,
			promo_bin_1,
			promo_type_2,
			sales_channel,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        VALUES (
            nextval('bl_3nf.sales_id_sec'),
            v_record.date::DATE,
			(SELECT product_id FROM BL_3NF.CE_PRODUCTS p WHERE p.source_id = v_record.product_id::TEXT),
			(SELECT employee_id FROM BL_3NF.CE_EMPLOYEES e WHERE e.source_id::TEXT = v_record.employee_id::TEXT),
			(SELECT store_id FROM BL_3NF.CE_STORES s WHERE s.source_id::TEXT = v_record.store_id::TEXT),
			(SELECT customer_id FROM BL_3NF.CE_Customers_scd cs 
			 WHERE cs.source_id::TEXT = v_record.customer_id::TEXT 
			 AND cs.is_active = 'Y'),
			v_record.quantity::NUMERIC,
			v_record.stock::NUMERIC,
			v_record.price::NUMERIC,
			v_record.cost::NUMERIC,
			v_record.promo_type_1,
			v_record.promo_bin_1,
			v_record.promo_type_2,
			v_record.sales_channel,
            current_timestamp,
            v_record.invoice_number,
            'SRC_OFFLINE_SALES',
            'BL_CL');
            
			inserted_count := inserted_count + 1;
		
        UPDATE sa_offline_sales.src_offline_sales s
		SET is_processed = 'Y'
		WHERE s.invoice_number=v_record.invoice_number;
			
        
    END LOOP;

    	RAISE NOTICE 'Inserted rows: % in ce_sales', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_fct_sales_procedure', inserted_count,'sucess');
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_fct_sales_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_products_from_online_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN


UPDATE sa_online_sales.SRC_ONLINE_SALES
SET product_length = -1
WHERE product_length = 'NULL';

UPDATE sa_online_sales.SRC_ONLINE_SALES
SET product_depth = -1 
WHERE product_depth = 'NULL';


UPDATE sa_online_sales.SRC_ONLINE_SALES
SET product_width = -1
WHERE product_width = 'NULL';

WITH RankedProducts AS (
    SELECT 
        product_id, 
        product_name,
        product_length,
        product_depth,
        product_width,
        product_price,
        product_cost,
        product_stock,
        hierarchy1_id,
        hierarchy2_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM sa_online_sales.SRC_ONLINE_SALES
),
		 distinct_source_rows AS (
			SELECT 
		product_id, 
		product_name,
		product_length,
		product_depth,
		product_width,
		product_price,
		product_cost,
		product_stock,
		hierarchy1_id,
		hierarchy2_id
	FROM RankedProducts
	WHERE rn = 1),
			
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_products (
            product_id,
			product_name,
			product_length,
			product_depth,
			product_width,
			product_price,
			product_cost,
			product_stock,
			hierarchy1_id,
			hierarchy2_id,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.products_id_sec'),
            dsr.product_name,
            COALESCE(dsr.product_length::NUMERIC,-1),
			COALESCE(dsr.product_depth::NUMERIC,-1),
			COALESCE(dsr.product_width::NUMERIC,-1),
			COALESCE(dsr.product_price::NUMERIC,-1),
			COALESCE(dsr.product_cost::NUMERIC,-1),
			COALESCE(dsr.product_stock::NUMERIC,-1),
			dsr.hierarchy1_id,
			dsr.hierarchy2_id,
            current_timestamp,
            dsr.product_id,
            'SRC_ONLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_products c
            WHERE c.source_id = dsr.product_id 
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

	RAISE NOTICE 'Inserted rows from online : % in ce_products', inserted_count;
	
	
	INSERT INTO logging (procedure_name, rows_affected,message)
	VALUES ('insert_ce_products_from_online_procedure', inserted_count,'success');
		
		
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_products_from_online_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;
   

--------------------------------------------------------------------

-- CE_STORES 

CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_stores_from_online_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            store_id, 
            shop_website
        FROM sa_online_sales.SRC_ONLINE_SALES
    ),
    inserted_rows AS (
        INSERT INTO BL_3NF.ce_stores (
            store_id,
			store_name,
			store_size,
			shop_website,
            store_type_id,
			store_address_id,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('bl_3nf.stores_id_sec'),
            'n.a.',
            -1,
			dsr.shop_website,
			'-1',
			'-1',
            current_timestamp,
            dsr.store_id,
            'SRC_ONLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_stores c
            WHERE c.source_id = dsr.store_id
        )
        RETURNING *
    )
        SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

		RAISE NOTICE 'Inserted rows from online: % in ce_stores', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_stores_from_online_procedure', inserted_count,'success');
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_ce_stores_from_online_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


----------------------------------------------------------------------



-- CE_CUSTOMERS_SCD
	
	
CREATE OR REPLACE PROCEDURE BL_3NF.insert_or_update_ce_customers_scd_from_online_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INT := 0;
    updated_count INT := 0;
    v_record RECORD;
BEGIN
	CREATE INDEX IF NOT EXISTS idx_ce_customers_source_id ON BL_3NF.ce_customers_scd(source_id);
	CREATE INDEX IF NOT EXISTS idx_src_online_sales_customer_id ON sa_online_sales.SRC_ONLINE_SALES(customer_id);

    FOR v_record IN (SELECT DISTINCT customer_id, f_name, l_name, email FROM sa_online_sales.src_online_sales)
    LOOP

        -- Check if the record exists in the target table
        IF EXISTS (
            SELECT 1
            FROM BL_3NF.ce_customers_scd cs
            WHERE cs.source_id::TEXT = v_record.customer_id::TEXT
        ) THEN

			IF NOT EXISTS (
                SELECT 1
                FROM BL_3NF.ce_customers_scd cs
                WHERE cs.source_id::INT = v_record.customer_id::INT AND 
                	cs.f_name = v_record.f_name AND 
                     cs.l_name = v_record.l_name AND
                     cs.email = v_record.email  
                     )
             THEN
                UPDATE BL_3NF.ce_customers_scd
                SET 
					end_dt = current_timestamp,
					is_active = 'N'
                WHERE source_id::INT = v_record.customer_id::INT AND is_active = 'Y';
				
				INSERT INTO BL_3NF.ce_customers_scd (customer_id, f_name, l_name, email, cust_phone, start_dt, end_dt, is_active, insert_dt,
                                                 source_id, source_entity, source_system)
				VALUES (nextval('bl_3nf.customer_id_sec'), v_record.f_name, v_record.l_name, v_record.email, 'n.a.',
						current_timestamp, '9999-01-01', 'Y', current_timestamp, v_record.customer_id, 'SRC_ONLINE_SALES', 'BL_CL');
				updated_count := updated_count + 1;
				
				
				
            END IF;
        ELSE

            -- Record does not exist, insert it
            INSERT INTO BL_3NF.ce_customers_scd (customer_id, f_name, l_name, email, cust_phone, start_dt, end_dt, is_active, insert_dt,
                                                 source_id, source_entity, source_system)
            VALUES (nextval('bl_3nf.customer_id_sec'), v_record.f_name, v_record.l_name, v_record.email, 'n.a.',
                    current_timestamp, '9999-01-01', 'Y', current_timestamp, v_record.customer_id, 'SRC_ONLINE_SALES', 'BL_CL');
            inserted_count := inserted_count + 1;
        END IF;
    END LOOP;

    	RAISE NOTICE 'Inserted rows from online : % in ce_customers_scd , updated %', inserted_count,updated_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_or_update_ce_customers_scd_from_online_procedure', inserted_count,updated_count);
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_or_update_ce_customers_scd_from_online_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;



---------------------------------------------


--CE_SALES TABLE 


CREATE OR REPLACE PROCEDURE  BL_3NF.insert_fct_sales_from_online_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INT := 0;
    v_record RECORD;
BEGIN
	CREATE INDEX IF NOT EXISTS idx_is_proc ON sa_online_sales.src_online_sales(is_processed);

	FOR v_record IN (SELECT  
					 			invoice_number,
								date,
								product_id,
								employee_id,
								store_id,
								customer_id,
								quantity,
								stock,
								price,
								cost,
								promo_type_1,
								promo_bin_1,
								sales_channel 
					 		FROM sa_online_sales.src_online_sales
							WHERE is_processed ='N'
)
    LOOP
			
				INSERT INTO BL_3NF.ce_sales (
            sale_id,
			date,
			product_id,
			employee_id,
			store_id,
			customer_id,
			quantity,
			stock,
			price,
			cost,
			promo_type_1,
			promo_bin_1,
			promo_type_2,
			sales_channel,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        VALUES (
            nextval('bl_3nf.sales_id_sec'),
            v_record.date::DATE,
			(SELECT product_id FROM BL_3NF.CE_PRODUCTS p WHERE p.source_id = v_record.product_id::TEXT),
			(SELECT employee_id FROM BL_3NF.CE_EMPLOYEES e WHERE e.source_id::TEXT = v_record.employee_id::TEXT),
			(SELECT store_id FROM BL_3NF.CE_STORES s WHERE s.source_id::TEXT = v_record.store_id::TEXT),
			(SELECT customer_id FROM BL_3NF.CE_Customers_scd cs 
			 WHERE cs.source_id::TEXT = v_record.customer_id::TEXT 
			 AND cs.is_active = 'Y'),
			v_record.quantity::NUMERIC,
			v_record.stock::NUMERIC,
			v_record.price::NUMERIC,
			v_record.cost::NUMERIC,
			v_record.promo_type_1,
			v_record.promo_bin_1,
			'n.a.',
			v_record.sales_channel,
            current_timestamp,
            v_record.invoice_number,
            'SRC_ONLINE_SALES',
            'BL_CL');
            
			inserted_count := inserted_count + 1;
		
        UPDATE sa_online_sales.src_online_sales s
		SET is_processed = 'Y'
		WHERE s.invoice_number=v_record.invoice_number;
			
        
    END LOOP;

    	RAISE NOTICE 'Inserted rows from online : % in ce_sales', inserted_count;
	
		INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_fct_sales_from_online_procedure', inserted_count,'sucess');
	
EXCEPTION
    WHEN OTHERS THEN
   
        INSERT INTO logging (procedure_name, rows_affected,message)
        VALUES ('insert_fct_sales_from_online_procedure', -1,'failed');

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




------------------
------------------
------------------
-- DIM_CUSTOMERS_SCD


CREATE SEQUENCE IF NOT EXISTS BL_DM.stores_id_sec_dm;


CREATE OR REPLACE PROCEDURE bl_dm.insert_DM_CUSTOMERS_SCD_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_CUSTOMERS_SCD (
            customer_surr_id,
            f_name,
            l_name,
            email,
            cust_phone,
            start_dt,
            end_dt,
            is_active,
            insert_dt,
            source_id,
            source_entity,
            source_system
        )
        SELECT 
            nextval('BL_DM.stores_id_sec_dm'),
            f_name,
            l_name,
            email,
            cust_phone,
            start_dt,
            end_dt,
            is_active,
            insert_dt,
            customer_id,
            'CE_CUSTOMERS_SCD',
            'BL_3NF'
        FROM BL_3NF.ce_customers_scd cus
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_CUSTOMERS_SCD c
            WHERE c.source_id::INT = cus.customer_id AND c.start_dt = cus.start_dt
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DM_COUNTRIES_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_COUNTRIES', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DM_COUNTRIES_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;







-----------------------------------------------
--DIM_DATES


CREATE OR REPLACE PROCEDURE bl_dm.insert_DIM_DATES_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_DATES (
            event_date_surr_id,
            day_of_week,
            day_of_month,
            day_of_year,
            week_of_year,
            month,
            quarter,
            year,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            dt ,
            EXTRACT(dow FROM dt) AS day_of_week,
            EXTRACT(day FROM dt) AS day_of_month,
            EXTRACT(doy FROM dt) AS day_of_year,
            EXTRACT(week FROM dt) AS week_of_year,
            EXTRACT(month FROM dt) AS month,
            EXTRACT(quarter FROM dt) AS quarter,
            EXTRACT(year FROM dt) AS year,
            '9999-01-01', -- Placeholder for some_column1
            'MANUAL', -- Placeholder for some_column2
            'MANUAL' -- Placeholder for some_column3
        FROM generate_series('2021-10-01'::date, '2024-02-01'::date, '1 day'::interval) AS dt
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_DATES c
            WHERE c.event_date_surr_id = dt
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DM_DATES_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_DATES', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DM_DATES_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;






-----------------------------------------------
--DIM_EMPLOYEES


CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_employees_id_sec;



CREATE OR REPLACE PROCEDURE bl_dm.insert_DIM_EMPLOYEES_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_EMPLOYEES (
            employee_surr_id,
            employee_name,
            employee_last_name,
            employee_email,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.dm_employees_id_sec') ,
            employee_name,
            employee_last_name,
            employee_email,
            employee_id, 
            'BL_3NF', 
            'CE_EMPLOYEES' 
        FROM BL_3NF.CE_EMPLOYEES e
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_EMPLOYEES c
            WHERE c.source_id = e.employee_id
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DIM_EMPLOYEES_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_EMPLOYEES', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_EMPLOYEES_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;









-----------------------------------------------
--DIM_PRODUCTS

CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_products_id_sec;



CREATE OR REPLACE PROCEDURE BL_DM.insert_DIM_PRODUCTS_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_PRODUCTS (
            product_surr_id,
            product_name,
            product_length,
            product_depth,
			product_width,
			product_cost,
			product_price,
			product_stock,
			hierarchy1_id,
			hierarchy2_id,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.dm_products_id_sec') ,
            product_name,
            product_length,
            product_depth,
			product_width,
			product_cost,
			product_price,
			product_stock,
			hierarchy1_id,
			hierarchy2_id,
            product_id, 
            'BL_3NF',
            'CE_PRODUCTS'
        FROM BL_3NF.CE_PRODUCTS p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_PRODUCTS c
            WHERE c.source_id = p.product_id
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DIM_PRODUCTS_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_PRODUCTS', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_PRODUCTS_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;





-----------------------------------------------


--DIM_PROMO_BIN_1

CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_promo_bin1_id_sec;



CREATE OR REPLACE PROCEDURE BL_DM.insert_DIM_PROMO_BIN_1_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_PROMO_BIN_1(
            promo_bin_surr_id,
            promo_bin1,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.dm_promo_bin1_id_sec'),
			promo_bin_1,
            promo_bin_1_id, 
            'BL_3NF',
            'CE_PROMO_BIN_1'
        FROM BL_3NF.CE_PROMO_BIN_1 p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_PROMO_BIN_1 pb
            WHERE pb.source_id = p.promo_bin_1_id
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DIM_PROMO_TYPE_1_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_PROMO_BIN_1', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_PROMO_TYPE_1_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;






-----------------------------------------------


--DIM_PROMO_TYPE_1

CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_promo_type_1_id_sec;



CREATE OR REPLACE PROCEDURE BL_DM.insert_DIM_PROMO_TYPE_1_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_PROMO_TYPE_1(
            promo_type1_surr_id,
            promo_type1,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.dm_promo_type_1_id_sec'),
			promo_type_1,
            promo_type_1_id, 
            'BL_3NF',
            'CE_PROMO_TYPE_1'
        FROM BL_3NF.CE_PROMO_TYPE_1 p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_PROMO_TYPE_1 pb
            WHERE pb.source_id = p.promo_type_1_id
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DIM_PROMO_TYPE_1_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_PROMO_TYPE_1', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_PROMO_TYPE_1_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;





-----------------------------------------------


--DIM_PROMO_TYPE_2

CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_promo_type_2_id_sec;



CREATE OR REPLACE PROCEDURE BL_DM.insert_DIM_PROMO_TYPE_2_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_PROMO_TYPE_2(
            promo_type_2_surr_id,
            promo_type2,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.dm_promo_type_2_id_sec'),
			promo_type_2,
            promo_type_2_id, 
            'BL_3NF',
            'CE_PROMO_TYPE_2'
        FROM BL_3NF.CE_PROMO_TYPE_2 p
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_PROMO_TYPE_2 pb
            WHERE pb.source_id = p.promo_type_2_id
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DIM_PROMO_TYPE_2_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_PROMO_TYPE_2', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_PROMO_TYPE_2_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;




-----------------------------------------------
--DIM_STORES

CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_store_id_sec;


CREATE OR REPLACE PROCEDURE BL_DM.insert_DIM_STORES_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.DIM_STORES (
            store_surr_id,
            store_name,
            store_size,
            shop_website,
			storetype_id,
			storetype_name,
			store_address_id,
			store_address,
			store_state,
			city_id,
			city_name,
			country_id,
			country_name,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.dm_store_id_sec') ,
            store_name,
            store_size,
            'n.a.',
			st.store_type_id,
			st.store_type_name,
			ad.store_address_id,
			ad.store_address,
			ad.store_state,
			ad.city_id,
			ci.city_name,
			co.country_id,
			co.country_name,
            store_id, 
            'BL_3NF',
            'CE_STORES'
        FROM BL_3NF.CE_STORES p
		INNER JOIN BL_3NF.ce_store_types st ON p.store_type_id::TEXT = st.store_type_id::TEXT
		INNER JOIN BL_3NF.ce_addresses ad ON p.store_address_id::TEXT = ad.store_address_id::TEXT
        INNER JOIN BL_3NF.ce_cities ci ON ad.city_id::TEXT = ci.city_id::TEXT
		INNER JOIN BL_3NF.ce_countries co ON ci.country_id::TEXT = co.country_id::TEXT
		
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.DIM_STORES c
            WHERE c.source_id::INT = p.store_id::INT
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_DIM_STORES_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in DIM_STORES', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_STORES_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


--       CALL insert_DIM_STORES_procedure();



-----------------------------------------------
--FCT_SALES_DD

CREATE SEQUENCE IF NOT EXISTS BL_DM.fct_sales_id_sec;

-- Index on sales_surr_id in BL_DM.FCT_SALES_DD
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_sales_surr_id
ON BL_DM.FCT_SALES_DD (sales_surr_id);

-- Index on event_date_surr_id in BL_DM.DIM_DATES
CREATE INDEX IF NOT EXISTS idx_dim_dates_event_date_surr_id
ON BL_DM.DIM_DATES (event_date_surr_id);

-- Index on source_id in BL_DM.DIM_PRODUCTS
CREATE INDEX IF NOT EXISTS idx_dim_products_source_id
ON BL_DM.DIM_PRODUCTS (source_id);

-- Index on source_id in BL_DM.DIM_EMPLOYEES
CREATE INDEX IF NOT EXISTS idx_dim_employees_source_id
ON BL_DM.DIM_EMPLOYEES (source_id);

-- Index on source_id in BL_DM.DIM_STORES
CREATE INDEX IF NOT EXISTS idx_dim_stores_source_id
ON BL_DM.DIM_STORES (source_id);

-- Index on source_id in BL_DM.DIM_CUSTOMERS_SCD
CREATE INDEX IF NOT EXISTS idx_dim_customers_scd_source_id
ON BL_DM.DIM_CUSTOMERS_SCD (source_id);


-- Index on source_id in BL_DM.DIM_EMPLOYEES
CREATE INDEX IF NOT EXISTS idx_dim_employees_source_idsssaassa
ON BL_DM.DIM_EMPLOYEES (employee_surr_id);

-- Index on source_id in BL_DM.DIM_STORES
CREATE INDEX IF NOT EXISTS idx_dim_stores_source_idsssss
ON BL_DM.DIM_STORES (store_surr_id);

-- Index on source_id in BL_DM.DIM_CUSTOMERS_SCD
CREATE INDEX IF NOT EXISTS idx_dim_customers_scd_source_id
ON BL_DM.DIM_CUSTOMERS_SCD (customer_surr_id);

-- Index on sale_id in BL_3NF.CE_SALES
CREATE INDEX IF NOT EXISTS idx_ce_sales_sale_id
ON BL_3NF.CE_SALES (sale_id);

-- Index on source_id in BL_DM.FCT_SALES_DD
CREATE INDEX IF NOT EXISTS idx_fct_sales_dd_source_id
ON BL_DM.FCT_SALES_DD (source_id);



CREATE INDEX IF NOT EXISTS idx_customer_id
ON BL_3NF.CE_SALES(customer_id);


CREATE INDEX IF NOT EXISTS idx_customer_id
ON BL_3NF.CE_SALES(employee_id);

CREATE INDEX IF NOT EXISTS idx_customer_id
ON BL_3NF.CE_SALES(store_id);

CREATE INDEX IF NOT EXISTS idx_customer_id
ON BL_3NF.CE_SALES(product_id)
;

CREATE INDEX IF NOT EXISTS idx_customer_id
ON BL_3NF.CE_SALES(date);


CREATE INDEX IF NOT EXISTS idx_event_date_surr_id
ON BL_DM.DIM_DATES(event_date_surr_id);






CREATE OR REPLACE PROCEDURE BL_DM.insert_FCT_SALES_DD_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
    default_product_surr_id INTEGER;
    default_employee_surr_id INTEGER;
    default_store_surr_id INTEGER;
    default_customer_surr_id INTEGER;
BEGIN
    -- Get default surrogate IDs from FCT_SALES_DD where the ID is -1
    SELECT product_surr_id, employee_surr_id, store_surr_id, customer_surr_id
    INTO default_product_surr_id, default_employee_surr_id, default_store_surr_id, default_customer_surr_id
    FROM BL_DM.FCT_SALES_DD
    WHERE sales_surr_id = -1;

    -- Insert into the target table and count the inserted rows
    WITH inserted AS (
        INSERT INTO BL_DM.FCT_SALES_DD (
            sales_surr_id,
            event_date_surr_id,
            product_surr_id,
            employee_surr_id,
            store_surr_id,
            customer_surr_id,
            quantity,
            stock,
            price,
            cost,
            sales_channel,
            source_id,
            source_entity,
            source_system
        )
        SELECT
            nextval('BL_DM.fct_sales_id_sec'),
                (SELECT event_date_surr_id FROM BL_DM.DIM_DATES dd WHERE dd.event_date_surr_id = s.date),

            COALESCE(
                (SELECT product_surr_id FROM BL_DM.DIM_PRODUCTS dp WHERE dp.source_id::INT = s.product_id::INT),
                -1
            ),
            COALESCE(
                (SELECT employee_surr_id FROM BL_DM.DIM_EMPLOYEES de WHERE de.source_id::INT= s.employee_id::INT),
                -1
            ),
            COALESCE(
                (SELECT store_surr_id FROM BL_DM.DIM_STORES ds WHERE ds.source_id::INT = s.store_id::INT),
                -1
            ),
            COALESCE(
                (SELECT customer_surr_id FROM BL_DM.DIM_CUSTOMERS_SCD dc WHERE dc.source_id::INT = s.customer_id::INT AND dc.is_active='Y'),
                -1
            ),
            s.quantity,
            s.stock,
            s.price,
            s.cost,
            s.sales_channel,
            s.sale_id,
            'BL_3NF',
            'CE_STORES'
        FROM BL_3NF.CE_SALES s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_DM.FCT_SALES_DD c
            WHERE c.source_id::INT = s.sale_id
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_FCT_SALES_DD_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: % in FCT_SALES_DD', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_FCT_SALES_DD_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;



--------\

--MAIN PROCEDURE 


CREATE OR REPLACE PROCEDURE BL_DM.main_insert_procedures_in_DM()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL BL_DM.insert_DM_CUSTOMERS_SCD_procedure();
    CALL BL_DM.insert_DIM_DATES_procedure();
    CALL BL_DM.insert_DIM_EMPLOYEES_procedure();
    CALL BL_DM.insert_DIM_PRODUCTS_procedure();
    CALL BL_DM.insert_DIM_PROMO_TYPE_1_procedure();
    CALL BL_DM.insert_DIM_PROMO_BIN_1_procedure();
    CALL BL_DM.insert_DIM_PROMO_TYPE_2_procedure();
    CALL BL_DM.insert_DIM_STORES_procedure();
    CALL BL_DM.insert_FCT_SALES_DD_procedure();
END;
$$;







CREATE OR REPLACE PROCEDURE BL_DM.main_insert_procedures()
LANGUAGE plpgsql
AS $$
BEGIN
		   
		CALL public.create_foreign_tables_procedure();
		CALL public.create_src_tables_procedure();
		--CALL sa_online_sales.insert_data_into_src_online_procedure();
		CALL sa_offline_sales.insert_data_into_src_offline_procedure();

		CALL BL_3NF.insert_default_rows_procedure();
		CALL public.create_3nf_and_tables_procedure();
		CALL BL_3NF.create_sequencies_for_3nf_tables_procedure();
		CALL BL_3NF.create_partitions_on_sales();

		CALL BL_3NF.insert_ce_countries_procedure();
		CALL BL_3NF.insert_ce_cities_procedure();
		CALL BL_3NF.insert_ce_addreses_procedure();
		CALL BL_3NF.insert_ce_products_procedure();
		CALL BL_3NF.insert_ce_store_types_procedure();
		CALL BL_3NF.insert_ce_stores_procedure();
		CALL BL_3NF.insert_or_update_ce_customers_scd_procedure();
		CALL BL_3NF.insert_ce_employees_procedure_online();
		CALL BL_3NF.insert_ce_promo_type_1_procedure();
		CAll BL_3NF.insert_ce_promo_type_2_procedure();
		CAll BL_3NF.insert_ce_promo_bin_1_procedure();
		CALL BL_3NF.insert_fct_sales_procedure();
		
		CALL BL_3NF.insert_fct_sales_from_online_procedure();
		CALL BL_3NF.insert_or_update_ce_customers_scd_from_online_procedure();
		CALL BL_3NF.insert_ce_stores_from_online_procedure();
		CALL BL_3NF.insert_ce_products_from_online_procedure();


		CALL BL_DM.main_insert_procedures_in_DM();
END;
$$;


-- Execute the main procedure
CALL BL_DM.main_insert_procedures();
