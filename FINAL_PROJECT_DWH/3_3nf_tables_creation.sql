
CREATE OR REPLACE PROCEDURE public.create_3nf_and_tables_procedure()
LANGUAGE plpgsql
AS $$
BEGIN

		CREATE SCHEMA IF NOT EXISTS BL_3NF;

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
	VALUES (-1, CURRENT_TIMESTAMP, 'n.a.', -1, 'n.a.', -1, -1, -1, -1, -1, 'n.a.', 'n.a.', 'n.a.', 'n.a.', CURRENT_TIMESTAMP, 'n.a.', 'MANUAL', 'MANUAL')
	ON CONFLICT DO NOTHING;



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








