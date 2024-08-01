--- LOGGIN TABLE 



CREATE TABLE IF NOT EXISTS  logging (
    log_id SERIAL PRIMARY KEY,
    log_datetime DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procedure_name VARCHAR(255) NOT NULL,
    rows_affected INT
);



---------------------------------------------------------


-- CE_COUNTRIES




CREATE SEQUENCE IF NOT EXISTS country_id_sec;



CREATE OR REPLACE FUNCTION insert_ce_countries()
RETURNS INTEGER AS $$
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
            nextval('country_id_sec'),
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

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;




CREATE OR REPLACE PROCEDURE insert_ce_countries_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    inserted_count := insert_ce_countries();

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_countries_procedure', inserted_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('execute_and_log_insert_ce_countries', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error accured';
END;
$$;



CALL insert_ce_countries_procedure();




----------------------------------------------------

-- CE_CITIES 

CREATE SEQUENCE IF NOT EXISTS city_id_sec;



CREATE OR REPLACE FUNCTION insert_ce_cities()
RETURNS INTEGER AS $$
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
            nextval('city_id_sec'),
            dsr.city_name,
            dsr.country_id::TEXT,
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

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;





CREATE OR REPLACE PROCEDURE insert_ce_cities_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    inserted_count := insert_ce_cities();

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_cities_procedure', inserted_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_ce_cities_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error accured';
END;
$$;


CALL insert_ce_cities_procedure();


------------------------------------------------------------------------


-- CE_ ADDRESSES

CREATE SEQUENCE IF NOT EXISTS address_id_sec;



CREATE OR REPLACE FUNCTION insert_ce_addreses()
RETURNS INTEGER AS $$
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
            nextval('address_id_sec'),
            dsr.store_address,
            dsr.store_state,
			dsr.city_id::TEXT,
            current_timestamp,
            dsr.store_address,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_addresses c
            WHERE c.source_id = dsr.store_address
              AND c.store_address = dsr.store_address 
              AND c.store_state = dsr.store_state AND c.city_id::TEXT = dsr.city_id::TEXT
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;


SELECT insert_ce_addreses();



CREATE OR REPLACE PROCEDURE insert_ce_addresses_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    inserted_count := insert_ce_addreses();

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_addresses_procedure', inserted_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_ce_addresses_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error accured';
END;
$$;


CALL insert_ce_addresses_procedure();

-------------------------------------------------------------




-- CE_PRODUCTS

CREATE SEQUENCE IF NOT EXISTS products_id_sec;



CREATE OR REPLACE FUNCTION insert_ce_products()
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT 
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
        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
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
            nextval('products_id_sec'),
            dsr.product_name,
            dsr.product_length::NUMERIC,
			dsr.product_depth::NUMERIC,
			dsr.product_width::NUMERIC,
			dsr.product_price::NUMERIC,
			dsr.product_cost::NUMERIC,
			dsr.product_stock::NUMERIC,
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
              AND c.product_name = dsr.product_name 
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;


SELECT insert_ce_products();


CREATE OR REPLACE PROCEDURE insert_ce_products_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    inserted_count := insert_ce_products();

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_products_procedure', inserted_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_ce_products_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error accured';
END;
$$;


CALL insert_ce_products_procedure();


------------------------------------------------------


--CE_SALES TABLE 




CREATE SEQUENCE IF NOT EXISTS sales_id_sec;





CREATE INDEX idx_ce_sales_source_id ON BL_3NF.ce_sales(source_id);
CREATE INDEX idx_src_offline_sales_invoice_number ON sa_offline_sales.SRC_OFFLINE_SALES(invoice_number);



CREATE OR REPLACE FUNCTION insert_ce_sales()
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH source_rows AS (
        SELECT  
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

        FROM sa_offline_sales.SRC_OFFLINE_SALES
    ),
    inserted_rows AS (
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
        SELECT 
            nextval('sales_id_sec'),
            dsr.date::DATE,
			dsr.product_id::TEXT,
			dsr.employee_id::INT,
			dsr.store_id::TEXT,
			dsr.customer_id::INT,
			dsr.quantity::NUMERIC,
			dsr.stock::NUMERIC,
			dsr.price::NUMERIC,
			dsr.cost::NUMERIC,
			dsr.promo_type_1,
			dsr.promo_bin_1,
			dsr.promo_type_2,
			dsr.sales_channel,
            current_timestamp,
            dsr.invoice_number,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_sales c
            WHERE c.source_id = dsr.invoice_number
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;




CREATE OR REPLACE PROCEDURE insert_ce_sales_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    inserted_count := insert_ce_sales();

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_sales_procedure', inserted_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_ce_sales_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error accured';
END;
$$;


CALL insert_ce_sales_procedure();




--------------------------------------------------------------------


-- CE_STORES 

CREATE SEQUENCE IF NOT EXISTS stores_id_sec;



CREATE OR REPLACE FUNCTION insert_ce_stores()
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH distinct_source_rows AS (
        SELECT DISTINCT
            store_id, 
            store_name,
			storetype_id,
			storetype_name,
			store_size,
			store_address
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
            nextval('stores_id_sec'),
            dsr.store_name,
            dsr.store_size::INT,
			'n.a.',
			dsr.storetype_id::TEXT,
			dsr.store_address::TEXT,
			
            current_timestamp,
            dsr.store_id,
            'SRC_OFFLINE_SALES',
            'BL_CL'
        FROM distinct_source_rows dsr
        WHERE NOT EXISTS (
            SELECT 1 
            FROM BL_3NF.ce_stores c
            WHERE c.source_id = dsr.store_id
              AND c.store_name = dsr.store_name 
              AND c.store_size::TEXT = dsr.store_size::TEXT
        )
        RETURNING *
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted_rows;

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE PROCEDURE insert_ce_stores_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    inserted_count := insert_ce_stores();

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_stores_procedure', inserted_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_ce_stores_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error accured';
END;
$$;


CALL insert_ce_sales_procedure();



