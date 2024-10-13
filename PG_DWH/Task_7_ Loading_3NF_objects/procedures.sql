--- LOGGIN TABLE 


CREATE TABLE IF NOT EXISTS  public.logging (
    log_id SERIAL PRIMARY KEY,
    log_datetime DATE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    procedure_name VARCHAR(255) NOT NULL,
    rows_affected INT,
	message TEXT
);

-------------------------------------------------------

--CREATE ROLE developer;


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sa_offline_sales TO developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sa_online_sales TO developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA BL_3NF TO developer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA BL_DM TO developer;



GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sa_offline_sales TO developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sa_online_sales TO developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA BL_3NF TO developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA BL_DM TO developer;



GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO developer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sa_offline_sales TO developer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sa_online_sales TO developer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA BL_3NF TO developer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA BL_DM TO developer;


GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA public TO developer;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA sa_offline_sales TO developer;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA sa_online_sales TO developer;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA BL_3NF TO developer;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA BL_DM TO developer;


---------------------------------------------------------


-- CE_COUNTRIES




CREATE SEQUENCE IF NOT EXISTS BL_3NF.country_id_sec;



CREATE OR REPLACE FUNCTION BL_3NF.insert_ce_countries()
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
            nextval('BL_3NF.country_id_sec'),
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




CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_countries_procedure()
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



CALL BL_3NF.insert_ce_countries_procedure();




----------------------------------------------------

-- CE_CITIES 

CREATE SEQUENCE IF NOT EXISTS BL_3NF.city_id_sec;



CREATE OR REPLACE FUNCTION BL_3NF.insert_ce_cities()
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
            nextval('BL_3NF.city_id_sec'),
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





CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_cities_procedure()
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


CALL BL_3NF.insert_ce_cities_procedure();


------------------------------------------------------------------------


-- CE_ ADDRESSES

CREATE SEQUENCE IF NOT EXISTS BL_3NF.address_id_sec;



CREATE OR REPLACE FUNCTION BL_3NF.insert_ce_addreses()
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
            nextval('BL_3NF.address_id_sec'),
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


SELECT BL_3NF.insert_ce_addreses();



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_addresses_procedure()
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


CALL BL_3NF.insert_ce_addresses_procedure();

-------------------------------------------------------------




-- CE_PRODUCTS

CREATE SEQUENCE IF NOT EXISTS BL_3NF.products_id_sec;



CREATE OR REPLACE FUNCTION BL_3NF.insert_ce_products()
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
            nextval('BL_3NF.products_id_sec'),
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


SELECT BL_3NF.insert_ce_products();


CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_products_procedure()
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


CALL BL_3NF.insert_ce_products_procedure();


------------------------------------------------------


--CE_SALES TABLE 




CREATE SEQUENCE IF NOT EXISTS BL_3NF.sales_id_sec;





CREATE INDEX idx_ce_sales_source_id ON BL_3NF.ce_sales(source_id);
CREATE INDEX idx_src_offline_sales_invoice_number ON sa_offline_sales.SRC_OFFLINE_SALES(invoice_number);



CREATE OR REPLACE FUNCTION BL_3NF.insert_ce_sales()
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
            nextval('BL_3NF.sales_id_sec'),
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




CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_sales_procedure()
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


CALL BL_3NF.insert_ce_sales_procedure();




--------------------------------------------------------------------


-- CE_STORES 

CREATE SEQUENCE IF NOT EXISTS BL_3NF.stores_id_sec;



CREATE OR REPLACE FUNCTION BL_3NF.insert_ce_stores()
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
            nextval('BL_3NF.stores_id_sec'),
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



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_stores_procedure()
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


CALL BL_3NF.insert_ce_sales_procedure();



----------------------------------------------------------------------


-- CE_CUSTOMERS_SCD



CREATE SEQUENCE IF NOT EXISTS BL_3NF.customer_id_sec;


CREATE INDEX idx_ce_customers_source_id ON BL_3NF.ce_customers_scd(source_id);
CREATE INDEX idx_src_offline_sales_customer_id ON sa_offline_sales.SRC_OFFLINE_SALES(customer_id);
	
CREATE OR REPLACE FUNCTION BL_3NF.insert_or_update_ce_customers_scd()
RETURNS TABLE (inserted_count INT, updated_count INT) AS $$
DECLARE
    inserted_count INT := 0;
    updated_count INT := 0;
    v_record RECORD;
BEGIN
    FOR v_record IN (SELECT customer_id, f_name, l_name, email, cust_phone FROM sa_offline_sales.src_offline_sales)
    LOOP
        
        -- Check if the record exists in the target table
        IF EXISTS (
            SELECT 1
            FROM BL_3NF.ce_customers_scd cs
            WHERE cs.source_id::INT = v_record.customer_id::INT
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
				VALUES (nextval('BL_3NF.customer_id_sec'), v_record.f_name, v_record.l_name, v_record.email, v_record.cust_phone,
						current_timestamp, '9999-01-01', 'Y', current_timestamp, v_record.customer_id, 'SRC_OFFLINE_SALES', 'BL_CL');
				updated_count := updated_count + 1;
				
				
				
            END IF;
        ELSE
            -- Record does not exist, insert it
            INSERT INTO BL_3NF.ce_customers_scd (customer_id, f_name, l_name, email, cust_phone, start_dt, end_dt, is_active, insert_dt,
                                                 source_id, source_entity, source_system)
            VALUES (nextval('BL_3NF.customer_id_sec'), v_record.f_name, v_record.l_name, v_record.email, v_record.cust_phone,
                    current_timestamp, '9999-01-01', 'Y', current_timestamp, v_record.customer_id, 'SRC_OFFLINE_SALES', 'BL_CL');
            inserted_count := inserted_count + 1;
        END IF;
    END LOOP;

    RETURN QUERY SELECT inserted_count, updated_count;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE PROCEDURE BL_3NF.insert_ce_customers_scd_procedure()
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER;
    updated_count INTEGER;
BEGIN
    -- Call the function and capture the returned values
    SELECT * INTO inserted_count, updated_count
    FROM insert_or_update_ce_customers_scd();
    
    -- Log the results
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_customers_scd_procedure', inserted_count);

    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_ce_customers_scd_procedure_updates', updated_count);

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_ce_customers_scd_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


CALL BL_3NF.insert_ce_customers_scd_procedure();






