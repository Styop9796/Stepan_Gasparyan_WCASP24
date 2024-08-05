-- DIM_CUSTOMERS_SCD


CREATE SEQUENCE IF NOT EXISTS BL_DM.stores_id_sec_dm;


CREATE OR REPLACE PROCEDURE insert_DM_CUSTOMERS_SCD_procedure()
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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


CREATE OR REPLACE PROCEDURE insert_DIM_DATES_procedure()
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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



CREATE OR REPLACE PROCEDURE insert_DIM_EMPLOYEES_procedure()
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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



CREATE OR REPLACE PROCEDURE insert_DIM_PRODUCTS_procedure()
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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



CREATE OR REPLACE PROCEDURE insert_DIM_PROMO_BIN_1_procedure()
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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



CREATE OR REPLACE PROCEDURE insert_DIM_PROMO_TYPE_1_procedure()
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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



CREATE OR REPLACE PROCEDURE insert_DIM_PROMO_TYPE_2_procedure()
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
    VALUES ('insert_DIM_PROMO_TYPE_1_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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
--DIM_STORES

CREATE SEQUENCE IF NOT EXISTS BL_DM.dm_store_id_sec;



CREATE OR REPLACE PROCEDURE insert_DIM_STORES_procedure()
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
		INNER JOIN BL_3NF.ce_addresses ad ON p.store_address_id::INT = ad.store_address_id::INT
        INNER JOIN BL_3NF.ce_cities ci ON ad.city_id::INT = ci.city_id::INT
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

    RAISE NOTICE 'Inserted rows: %', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_DIM_STORES_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;





-----------------------------------------------
--FCT_SALES_DD

CREATE SEQUENCE IF NOT EXISTS BL_DM.fct_sales_id_sec;



CREATE OR REPLACE PROCEDURE insert_FCT_SALES_DD_procedure()
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
                (SELECT event_date_surr_id FROM BL_DM.DIM_DATES dd WHERE dd.event_date_surr_id::TEXT = s.date::TEXT),
            
            COALESCE(
                (SELECT product_surr_id FROM BL_DM.DIM_PRODUCTS dp WHERE dp.source_id::TEXT = s.product_id::TEXT),
                default_product_surr_id
            ),
            COALESCE(
                (SELECT employee_surr_id FROM BL_DM.DIM_EMPLOYEES de WHERE de.source_id::TEXT= s.employee_id::TEXT),
                default_employee_surr_id
            ),
            COALESCE(
                (SELECT store_surr_id FROM BL_DM.DIM_STORES ds WHERE ds.source_id::TEXT = s.store_id::TEXT),
                default_store_surr_id
            ),
            COALESCE(
                (SELECT customer_surr_id FROM BL_DM.DIM_CUSTOMERS_SCD dc WHERE dc.source_id::TEXT = s.customer_id::TEXT),
                default_customer_surr_id
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
            WHERE c.source_id::TEXT = s.sale_id::TEXT
        )
        RETURNING 1 -- Use RETURNING to count the rows
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_FCT_SALES_DD_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: %', inserted_count;

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

CALL insert_DM_CUSTOMERS_SCD_procedure();
CALL insert_DIM_DATES_procedure();
CALL insert_DIM_EMPLOYEES_procedure();
CALL insert_DIM_PRODUCTS_procedure();
CALL insert_DIM_PROMO_TYPE_1_procedure();
CALL insert_DIM_PROMO_BIN_1_procedure();
CALL insert_DIM_PROMO_TYPE_2_procedure();
CALL insert_DIM_STORES_procedure();
CALL insert_FCT_SALES_DD_procedure();



CREATE OR REPLACE PROCEDURE main_insert_procedures()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL insert_DM_CUSTOMERS_SCD_procedure();
    CALL insert_DIM_DATES_procedure();
    CALL insert_DIM_EMPLOYEES_procedure();
    CALL insert_DIM_PRODUCTS_procedure();
    CALL insert_DIM_PROMO_TYPE_1_procedure();
    CALL insert_DIM_PROMO_BIN_1_procedure(); -- Duplicate call, verify if intentional
    CALL insert_DIM_PROMO_TYPE_2_procedure();
    CALL insert_DIM_STORES_procedure();
    CALL insert_FCT_SALES_DD_procedure();
END;
$$;

-- Execute the main procedure
CALL main_insert_procedures();





