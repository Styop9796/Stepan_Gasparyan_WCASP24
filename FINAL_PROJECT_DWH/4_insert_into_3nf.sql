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

