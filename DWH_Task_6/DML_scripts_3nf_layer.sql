

CREATE OR REPLACE FUNCTION insert_countries_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT country_name, country_id
        FROM sa_offline_sales.src_offline_sales
    LOOP
        -- Check for existence in target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_COUNTRIES co
            WHERE co.country_name = rec.country_name
            AND co.source_id = rec.country_id::TEXT
        ) THEN
            INSERT INTO BL_3NF.CE_COUNTRIES (country_name, insert_dt, update_dt, source_id, source_entity, source_system)
            VALUES (
                rec.country_name,
                current_date,
                current_date,
                rec.country_id::TEXT,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT insert_countries_offline_sales();


CREATE OR REPLACE FUNCTION insert_cities_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_country_id INT;
BEGIN
    FOR rec IN 
        SELECT city_name, city_id, country_id
        FROM sa_offline_sales.src_offline_sales
    LOOP
        -- Ensure that target_country_id is correctly retrieved
        SELECT country_id INTO target_country_id
        FROM BL_3NF.CE_COUNTRIES co
        WHERE co.source_id = rec.country_id::TEXT;

        -- Check for existence in target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_CITIES ct
            WHERE ct.city_name = rec.city_name
            AND ct.source_id = rec.city_id::TEXT
            AND ct.country_id = target_country_id
        ) THEN
            INSERT INTO BL_3NF.CE_CITIES (city_name, country_id, insert_dt, update_dt, source_id, source_entity, source_system)
            VALUES (
                rec.city_name,
                target_country_id,
                current_date,
                current_date,
                rec.city_id::TEXT,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;





SELECT insert_cities_offline_sales();



CREATE OR REPLACE FUNCTION insert_addresses_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_city_id INT;
BEGIN
    FOR rec IN 
        SELECT store_address, store_state, city_id
        FROM sa_offline_sales.src_offline_sales
    LOOP
        -- Ensure that target_city_id is correctly retrieved
        SELECT city_id INTO target_city_id
        FROM BL_3NF.CE_CITIES ci
        WHERE ci.source_id = rec.city_id::TEXT;

        -- Check for existence in target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_ADDRESSES ad
            WHERE ad.store_address = rec.store_address
            AND ad.store_state = rec.store_state
            AND ad.city_id = target_city_id
        ) THEN
            INSERT INTO BL_3NF.CE_ADDRESSES (store_address, store_state, city_id, insert_dt, update_dt, source_id, source_entity, source_system)
            VALUES (
                rec.store_address,
                rec.store_state,
                target_city_id,
                current_date,
                current_date,
                rec.city_id::TEXT,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;



SELECT insert_addresses_offline_sales();




CREATE OR REPLACE FUNCTION insert_store_types_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT storetype_name, storetype_id
        FROM sa_offline_sales.src_offline_sales
    LOOP
        -- Check for existence in target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_STORE_TYPES co
            WHERE co.storetype_name = rec.storetype_name
            AND co.source_id = rec.storetype_id
        ) THEN
            INSERT INTO BL_3NF.CE_STORE_TYPES (storetype_name, insert_dt, update_dt, source_id, source_entity, source_system)
            VALUES (
                rec.storetype_name,
                current_date,
                current_date,
                rec.storetype_id,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT insert_store_types_offline_sales();








CREATE OR REPLACE FUNCTION insert_stores_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_store_id INT;
BEGIN
    FOR rec IN 
        SELECT store_id, store_name, store_size, storetype_id, city_id, store_address
        FROM sa_offline_sales.src_offline_sales
    LOOP
        -- Retrieve the store_id from BL_3NF.CE_STORES where source_id matches
        SELECT store_id INTO target_store_id
        FROM BL_3NF.CE_STORES ci
        WHERE ci.source_id = rec.store_id::TEXT;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_STORES ad
            WHERE ad.store_name = rec.store_name
            AND ad.store_size = rec.store_size::INT
            AND ad.source_id = rec.store_id::TEXT
        ) THEN
            INSERT INTO BL_3NF.CE_STORES (
                store_name,
                store_size,
                shop_website,
                storetype_id,
                store_address_id,
                insert_dt,
                update_dt,
                source_id,
                source_entity,
                source_system
            )
            VALUES (
                rec.store_name,
                rec.store_size::INT,
                'n.a.',
                (SELECT  storetype_id FROM BL_3NF.CE_STORE_TYPES WHERE source_id = rec.storetype_id::TEXT LIMIT 1),
                (SELECT  store_address_id FROM BL_3NF.CE_ADDRESSES WHERE store_address = rec.store_address LIMIT 1),
                current_date,
                current_date,
                rec.store_id::TEXT,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;




SELECT insert_stores_offline_sales();







CREATE OR REPLACE FUNCTION insert_customers_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_customer_id INT;
BEGIN
    FOR rec IN 
        SELECT customer_id, f_name,l_name,email,cust_phone
        FROM sa_offline_sales.src_offline_sales
    LOOP
        SELECT customer_id INTO target_customer_id
        FROM BL_3NF.CE_CUSTOMERS_SCD ci
        WHERE ci.source_id::INT = rec.customer_id;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_CUSTOMERS_SCD ad
            WHERE ad.f_name = rec.f_name
            AND ad.l_name = rec.l_name
            AND ad.cust_phone = rec.cust_phone
			AND ad.email = rec.email
			AND ad.source_id::INT = rec.customer_id)
			
		THEN
            INSERT INTO BL_3NF.CE_CUSTOMERS_SCD (
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
            VALUES (
                rec.f_name,
                rec.l_name,
                rec.email,
				rec.cust_phone,
                current_date,
                '9999-12-31'::date,
				'Y',
				current_date,
                rec.customer_id,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;




SELECT insert_customers_offline_sales();





CREATE OR REPLACE FUNCTION insert_products_offline_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_product_id INT;
BEGIN
    FOR rec IN 
        SELECT product_id,product_name,product_length,product_depth,product_width,hierarchy1_id,hierarchy2_id,product_price,product_cost,product_stock
        FROM sa_offline_sales.src_offline_sales
    LOOP
        SELECT product_id INTO target_product_id
        FROM BL_3NF.CE_PRODUCTS_SCD ci
        WHERE ci.source_id = rec.product_id;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_PRODUCTS_SCD ad
            WHERE ad.product_name = rec.product_name
            AND ad.product_length::TEXT = rec.product_length
            AND ad.product_depth::TEXT = rec.product_depth
			AND ad.product_width::TEXT = rec.product_width
			AND ad.hierarchy1_id = rec.hierarchy1_id
			AND ad.hierarchy2_id = rec.hierarchy2_id
			AND ad.source_id = rec.product_id)
			
		THEN
            INSERT INTO BL_3NF.CE_PRODUCTS_SCD (
                
				product_name,
				product_length,
				product_depth,
				product_width,
				product_price,
				product_cost,
				product_stock,
				hierarchy1_id,
				hierarchy2_id,
				start_dt,
				end_dt,
				is_active,
                insert_dt,
                source_id,
                source_entity,
                source_system
            )
            VALUES (
                rec.product_name,
				CASE WHEN rec.product_length is null THEN -1 ELSE rec.product_length::NUMERIC END,
                CASE WHEN rec.product_depth is null THEN -1 ELSE rec.product_depth::NUMERIC END,
                CASE WHEN rec.product_width is null THEN -1 ELSE rec.product_width::NUMERIC END,
				rec.product_price::NUMERIC,
				rec.product_cost::NUMERIC,
				rec.product_stock::NUMERIC,
				rec.hierarchy1_id,
				rec.hierarchy2_id,
                current_date,
                '9999-12-31'::date,
				'Y',
				current_date,
                rec.product_id,
                'src_offline_sales',
                'sa_offline_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT insert_products_offline_sales();






CREATE OR REPLACE FUNCTION insert_employees_online_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_employee_id INT;
BEGIN
    FOR rec IN 
        SELECT employee_id, employee_name,employee_last_name,employee_email
        FROM sa_online_sales.src_online_sales
    LOOP
        SELECT employee_id INTO target_employee_id
        FROM BL_3NF.CE_EMPLOYEES_SCD ci
        WHERE ci.source_id::INT = rec.employee_id;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_EMPLOYEES_SCD ad
            WHERE ad.employee_name = rec.employee_name
            AND ad.employee_last_name = rec.employee_last_name
            AND ad.employee_email = rec.employee_email
			AND ad.source_id::INT = rec.employee_id)
			
		THEN
            INSERT INTO BL_3NF.CE_EMPLOYEES_SCD (
                employee_name,
				employee_last_name,
				employee_email,
				start_dt,
				end_dt,
				is_active,
                insert_dt,
                source_id,
                source_entity,
                source_system
            )
            VALUES (
                rec.employee_name,
                rec.employee_last_name,
                rec.employee_email,
                current_date,
                '9999-12-31'::date,
				'Y',
				current_date,
                rec.employee_id,
                'src_online_sales',
                'sa_online_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;




SELECT insert_employees_online_sales();



CREATE OR REPLACE FUNCTION insert_sales_online_sales()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
    target_sale_id INT;
BEGIN
    FOR rec IN 
        SELECT invoice_number, date,product_id,employee_id,store_id,customer_id,quantity,stock,price,cost,sales_channel
        FROM sa_online_sales.src_online_sales
    LOOP
        SELECT sale_id INTO target_sale_id
        FROM BL_3NF.CE_SALES_SCD ci
        WHERE ci.source_id = rec.invoice_number;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_SALES_SCD ad
            WHERE ad.date = rec.date
            AND ad.product_id::TEXT = rec.product_id
            AND ad.employee_id = rec.employee_id
			AND ad.store_id::TEXT = rec.store_id
			AND ad.customer_id = rec.customer_id
            AND ad.quantity = rec.quantity
			AND ad.stock = rec.stock
			and ad.price = rec.price
			AND ad.cost = rec.cost 
			AND ad.sales_channel = rec.sales_channel
		
		)
			
		THEN
            INSERT INTO BL_3NF.CE_SALES_SCD (
				date,
				product_id,
				employee_id,
				store_id,
				customer_id,
				quantity,
				stock,
				price,
				cost,
				sales_channel,
				start_dt,
				end_dt,
				is_active,
                insert_dt,
                source_id,
                source_entity,
                source_system
            )
            VALUES (
                rec.date,
				(SELECT product_id FROM BL_3NF.CE_PRODUCTS_SCD WHERE source_id = rec.product_id LIMIT 1),
				(SELECT employee_id FROM BL_3NF.CE_EMPLOYEES_SCD WHERE source_id = rec.employee_id::TEXT ),
                (SELECT store_id FROM BL_3NF.CE_STORES WHERE source_id = rec.store_id),
				(SELECT customer_id FROM BL_3NF.CE_CUSTOMERS_SCD WHERE source_id = rec.customer_id::TEXT),
                rec.quantity,
				rec.stock,
				rec.price,
				rec.cost,
				rec.sales_channel,
				current_date,
                '9999-12-31'::date,
				'Y',
				current_date,
				rec.invoice_number,
				'src_online_sales',
                'sa_online_sales'
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT insert_sales_online_sales();

