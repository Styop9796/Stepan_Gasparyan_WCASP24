-- Insert records into ce_sales in BL_3NF layer 

CREATE OR REPLACE PROCEDURE BL_3NF.insert_online_sales_into_3nf_procedure()
AS $$
DECLARE
    rec RECORD;
    target_sale_id INT;
	inserted_count INT := 0;

BEGIN
    FOR rec IN 
        SELECT invoice_number, date,product_id,employee_id,store_id,customer_id,quantity,stock,price,cost,sales_channel,promo_type_1,promo_bin_1
        FROM sa_online_sales.src_online_sales
    LOOP
        SELECT sale_id INTO target_sale_id
        FROM BL_3NF.CE_SALES ci
        WHERE ci.source_id = rec.invoice_number;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_SALES ad
            WHERE ad.source_id = rec.invoice_number
		)
			
		THEN
            INSERT INTO BL_3NF.CE_SALES (
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
				sales_channel,
				promo_type_1,
				promo_bin_1,
				promo_type_2,
                insert_dt,
                source_id,
                source_entity,
                source_system
            )
            VALUES (
				nextval('BL_3NF.fct_sales_id_sec'),
                rec.date,
				(SELECT product_id FROM BL_3NF.CE_PRODUCTS WHERE source_id = rec.product_id LIMIT 1),
				(SELECT employee_id FROM BL_3NF.CE_EMPLOYEES WHERE source_id = rec.employee_id::TEXT ),
                (SELECT store_id FROM BL_3NF.CE_STORES WHERE source_id = rec.store_id),
				(SELECT customer_id FROM BL_3NF.CE_CUSTOMERS_SCD WHERE source_id = rec.customer_id::TEXT AND is_active='Y'),
                rec.quantity,
				rec.stock,
				rec.price,
				rec.cost,
				rec.sales_channel,
				rec.promo_type_1,
				rec.promo_bin_1,
				'n.a.',
				current_date,
				rec.invoice_number,
				'src_online_sales',
                'sa_online_sales'
            );
				inserted_count := inserted_count + 1;

        END IF;
    END LOOP;
	
	INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_online_sales_into_3nf_procedure', inserted_count);
	
END;
$$ LANGUAGE plpgsql;




CREATE OR REPLACE PROCEDURE BL_3NF.insert_offline_sales_into_3nf_procedure()
AS $$
DECLARE
    rec RECORD;
    target_sale_id INT;
	inserted_count INT := 0;

BEGIN
    FOR rec IN 
        SELECT invoice_number, date,product_id,employee_id,store_id,customer_id,quantity,stock,price,cost,sales_channel,promo_type_1,promo_bin_1,
				promo_type_2
        FROM sa_offline_sales.src_offline_sales
    LOOP
        SELECT sale_id INTO target_sale_id
        FROM BL_3NF.CE_SALES ci
        WHERE ci.source_id = rec.invoice_number;

        -- Check for existence in the target table
        IF NOT EXISTS (
            SELECT 1
            FROM BL_3NF.CE_SALES ad
            WHERE ad.source_id = rec.invoice_number
		
		)
			
		THEN
            INSERT INTO BL_3NF.CE_SALES (
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
				sales_channel,
				promo_type_1,
				promo_bin_1,
				promo_type_2,
                insert_dt,
                source_id,
                source_entity,
                source_system
            )
            VALUES (
				nextval('BL_3NF.fct_sales_id_sec'),
                rec.date::DATE,
				(SELECT product_id FROM BL_3NF.CE_PRODUCTS WHERE source_id = rec.product_id LIMIT 1),
				(SELECT employee_id FROM BL_3NF.CE_EMPLOYEES WHERE source_id = rec.employee_id::TEXT ),
                (SELECT store_id FROM BL_3NF.CE_STORES WHERE source_id = rec.store_id),
				(SELECT customer_id FROM BL_3NF.CE_CUSTOMERS_SCD WHERE source_id = rec.customer_id::TEXT AND is_active='Y'),
                rec.quantity::INT,
				rec.stock::INT,
				rec.price::NUMERIC,
				rec.cost::NUMERIC,
				rec.sales_channel,
				rec.promo_type_1,
				rec.promo_bin_1,
				rec.promo_type_2,
				current_date,
				rec.invoice_number,
				'src_offline_sales',
                'sa_ffline_sales'
            );
			inserted_count := inserted_count + 1;
        END IF;
    END LOOP;
	
	INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_offline_sales_into_3nf_procedure', inserted_count);
	
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE PROCEDURE BL_3NF.insert_data_info_ce_sales()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL BL_3NF.insert_offline_sales_into_3nf_procedure();
	CALL BL_3NF.insert_online_sales_into_3nf_procedure();
END;
$$;




--FCT_SALES_DD


CREATE OR REPLACE PROCEDURE BL_DM.insert_FCT_SALES_DD_FROM_3NF_procedure()
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
        RETURNING 1
    )
    SELECT COUNT(*) INTO inserted_count FROM inserted;

    -- Log the number of inserted rows
    INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_FCT_SALES_DD_FROM_3NF_procedure', inserted_count);

    RAISE NOTICE 'Inserted rows: %', inserted_count;

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_FCT_SALES_DD_FROM_3NF_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;



CREATE OR REPLACE PROCEDURE BL_3NF.MAIN_PROCEDURE()
LANGUAGE plpgsql
AS $$
BEGIN
	CALL BL_3NF.insert_data_info_ce_sales();
	CALL BL_DM.insert_FCT_SALES_DD_FROM_3NF_procedure(); 
	
	
EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('MAIN_PROCEDURE', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'MAIN PROCEDURE FAILED';
END;
$$;


CALL BL_3NF.MAIN_PROCEDURE()