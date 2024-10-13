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





CREATE OR REPLACE PROCEDURE sa_online_sales.insert_data_into_src_online_procedure()
LANGUAGE plpgsql
AS $$
DECLARE 
	inserted_count INTEGER;
BEGIN
	WITH inserted AS (INSERT INTO sa_offline_sales.src_offline_sales(
	invoice_number,employee_id, date, quantity, stock, price, 
	   cost, promo_type_1, promo_bin_1, promo_type_2, 
	   product_id, product_name, product_length, 
	   product_depth, product_width, hierarchy1_id, 
	   hierarchy2_id, sales_channel, store_id,
	   store_name, storetype_id, storetype_name, 
	   store_size, city_id, city_name, customer_id, 
	   f_name, l_name, email, cust_phone, store_state,
	   country_id, country_name, store_address,
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
	  promo_type_2 ,
	  product_id ,
	  product_name ,
	  product_length ,
	  product_depth ,
	  product_width ,
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
WHERE NOT EXISTS( SELECT 1 FROM sa_offline_sales.src_offline_sales src WHERE src.invoice_number = s.invoice_number) 
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
	WITH inserted AS ( 
		INSERT INTO sa_offline_sales.src_offline_sales(
	invoice_number,employee_id, date, quantity, stock, price, 
	   cost, promo_type_1, promo_bin_1, promo_type_2, 
	   product_id, product_name, product_length, 
	   product_depth, product_width, hierarchy1_id, 
	   hierarchy2_id, sales_channel, store_id,
	   store_name, storetype_id, storetype_name, 
	   store_size, city_id, city_name, customer_id, 
	   f_name, l_name, email, cust_phone, store_state,
	   country_id, country_name, store_address,
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
	  promo_type_2 ,
	  product_id ,
	  product_name ,
	  product_length ,
	  product_depth ,
	  product_width ,
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
	WHERE NOT EXISTS( SELECT 1 FROM sa_offline_sales.src_offline_sales src WHERE src.invoice_number = s.invoice_number)
	RETURNING 1)
					  
	SELECT COUNT(*) INTO inserted_count FROM inserted;
	
	INSERT INTO logging (procedure_name, rows_affected)
    VALUES ('insert_data_into_src_offline_procedure', inserted_count);
	



    RAISE NOTICE 'Inserted rows into src_offline_sales : %', inserted_count;
EXCEPTION
    WHEN OTHERS THEN
   
        -- Log any errors that occur
        INSERT INTO logging (procedure_name, rows_affected)
        VALUES ('insert_data_into_src_offline_procedure', -1);

        -- Raise the exception to propagate the error
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END;
$$;


CREATE INDEX IF NOT EXISTS inv_num_ind_for_offline ON sa_offline_sales.src_offline_sales(invoice_number);
CREATE INDEX IF NOT EXISTS inv_num_ind_for_online ON sa_online_sales.src_online_sales(invoice_number);



CALL sa_online_sales.insert_data_into_src_online_procedure();
CALL sa_offline_sales.insert_data_into_src_offline_procedure();



