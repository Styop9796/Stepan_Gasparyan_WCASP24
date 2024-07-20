CREATE SCHEMA IF NOT EXISTS BL_DM;



CREATE TABLE IF NOT EXISTS BL_DM.DIM_DATE(
	event_date_surr_id 	DATE PRIMARY KEY,
	day_of_week 		INT NOT NULL,
	day_of_month 		INT NOT NULL,
	day_of_year 		INT NOT NULL,
	week_of_year 		INT NOT NULL,
	month 				INT NOT NULL,
	quarter 			INT NOT NULL,
	yaer 				INT NOT NULL,
	source_id 			DATE NOT NULL,
	source_entity 		VARCHAR(100) NOT NULL,
	source_system  		VARCHAR(100) NOT NULL

);

CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_DATE_seq;

INSERT INTO BL_DM.DIM_DATE(event_date_surr_id, day_of_week, day_of_month, day_of_year, week_of_year, month, 
						   quarter, yaer, source_id, source_entity, source_system)
SELECT '1900-01-01'::DATE, -1, -1, -1, -1, -1, -1, -1, '1900-01-01'::DATE, 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_DATE WHERE event_date_surr_id = '1900-01-01');



CREATE TABLE IF NOT EXISTS BL_DM.DIM_PRODUCTS(
	
	product_surr_id 	BIGINT PRIMARY KEY ,
	product_name	 	VARCHAR(100) NOT NULL,
	product_length   	DECIMAL(5,2) NOT NULL,
	product_depth	   	DECIMAL(5,2) NOT NULL,
	product_width	   	DECIMAL(5,2) NOT NULL,
	product_cost	   	DECIMAL(5,2) NOT NULL,
	product_price	   	DECIMAL(5,2) NOT NULL,
	product_stock	   	INT  NOT NULL,
	hierarchy1_id     	VARCHAR(30) NOT NULL,
	hierarchy2_id	   	VARCHAR(30) NOT NULL,
	source_id          	INT NOT NULL,
	source_system  		VARCHAR(100) NOT NULL,
	source_entity     	VARCHAR(100) NOT NULL
	
);

INSERT INTO BL_DM.DIM_PRODUCTS(product_surr_id,product_name,product_length,product_depth,product_width,product_cost,product_price,product_stock,
							   hierarchy1_id,hierarchy2_id,
						       source_id,source_system,source_entity)
SELECT -1,'n.a.',-1,-1,-1,-1,-1,-1,'n.a.','n.a.',-1,'BL_3NF','CE_PRODUCTS'
WHERE NOT EXISTS(SELECT 1 FROM BL_DM.DIM_PRODUCTS WHERE product_surr_id = -1 );


CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_PRODUCTS_seq;




CREATE TABLE IF NOT EXISTS BL_DM.DIM_EMPLOYEES(
	employee_surr_id		BIGINT PRIMARY KEY,
	employee_name  			VARCHAR(50) NOT NULL,
	employee_last_name 		VARCHAR(50) NOT NULL,
	employee_email	 		VARCHAR(100) NOT NULL,
	source_id				INT NOT NULL,
	source_system			VARCHAR(100) NOT NULL,
	source_entity			VARCHAR(100) NOT NULL
);

INSERT INTO BL_DM.DIM_EMPLOYEES(employee_surr_id,employee_name,employee_last_name,employee_email,source_id,source_system,source_entity)
SELECT -1,'n.a.','n.a.','n.a.',-1,'BL_3NF','CE_EMPLOYEES'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.DIM_EMPLOYEES WHERE employee_surr_id = -1);


CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_EMPLOYEES_seq;





CREATE TABLE IF NOT EXISTS BL_DM.DIM_PROMO_TYPE_1(
	promo_type1_surr_id    BIGINT PRIMARY KEY,
	promo_type1     	   VARCHAR(100) NOT NULL,
	source_id			   INT NOT NULL,
	source_system		   VARCHAR(100) NOT NULL,
	source_entity		   VARCHAR(100) NOT NULL
);

INSERT INTO BL_DM.DIM_PROMO_TYPE_1(promo_type1_surr_id,promo_type1,source_id,source_system,source_entity)
SELECT -1,'n.a.',-1,'BL_3NF','CE_PROMO_TYPE_1'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.DIM_PROMO_TYPE_1 WHERE promo_type1_surr_id = -1);



CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_PROMO_TYPE_1_seq;




CREATE TABLE IF NOT EXISTS BL_DM.DIM_PROMO_BIN_1(
	promo_bin_surr_id    BIGINT PRIMARY KEY,
	promo_bin1     	VARCHAR(100) NOT NULL,
	source_id			INT NOT NULL,
	source_system		VARCHAR(100) NOT NULL,
	source_entity		VARCHAR(100) NOT NULL
);

INSERT INTO BL_DM.DIM_PROMO_BIN_1(promo_bin_surr_id,promo_bin1,source_id,source_system,source_entity)
SELECT -1,'n.a.',-1,'BL_3NF','CE_PROMO_BIN_1'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.DIM_PROMO_BIN_1 WHERE promo_bin_surr_id = -1);



CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_PROMO_BIN_1_seq;




CREATE TABLE IF NOT EXISTS BL_DM.DIM_PROMO_TYPE_2(
	promo_type2surr_id    BIGINT PRIMARY KEY,
	promo_type2    		  VARCHAR(100) NOT NULL,
	source_id		      INT NOT NULL,
	source_system	      VARCHAR(100) NOT NULL,
	source_entity		  VARCHAR(100) NOT NULL
);

INSERT INTO BL_DM.DIM_PROMO_TYPE_2(promo_type2surr_id,promo_type2,source_id,source_system,source_entity)
SELECT -1,'n.a.',-1,'BL_3NF','CE_PROMO_TYPE_2'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.DIM_PROMO_TYPE_2 WHERE promo_type2surr_id = -1);



CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_PROMO_TYPE_2_seq;



CREATE TABLE IF NOT EXISTS BL_DM.DIM_STORES(
	store_surr_id 		INT PRIMARY KEY ,
	store_name			VARCHAR(100) NOT NULL,
	store_size			INT NOT NULL ,
	shop_website	 	VARCHAR(100) NOT NULL,
	storetype_id		BIGINT NOT NULL ,
	storetype_name		VARCHAR(50) NOT NULL,
	store_address_id	BIGINT NOT NULL,
	store_address	    VARCHAR(100) NOT NULL,
	store_state         VARCHAR (50) NOT NULL ,
	city_id				BIGINT NOT NULL,
	city_name			VARCHAR(100) NOT NULL,
	country_id			BIGINT NOT NULL,
	country_name	   	VARCHAR(100) NOT NULL,
	source_id           VARCHAR(100) NOT NULL,
	source_system    	VARCHAR(100) NOT NULL,
	source_entity       VARCHAR(100) NOT NULL
	

); 

INSERT INTO BL_DM.DIM_STORES(store_surr_id,store_name,store_size,shop_website,storetype_id,storetype_name,store_address_id,store_address,
							 store_state,city_id,city_name,country_id,country_name,source_id,source_system,source_entity)
SELECT -1,'n.a.',-1,'n.a.',-1,'n.a.',-1,'n.a.','n.a.',-1,'n.a.',-1,'n.a.',-1,'BL_3NF','CE_STORES'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.DIM_STORES WHERE store_surr_id = -1);




CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_STORES_seq;


CREATE TABLE IF NOT EXISTS BL_DM.DIM_CUSTOMERS_SCD(
	customer_surr_id 	BIGINT PRIMARY KEY,
	f_name				VARCHAR(50) NOT NULL,
	l_name			VARCHAR(50) NOT NULL,
	email				VARCHAR(100) NOT NULL,
	cust_phone			VARCHAR(30) NOT NULL,
	start_dt			DATE	NOT NULL,
	end_dt				DATE	NOT NULL,
	is_active		    VARCHAR(10) NOT NULL,
	insert_dt			DATE	NOT NULL,
	source_id	   		VARCHAR(100) NOT NULL,
	source_system   	VARCHAR(100) NOT NULL,
	source_entity	    VARCHAR(100) NOT NULL
	
);   

INSERT INTO BL_DM.DIM_CUSTOMERS_SCD(customer_surr_id,f_name,l_name,email,cust_phone,start_dt,end_dt,
									is_active,insert_dt,source_id,source_system,
									source_entity)
SELECT -1,'n.a.','n.a.','n.a.','n.a.','1-1-1900'::DATE,'9999-12-31'::DATE,'Y','1-1-1900'::DATE,-1,'BL_3NF','CE_CUSTOMERS_SCD'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.DIM_CUSTOMERS_SCD WHERE customer_surr_id = -1);




CREATE SEQUENCE IF NOT EXISTS BL_DM.DIM_CUSTOMERS_SCD_seq;



CREATE TABLE IF NOT EXISTS BL_DM.FCT_SALES_DD(
	sales_surr_id			BIGINT PRIMARY KEY ,
	event_date_surr_id		DATE NOT NULL ,
	product_surr_id		    BIGINT NOT NULL,
	employee_surr_id		BIGINT NOT NULL,
	store_surr_id			INT NOT NULL,
	customer_surr_id		BIGINT NOT NULL,
	quantity				INT NOT NULL,
	stock			        INT NOT NULL,
	price	 				DECIMAL(10,2) NOT NULL,
	cost					DECIMAL(10,2) NOT NULL,
	sales_channel	        VARCHAR(50) NOT NULL,
	source_id	   			VARCHAR(100) NOT NULL,
	source_system   		VARCHAR(100) NOT NULL,
	source_entity	   		VARCHAR(100) NOT NULL
);


CREATE SEQUENCE IF NOT EXISTS BL_DM.FCT_SALES_DD_seq;



INSERT INTO BL_DM.FCT_SALES_DD(sales_surr_id,event_date_surr_id,product_surr_id,
							   employee_surr_id,store_surr_id,
							   customer_surr_id,quantity,
							   stock,price,cost,sales_channel,
							   source_id,source_system,source_entity)
SELECT -1,'1-1-1900'::DATE,-1,-1,-1,-1,-1,-1,-1,-1,'n.a',-1,'BL_3NF','CE_SALES'
WHERE NOT EXISTS ( SELECT 1 FROM BL_DM.FCT_SALES_DD WHERE sales_surr_id = -1);





