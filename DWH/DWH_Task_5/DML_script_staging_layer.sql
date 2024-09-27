
INSERT INTO sa_offline_sales.src_offline_sales(
	invoice_number, date, quantity, stock, price, 
											   cost, promo_type_1, promo_bin_1, promo_type_2, 
											   product_id, product_name, product_length, 
											   product_depth, product_width, hierarchy1_id, 
											   hierarchy2_id, sales_channel, store_id,
											   store_name, storetype_id, storetype_name, 
											   store_size, city_id, city_name, customer_id, 
											   f_name, l_name, email, cust_phone, store_state,
											   country_id, country_name, store_address,
											   product_price, product_cost, product_stock)
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
	  product_stock
FROM sa_offline_sales.ext_offline_sales
;
 
 

INSERT INTO sa_online_sales.src_online_sales (invoice_number,date,store_id,shop_website,product_id,
											  product_name,quantity,stock,price,cost,promo_type_1,
  											  promo_bin_1,product_length,product_depth,product_width,
  											  employee_id,employee_name,hierarchy1_id,hierarchy2_id,
   											  sales_channel,customer_id,f_name,l_name,email,
											  employee_last_name,employee_email,product_price,product_cost,
											  product_stock
)
SELECT
    invoice_number,
    date,
    store_id,
    shop_website,
    product_id,
    product_name,
    quantity,
    stock,
    price,
    cost,
    promo_type_1,
    promo_bin_1,
    product_length,
    product_depth,
    product_width,
    employee_id,
    employee_name,
    hierarchy1_id,
    hierarchy2_id,
    sales_channel,
    customer_id,
    f_name,
    l_name,
    email,
    employee_last_name,
    employee_email,
    product_price,
    product_cost,
    product_stock
FROM sa_online_sales.ext_online_sales
;


SELECT * FROM sa_offline_sales.src_offline_sales;
SELECT * FROM sa_online_sales.src_online_sales;




