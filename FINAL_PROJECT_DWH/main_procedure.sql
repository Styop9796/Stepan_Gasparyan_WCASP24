
CREATE OR REPLACE PROCEDURE main_insert_procedures()
LANGUAGE plpgsql
AS $$
BEGIN
		   
		CALL public.create_foreign_tables_procedure();
		CALL public.create_src_tables_procedure();
		CALL sa_online_sales.insert_data_into_src_online_procedure();
		CALL sa_offline_sales.insert_data_into_src_offline_procedure();

		CALL BL_3NF.insert_default_rows_procedure();
		CALL public.create_3nf_and_tables_procedure();
		CALL BL_3NF.create_sequencies_for_3nf_tables_procedure();

		CALL BL_3NF.insert_ce_countries_procedure();
		CALL BL_3NF.insert_ce_cities_procedure();
		CALL BL_3NF.insert_ce_addreses_procedure();
		CALL BL_3NF.insert_ce_products_procedure();
		CALL BL_3NF.insert_ce_store_types_procedure();
		CALL BL_3NF.insert_ce_stores_procedure();
		CALL BL_3NF.insert_or_update_ce_customers_scd_procedure();
		CALL BL_3NF.insert_ce_employees_procedure_online();
		CALL BL_3NF.insert_ce_promo_type_1_procedure();
		CAll BL_3NF.insert_ce_promo_type_2_procedure();
		CAll BL_3NF.insert_ce_promo_bin_1_procedure();
		CALL BL_3NF.insert_fct_sales_procedure();

END;
$$;

-- Execute the main procedure
CALL main_insert_procedures();
