CREATE OR REPLACE PROCEDURE "SP_LOAD_META_DATA_SHOW"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
COMMENT='Metadata Snowflake snapshot'
EXECUTE AS CALLER
AS '  
	
	var show_text = "SHOW ROLES IN ACCOUNT";
	var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_ROLES COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_ROLES TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
	snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW USERS IN ACCOUNT";
	var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_USERS COPY GRANTS AS select A.*, CURRENT_TIMESTAMP() PPN_DATE from table(result_scan(last_query_id())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_USERS TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
	snowflake.execute( {sqlText: show_text} );
	snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW IMPORTED KEYS IN ACCOUNT";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_IMPORTED_KEYS COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_IMPORTED_KEYS TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW EXPORTED KEYS IN ACCOUNT";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
    
    var show_text = "SHOW IMPORTED KEYS IN SCHEMA DWH_PROD.APLSDB";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_IMPORTED_KEYS_DWH_PROD_APLSDB COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_IMPORTED_KEYS_DWH_PROD_APLSDB TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW EXPORTED KEYS IN SCHEMA DWH_PROD.APLSDB";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS_DWH_PROD_APLSDB COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS_DWH_PROD_APLSDB TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	/*Tale del narejen zato ker GETL daje vse kolone to UPPERCASE v DQju tukaj so pa kolone v SHOW z malimi in dejansko je case sensitive ker je z dvojnimi narekovaji*/
	var truncate_text = "TRUNCATE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS_DWH_PROD_APLSDB_DQ";
    var create_text = "INSERT INTO DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS_DWH_PROD_APLSDB_DQ SELECT \\"created_on\\",\\"pk_database_name\\",\\"pk_schema_name\\",\\"pk_table_name\\",\\"pk_column_name\\",\\"fk_database_name\\",\\"fk_schema_name\\",\\"fk_table_name\\",\\"fk_column_name\\",\\"key_sequence\\",\\"update_rule\\",\\"delete_rule\\",\\"fk_name\\",\\"pk_name\\",\\"deferrability\\",\\"rely\\",\\"comment\\",PPN_DATE FROM DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS_DWH_PROD_APLSDB";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_EXPORTED_KEYS_DWH_PROD_APLSDB_DQ TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
	var commit_text = "COMMIT";
    snowflake.execute( {sqlText: truncate_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	snowflake.execute( {sqlText: commit_text} );

	var show_text = "SHOW PRIMARY KEYS IN ACCOUNT";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_PRIMARY_KEYS COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_PRIMARY_KEYS TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW OBJECTS IN ACCOUNT";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_OBJECT COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_OBJECT TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW TABLES IN ACCOUNT";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_TABLES COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_TABLES TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW COLUMNS IN ACCOUNT";
    var create_text = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_COLUMNS COPY GRANTS AS SELECT A.*, CURRENT_TIMESTAMP() PPN_DATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) A";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_COLUMNS TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
    snowflake.execute( {sqlText: show_text} );
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	
	/*Brisemo vse kar je danasnji dan ce bi se slucajno se enkrat zagnalo da imamo samo zadnje stanje tega dne v tabeli in brisemo vse starejse od 6mesecov*/
	/*Zelja je da imamo po dnevih podatke kako se tabele veèajo dejansko te informacije nimamo v SnowFlake metadata zato bomo delali snapshot*/
	/*var delete_text = "DELETE FROM DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_TABLE_STORAGE_METRICS WHERE (PPN_DATE >= CURRENT_DATE OR PPN_DATE < ADD_MONTHS(CURRENT_DATE, -6))";*/
    var create_text = "INSERT INTO DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_TABLE_STORAGE_METRICS SELECT A.TABLE_CATALOG, A.TABLE_SCHEMA, A.TABLE_NAME, SUM(A.ACTIVE_BYTES) / pow(1024, 4) ACTIVE_TB, SUM(A.TIME_TRAVEL_BYTES) / pow(1024, 4) TIME_TB, SUM(A.FAILSAFE_BYTES) / pow(1024, 4) FAILSAFE_TB, SUM(A.ACTIVE_BYTES + A.TIME_TRAVEL_BYTES + A.FAILSAFE_BYTES) / pow(1024, 4) ALL_TB, MAX(A.TABLE_CREATED) TABLE_CREATED, MAX(A.TABLE_DROPPED) TABLE_DROPPED, CURRENT_TIMESTAMP() PPN_DATE FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS A GROUP BY A.TABLE_CATALOG, A.TABLE_SCHEMA, A.TABLE_NAME";
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_TABLE_STORAGE_METRICS TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
	var commit_text = "COMMIT";
	/*snowflake.execute( {sqlText: delete_text} );*/
    snowflake.execute( {sqlText: create_text} );
	snowflake.execute( {sqlText: owner_text} );
	snowflake.execute( {sqlText: commit_text} );
	
  return ''Complete'';
  ';
