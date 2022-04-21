CREATE OR REPLACE PROCEDURE "SP_CHECK_FK_DATA_INVALID"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '  

	var create_target_table = "CREATE OR REPLACE TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_FK_DATA_INVALID(COUNTER NUMBER,FK_DATABASE_NAME TEXT, FK_SCHEMA_NAME TEXT, FK_TABLE_NAME TEXT, FK_COLUMN_NAME TEXT, PK_DATABASE_NAME TEXT, PK_SCHEMA_NAME TEXT, PK_TABLE_NAME TEXT, PK_COLUMN_NAME TEXT, CREATION_DATE TIMESTAMP_NTZ(9)) COPY GRANTS";
	var result_set = snowflake.execute( {sqlText: create_target_table} );
	var owner_text = "GRANT OWNERSHIP ON TABLE DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_FK_DATA_INVALID TO ROLE ROLE_APLRUNS_DWH COPY CURRENT GRANTS";
	snowflake.execute( {sqlText: owner_text} );
	
	var show_text = "SHOW EXPORTED KEYS IN SCHEMA DWH_PROD.APLSDB";
	var loop_views = "SELECT ''INSERT INTO DWH_PROD.APLRUNS.METADATA_SNOWFLAKE_FK_DATA_INVALID SELECT COUNT(*) COUNTER, '''''' || \\"fk_database_name\\" || '''''' FK_DATABASE_NAME, '''''' || \\"fk_schema_name\\" || '''''' FK_SCHEMA_NAME, '''''' || \\"fk_table_name\\" || '''''' FK_TABLE_NAME, '''''' || \\"fk_column_name\\" || '''''' FK_COLUMN_NAME, '''''' || \\"pk_database_name\\" || '''''' PK_DATABASE_NAME, '''''' || \\"pk_schema_name\\" || '''''' PK_SCHEMA_NAME, '''''' || \\"pk_table_name\\" || '''''' PK_TABLE_NAME, '''''' || \\"pk_column_name\\" || '''''' PK_COLUMN_NAME, CURRENT_TIMESTAMP CREATION_DATE FROM '' || \\"fk_database_name\\" || ''.'' || \\"fk_schema_name\\" || ''.'' || \\"fk_table_name\\" || '' A WHERE NOT EXISTS (SELECT 1 FROM '' || \\"pk_database_name\\" || ''.'' || \\"pk_schema_name\\" || ''.'' || \\"pk_table_name\\" || '' B WHERE B.'' || \\"pk_column_name\\" ||'' = A.'' || \\"fk_column_name\\" || '');'' SQL_TEXT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE \\"pk_database_name\\" = ''DWH_PROD'' AND \\"fk_database_name\\" = ''DWH_PROD'' ORDER BY 1";
	
	snowflake.execute( {sqlText: show_text} );
    var result_set = snowflake.execute( {sqlText: loop_views} );
	
	// Loop through the results, processing one row at a time... 
	var result_sql = "";
	var report_return = "";
	var count_err = 0;
	while (result_set.next())  {
		result_sql = result_set.getColumnValue(1);
		
		try {
        	snowflake.execute( {sqlText: result_sql} )
        }
    	catch (err)  {
			count_err++;
        	report_return = report_return + "Failed: " + result_sql + " with error:" + err + "\\n" + "\\n";   
        }
	}

	if (count_err == 0) {
	  report_return = "Done.";
	}

	return report_return + "\\n" + "Db errors count = " + count_err + ".";
  ';
