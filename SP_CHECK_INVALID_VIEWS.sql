CREATE OR REPLACE PROCEDURE "SP_CHECK_INVALID_VIEWS"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '  

	var loop_views = "select upper(''select 1 test_view from \\"''||table_catalog||''\\".\\"''||table_schema||''\\".\\"''||table_name||''\\" where 1=0 limit 0'') INVALID_VIEWS_CHECK from dwh_prod.INFORMATION_SCHEMA.views where table_catalog = ''DWH_PROD'' AND table_schema NOT IN (''INFORMATION_SCHEMA'') ORDER BY TABLE_CATALOG, table_schema, table_name";
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
	  report_return = "All OK.";
	}

	return report_return + "\\n" + "Invalid views count = " + count_err + ".";
  ';
