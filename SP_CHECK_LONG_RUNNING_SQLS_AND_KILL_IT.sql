CREATE OR REPLACE PROCEDURE "SP_CHECK_LONG_RUNNING_SQLS_AND_KILL_IT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '  

	var loop_views = "SELECT ''select system$cancel_query('''''' || A.query_id || '''''')'' SQL_TEXT" + "\\n";
          loop_views = loop_views + "FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.query_history()) A" + "\\n";
          loop_views = loop_views + "WHERE A.execution_status = ''RUNNING''" + "\\n"; 
          loop_views = loop_views + "AND datediff(MIN,start_time,CURRENT_TIMESTAMP) >= 90" + "\\n";
          loop_views = loop_views + "AND a.user_name NOT IN (''APLLOAD'',''APLRUNS'',''APLSDB'',''APLTRADEDEV'',''APLTRADEPROD'',''APLTRADETEST'',''DSETL'',''FIRST_USER'',''PC_FIVETRAN_USER'',''TRADEDEVETL'',''TRADEPRODETL'',''TRADETESTETL'',''SYSTEM'')" + "\\n";

    var result_set = snowflake.execute( {sqlText: loop_views} );
	
	// Loop through the results, processing one row at a time... 
	var result_sql = "";
	var report_return = "";
	var count_err = 0;
	while (result_set.next())  {
		result_sql = result_set.getColumnValue(1);
		
		try {
        	snowflake.execute( {sqlText: result_sql} )
            count_err++;
        }
    	catch (err)  {
        	report_return = report_return + "Failed: " + result_sql + " with error:" + err + "\\n" + "\\n";   
        }
	}

	if (count_err == 0) {
	  report_return = "All OK.";
	}

	return report_return + "\\n" + "Killed SQLs count = " + count_err + ".";
  ';
