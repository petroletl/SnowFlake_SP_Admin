CREATE OR REPLACE PROCEDURE "SP_CLUSTER_TO_SUSPEND"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '  

	var loop_tables = "SELECT ''ALTER TABLE '' || TABLE_CATALOG || ''.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME || '' SUSPEND RECLUSTER;'' SQL_TEXT FROM DWH_DEV.INFORMATION_SCHEMA.TABLES WHERE CLUSTERING_KEY IS NOT NULL AND AUTO_CLUSTERING_ON = ''YES'' UNION ALL SELECT ''ALTER TABLE '' || TABLE_CATALOG || ''.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME || '' SUSPEND RECLUSTER;'' SQL_TEXT FROM DWH_TEST.INFORMATION_SCHEMA.TABLES WHERE CLUSTERING_KEY IS NOT NULL AND AUTO_CLUSTERING_ON = ''YES'' UNION ALL SELECT ''ALTER TABLE '' || TABLE_CATALOG || ''.'' || TABLE_SCHEMA || ''.'' || TABLE_NAME || '' SUSPEND RECLUSTER;'' SQL_TEXT FROM DWH_BACKUP.INFORMATION_SCHEMA.TABLES WHERE CLUSTERING_KEY IS NOT NULL AND AUTO_CLUSTERING_ON = ''YES''";
    var result_set = snowflake.execute( {sqlText: loop_tables} );
	
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

	return report_return + "\\n" + "Tables err count = " + count_err + ".";
  ';
