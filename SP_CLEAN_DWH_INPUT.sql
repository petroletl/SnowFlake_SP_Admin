CREATE OR REPLACE PROCEDURE "SP_CLEAN_DWH_INPUT"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
      var result = "Succeeded";
      try {
		  
		  var my_sql_command_databases = "SELECT TABLE_CATALOG ||''.''|| TABLE_SCHEMA ||''.''|| TABLE_NAME TABLE_NAME FROM DWH_INPUT.INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG LIKE ''DWH_INPUT'' AND TABLE_SCHEMA IN (''SCHEMA_BI'', ''SCHEMA_MA'', ''SCHEMA_TRADING'', ''SCHEMA_CO'') AND CREATED < CURRENT_DATE - 30 AND TABLE_TYPE = ''BASE TABLE''";
		  var statement1 = snowflake.createStatement( {sqlText: my_sql_command_databases} );
		  var result_set1 = statement1.execute();
		  result = result_set1;

		  // Loop through the results, processing one row at a time... 
		  while (result_set1.next())  {
				var column1 = result_set1.getColumnValue(1);
				var my_sql_command_drop				= "drop table " + column1;
				var statement_drop = snowflake.createStatement( {sqlText: my_sql_command_drop} );
				statement_drop.execute();
			}
          }
      catch (err)  {
          result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
          result += "\\n  Message: " + err.message;
          result += "\\nStack Trace:\\n" + err.stackTraceTxt; 
          }
      return result;
      ';
