CREATE OR REPLACE PROCEDURE "SP_BACKUP_DWH_PROD"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
      var result = "Succeeded";
      try {
		  
		  var my_sql_command_databases = "SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME LIKE ''DWH_PROD_%'' AND CREATED < CURRENT_DATE - 5";
		  var statement1 = snowflake.createStatement( {sqlText: my_sql_command_databases} );
		  var result_set1 = statement1.execute();
		  
		  // Loop through the results, processing one row at a time... 
		  while (result_set1.next())  {
				var column1 = result_set1.getColumnValue(1);
				var my_sql_command_drop				= "drop database " + column1;
				var statement_drop = snowflake.createStatement( {sqlText: my_sql_command_drop} );
				statement_drop.execute();
			}
		  
		  let current_datetime = new Date();
		  let formatted_date = current_datetime.getFullYear() + "_" + (current_datetime.getMonth() + 1) + "_" + current_datetime.getDate() + "_" + current_datetime.getHours() + "_" + current_datetime.getMinutes() + "_" + current_datetime.getSeconds();		  
		  
		  var my_sql_command1 = "CREATE DATABASE DWH_PROD_" +formatted_date+ " CLONE DWH_PROD COMMENT = ''Production DW''";
		  var statement_clone = snowflake.createStatement( {sqlText: my_sql_command1} );
		  result = my_sql_command1;
		  statement_clone.execute();
		  
          }
      catch (err)  {
          result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
          result += "\\n  Message: " + err.message;
          result += "\\nStack Trace:\\n" + err.stackTraceTxt; 
          }
      return result;
      ';
