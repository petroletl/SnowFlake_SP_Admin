CREATE OR REPLACE PROCEDURE "SP_KILL_USER_SQLS"("V_USER_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
      var result = "Succeeded";
      try {
		  
		  var my_sql_command_databases = "ALTER USER "+V_USER_NAME+" ABORT ALL QUERIES";
		  var statement1 = snowflake.createStatement( {sqlText: my_sql_command_databases} );
		  var result_set1 = statement1.execute();
		  result = result_set1;
		  
          }
      catch (err)  {
          result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
          result += "\\n  Message: " + err.message;
          result += "\\nStack Trace:\\n" + err.stackTraceTxt; 
          }
      return result;
      ';
