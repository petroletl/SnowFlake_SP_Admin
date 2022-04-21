CREATE OR REPLACE PROCEDURE "SP_CREATE_DWH_PROD_2_DWH_TEST"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
      var result = "Started";
      try {
		  
		  var my_sql_command_databases = "SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME LIKE ''DWH_TEST_%'' AND CREATED < CURRENT_DATE - 5";
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
		  
		  var my_sql_command = "CREATE DATABASE DWH_TEST_NEW CLONE DWH_PROD COMMENT = ''Test DW'';";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "REVOKE USAGE ON DATABASE DWH_TEST FROM ROLE ROLE_DWH_TEST;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  //var my_sql_command = "ALTER DATABASE IF EXISTS DWH_TEST RENAME TO DWH_TEST_OLD;";
		  //result = my_sql_command;
		  //var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  //statement_sql.execute();
		  var my_sql_command = "ALTER DATABASE IF EXISTS DWH_TEST SET DATA_RETENTION_TIME_IN_DAYS = 1;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  var my_sql_command = "ALTER DATABASE IF EXISTS DWH_TEST RENAME TO DWH_TEST_" + formatted_date + ";";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "ALTER DATABASE IF EXISTS DWH_TEST_NEW RENAME TO DWH_TEST;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  var my_sql_command = "TRUNCATE TABLE DWH_TEST.APLRUNS.JOB_LOAD_ENGINES;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (1, 7, ''DWH_TEST_XS_ETL'', ''Default engine'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (2, 7, ''DWH_TEST_XS_ETL'', ''Snowflake extra small'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (3, 7, ''DWH_TEST_S_ETL'', ''Snowflake small'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (4, 7, ''DWH_TEST_M_ETL'', ''Snowflake medium'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (5, 7, ''DWH_TEST_L_ETL'', ''Snowflake large'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (6, 7, ''DWH_TEST_XL_ETL'', ''Snowflake extra large'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (7, 7, ''DWH_TEST_2XL_ETL'', ''Snowflake extra 2 large'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (8, 7, ''DWH_TEST_3XL_ETL'', ''Snowflake extra 3 large'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "INSERT INTO  DWH_TEST.APLRUNS.JOB_LOAD_ENGINES VALUES (9, 7, ''DWH_TEST_4XL_ETL'', ''Snowflake extra 4 large'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLRUNS.ALL_DATABASES SET DEFAULT_DATABASE = ''DWH_TEST'' WHERE DEFAULT_DATABASE IN (''DWH_DEV'', ''DWH_PROD'');";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLRUNS.ADM_EX_IN A SET A.TGT_TABLE_DB = ''DWH_TEST'';";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLRUNS.ALL_TABLES_EXTRACT A SET A.EX_DATABASE_NAME = ''DWH_TEST'', A.ST_DATABASE_NAME = ''DWH_TEST'';";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLRUNS.ADM_PARAM SET PARAM_VALUE = ''PETROL_ETL_TEST'' WHERE PARAM_GROUP = ''NOTIFICATION'' AND PARAM_NAME = ''ENV'' AND ACTIVE = 1;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  var my_sql_command = "ALTER DATABASE IF EXISTS DWH_TEST SET DATA_RETENTION_TIME_IN_DAYS = 14 COMMENT = ''Test DW'';";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  var my_sql_command = "UPDATE DWH_TEST.APLSDB.D_CUSTOMER_SD A SET A.ACCOUNT_NAME = NULL, A.FIRST_NAME = NULL, A.LAST_NAME = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLSDB.D_EM_USER_SD A SET A.FIRST_NAME = NULL, A.LAST_NAME = NULL, A.VAT_NUM = NULL, A.TELEPHONE_NUMBER = NULL, A.GSM_NUMBER = NULL, A.EMAIL = NULL, A.STREET_DESCRIPTION = NULL, A.HOUSE_NO_DESCRIPTION = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  		
		  var my_sql_command = "UPDATE DWH_TEST.APLSDB.D_CUSTOMER_CONTACT B SET B.LAST_NAME = NULL, B.FIRST_NAME = NULL, B.CONTACT = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLSDB.DM_CUSTOMER_SD A SET A.NAME = NULL, A.SURNAME = NULL, A.VAT = NULL, STREET = NULL, HOUSE_NUM = NULL, ZIP_CODE = NULL, POST = NULL, MUNICIPALITY = NULL, REGION = NULL, EMAIL = NULL, PHONE = NULL, MOBILE_PHONE = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLSDB.D_EMPLOYEE_SD A SET A.FIRST_NAME = NULL, A.LAST_NAME = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_DB2PE_PROD_ORGANIZACIJA C SET D057EPO = NULL, D057NAZ = NULL, D057EMS = NULL, D057TEL = NULL, D057PC = NULL, D057GSM = NULL, D057PN1 = NULL, D057PN2 = NULL, D057IME = NULL, D057PRI = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_DB2PE_PROD_ORGANIZACIJA D SET D057NAZ = NULL, D057EMS = NULL, D057TEL = NULL, D057PC = NULL, D057GSM = NULL, D057PN1 = NULL, D057PN2 = NULL, D057IME = NULL, D057PRI = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_SAP_SAPABAP1_FKKVKP A SET A.VKBEZ = NULL;";		  
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_DB2PE_PROD_POTROSNIK_TELEFON E SET DA24TEL = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_DB2PE_PROD_POTROSNIK_TELEFON F SET DA24TEL = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_SAP_SAPABAP1_ZIBP01 G SET FIRSTNAME = NULL, LASTNAME = NULL, BUSINESSPARTNERNAME = NULL, BUSINESSPARTNERFULLNAME = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_SAP_SAPABAP1_ZIBP01 G SET FIRSTNAME = NULL, LASTNAME = NULL, BUSINESSPARTNERNAME = NULL, BUSINESSPARTNERFULLNAME = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_SAP_SAPABAP1_IBUPATAXNUMBERTP H SET BPTAXNUMBER = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_SAP_SAPABAP1_IBUPATAXNUMBERTP I SET BPTAXNUMBER = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_SF_ACCOUNT J SET NAME = NULL, LAST_NAME_C = NULL, FIRST_NAME_C = NULL, EMAIL_C = NULL, SECONDARY_EMAIL_C = NULL, PRIMARY_MOBILE_C = NULL, OTHER_MOBILE_C = NULL, WORK_MOBILE_C = NULL, PHONE = NULL, OTHER_PHONE_C = NULL, WORK_PHONE_C = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_SF_ACCOUNT J SET NAME = NULL, LAST_NAME_C = NULL, FIRST_NAME_C = NULL, EMAIL_C = NULL, SECONDARY_EMAIL_C = NULL, PRIMARY_MOBILE_C = NULL, OTHER_MOBILE_C = NULL, WORK_MOBILE_C = NULL, PHONE = NULL, OTHER_PHONE_C = NULL, WORK_PHONE_C = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_SF_CONTACT K SET LAST_NAME = NULL, FIRST_NAME = NULL, NAME = NULL, EMAIL = NULL, SECONDARY_EMAIL_C = NULL, MOBILE_PHONE = NULL, PRIMARY_MOBILE_C = NULL, OTHER_MOBILE_C = NULL, WORK_MOBILE_C = NULL, PHONE = NULL, OTHER_PHONE_C = NULL, WORK_PHONE_C = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_SF_CONTACT K SET LAST_NAME = NULL, FIRST_NAME = NULL, NAME = NULL, EMAIL = NULL, SECONDARY_EMAIL_C = NULL, MOBILE_PHONE = NULL, PRIMARY_MOBILE_C = NULL, OTHER_MOBILE_C = NULL, WORK_MOBILE_C = NULL, PHONE = NULL, OTHER_PHONE_C = NULL, WORK_PHONE_C = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.ST_SF_INDIVIDUAL L SET LAST_NAME = NULL,FIRST_NAME = NULL,NAME = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();
		  
		  var my_sql_command = "UPDATE DWH_TEST.APLLOAD.EX_SF_INDIVIDUAL L SET LAST_NAME = NULL, FIRST_NAME = NULL, NAME = NULL;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  var my_sql_command = "GRANT USAGE ON DATABASE DWH_TEST TO ROLE ROLE_DWH_TEST;";
		  result = my_sql_command;
		  var statement_sql = snowflake.createStatement( {sqlText: my_sql_command} )
		  statement_sql.execute();

		  result = "Succeeded";
		  return result;
		  
          }
      catch (err)  {
          result +=  " Failed: Code: " + err.code + "\\n  State: " + err.state;
          result += "\\n  Message: " + err.message;
          result += "\\nStack Trace:\\n" + err.stackTraceTxt; 
          }
      return result;
      ';
