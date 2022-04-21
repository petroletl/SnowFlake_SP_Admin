CREATE OR REPLACE PROCEDURE "SP_GRANT_RULES"("P_DATABASE" VARCHAR(16777216), "P_SCHEMA_NAME" VARCHAR(16777216), "P_EXACT_TABLE_VIEW_ALL" VARCHAR(16777216), "P_ROLE_NAME" VARCHAR(16777216), "P_ROLE_CURRENT" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '  
    
	//Sample:CALL SNOWFLAKE_ADMIN.PUBLIC.SP_GRANT_RULES(DWH_PROD, APLLOAD, NULL, NULL, CURRENT_ROLE());
    
    var v_database_name = P_DATABASE ?? "Default";
    var v_schema_name = P_SCHEMA_NAME ?? "Default";
    var v_role_name = P_ROLE_NAME ?? "Default";
    var v_table_name = P_EXACT_TABLE_VIEW_ALL ?? "Default";
    var v_role_current = P_ROLE_CURRENT;
    
    //IF role_current is role_admin or role_current is not ACCOUNTADMIN then is GO!
    if (v_role_current == "ROLE_ADMIN_DWH" || v_role_current == "ACCOUNTADMIN") {
    
          var loop_views = "SELECT SQL_TEXT,GRANT_REVOKE,SEL_UPD_DEL_INS,ON_ON_ALL,TABLE_VIEW_ALL,\\"DATABASE\\" DATABASE_NAME,\\"SCHEMA\\" SCHEMA_NAME ,EXACT_TABLE_VIEW_ALL,\\"ROLE\\" ROLE_NAME ,RANKING" + "\\n";
          loop_views = loop_views + "FROM DWH_PROD.APLRUNS.V_ADM_GRANTS_RULES" + "\\n";
          loop_views = loop_views + "WHERE NVL(UPPER(DATABASE_NAME), ''-1'') ILIKE ''%'' || (CASE WHEN ''" + v_database_name + "'' = ''Default'' THEN NVL(UPPER(DATABASE_NAME), ''-1'') ELSE UPPER(''" + v_database_name + "'') END) || ''%''" + "\\n"; 
          loop_views = loop_views + "AND NVL(UPPER(SCHEMA_NAME), ''-1'') ILIKE ''%'' || (CASE WHEN ''" + v_schema_name + "'' = ''Default'' THEN NVL(UPPER(SCHEMA_NAME), ''-1'') ELSE UPPER(''" + v_schema_name + "'') END) || ''%''" + "\\n";
          loop_views = loop_views + "AND NVL(UPPER(ROLE_NAME), ''-1'') ILIKE ''%'' || (CASE WHEN ''" + v_role_name + "'' = ''Default'' THEN NVL(UPPER(ROLE_NAME), ''-1'') ELSE UPPER(''" + v_role_name + "'') END) || ''%''" + "\\n";
          loop_views = loop_views + "AND NVL(UPPER(EXACT_TABLE_VIEW_ALL), ''-1'') ILIKE ''%'' || (CASE WHEN ''" + v_table_name + "'' = ''Default'' THEN NVL(UPPER(EXACT_TABLE_VIEW_ALL), ''-1'') ELSE UPPER(''" + v_table_name + "'') END) || ''%''" + "\\n";
          loop_views = loop_views + "ORDER BY RANKING" + "\\n";

          var result_set = snowflake.execute( {sqlText: loop_views} );

          // Loop through the results, processing one row at a time... 
          var result_sql = "";
          var report_return = "";
          var count_err = 0;
          var count_sqls = 0;
          while (result_set.next())  {
          
              result_sql = result_set.getColumnValue(1);

              try {
                  count_sqls++;
                  snowflake.execute( {sqlText: result_sql} )
              }
              catch (err)  {
                  count_err++;
                  report_return = report_return + "Failed: " + result_sql + " with error:" + err + "\\n" + "\\n";   
              }
          }

          if (count_err == 0) {
            report_return = "Done" + "\\n" + "Sqls executed = " + count_sqls + ".";
          } else {
            report_return = report_return + "\\n" + "Sqls executed with errors = " + count_err + "\\n" + "Sqls executed = " + count_sqls + ".";
          }

        return report_return;  
    } else {
        return "You do not have rights to do that! Role:" + v_role_current;
    }
  ';
