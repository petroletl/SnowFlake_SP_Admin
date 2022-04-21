CREATE OR REPLACE PROCEDURE "SP_RETENTION_STAGES"("RETENTION" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '      
      
      class ListStages{
                constructor() {}
                get dataName() { return "STAGE FILES"; }
                get opType() { return "SELECT";}                
                get sqlText() { return `SHOW STAGES IN ACCOUNT`}
                checkForError(rowObj){return null;}
      } 
      
      class RetentionStage{
                constructor(db_name,schema_name,stage_name,retention) {this.stage_name=stage_name;this.db_name=db_name;this.schema_name=schema_name;this.retention=retention}
                get dataName() { return this.stage_name; }
                get opType() { return "CALL";}              
                get binds() {return [this.db_name,this.schema_name,this.stage_name,this.retention];}
                get sqlText() { return `call SNOWFLAKE_ADMIN.PUBLIC.sp_CLEAR_STAGE_W_RETENTION(:1,:2,:3,:4)`}
                get stopIfError() { return false; }
                get logCount() { return true; }
                checkForError(rowObj){return null;}
      } 
      
      
      var run_over_stages = function(retention,table_as_json) {
                        var qry_objs = [];      
                        var stg_cpy_obj = new ListStages();                                                
                        var stmt = snowflake.createStatement(stg_cpy_obj);
                        var rs = stmt.execute();                           
                        var num_cols = stmt.getColumnCount()                          
                        while (rs.next())  {        
                                           row_as_json = {};        
                                           for (var col_num = 0; col_num < num_cols; col_num = col_num + 1) {
                                                var col_name = stmt.getColumnName(col_num+1);
                                                row_as_json[col_name] = rs.getColumnValue(col_num + 1);
                                                }      
                                           tp = row_as_json["type"]
                                           db_name = row_as_json["database_name"]
                                           schema_name = row_as_json["schema_name"]
                                           stage = row_as_json["name"]                                           
                                           if (tp=="INTERNAL" && !["WORKSHEETS_APP"].includes(db_name))
                                           {
                                              qry = new RetentionStage(db_name,schema_name,stage,retention);
                                              qry_objs.push (qry); 
                                           }
                                                                                                                            
                                           }        
                              
                        for (const qry_obj of qry_objs){ 
                             if (!table_as_json[qry_obj.dataName])
                             {
                              table_as_json[qry_obj.dataName] = {};
                              table_as_json[qry_obj.dataName]["RETURN"]= [];
                             }
                             table_as_json[qry_obj.dataName]["OPERATION"]= qry_obj.opType;                             
                             try {
                                 var stmt = snowflake.createStatement(qry_obj);
                                 var rs = stmt.execute();                           
                             }
                             catch (err)
                             {
                                  table_as_json[qry_obj.dataName]["ERROR"] = err.message;
                                  if (qry_obj.stopIfError){break;};                                  
                             }
                             var num_cols = stmt.getColumnCount();     
                             table_as_json[qry_obj.dataName]["ROW_COUNT"] = stmt.getRowCount()
                             var stop = false;
                             while (rs.next())  {        
                                              row_as_json = {};                                                      
                                              for (var col_num = 0; col_num < num_cols; col_num = col_num + 1) {
                                                   var col_name = stmt.getColumnName(col_num+1);
                                                   var val = rs.getColumnValue(col_num + 1);
                                                   row_as_json[col_name] = val;                                                                                                     
                                                   }        
                                              table_as_json[qry_obj.dataName]["RETURN"].push(row_as_json); 
                                              var error = qry_obj.checkForError(row_as_json);
                                              if (error) {                                                  
                                                  table_as_json[qry_obj.dataName]["ERROR"]= error;
                                                  if (qry_obj.stopIfError){stop = true;}
                                                  
                                              }
                                              }      
                             if (stop) {break;}
                           }
      
         return null
         };  
            
      var table_as_json = {}            
      run_over_stages(RETENTION,table_as_json)
      
      return table_as_json;
      ';
