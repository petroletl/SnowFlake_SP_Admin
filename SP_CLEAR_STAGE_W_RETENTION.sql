CREATE OR REPLACE PROCEDURE "SP_CLEAR_STAGE_W_RETENTION"("DB_NAME" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216), "STAGE_NAME" VARCHAR(16777216), "RETENTION" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '      
      
      class GetFiles{
                constructor(db_name,schema_name,stage_name) {this.stage_name=stage_name;this.db_name=db_name;this.schema_name=schema_name;}
                get dataName() { return "STAGE FILES"; }
                get opType() { return "SELECT";}                
                get sqlText() { return `LIST @${this.db_name}.${this.schema_name}.${this.stage_name}`}
                checkForError(rowObj){return null;}
      } 
      
      class RemoveFiles{
                constructor(db_name,schema_name,file_name) {this.file_name=file_name;this.db_name=db_name;this.schema_name=schema_name;}
                get dataName() { return this.file_name; }
                get opType() { return "REMOVE";}                
                get sqlText() { return `REMOVE @${this.db_name}.${this.schema_name}.${this.file_name}`}
                get stopIfError() { return false; }
                get logCount() { return true; }
                checkForError(rowObj){return null;}
      } 
      
      
      var remove_stage_files = function(db_name,schema_name,stage_name,retention,table_as_json) {
                        var qry_objs = [];      
                        var stg_cpy_obj = new GetFiles(db_name,schema_name,stage_name);                                                
                        var stmt = snowflake.createStatement(stg_cpy_obj);
                        var rs = stmt.execute();                           
                        var num_cols = stmt.getColumnCount()    
                        cd = new Date();
                        cd.setDate(cd.getDate()-1*retention);                                                                                            
                        while (rs.next())  {        
                                           row_as_json = {};        
                                           for (var col_num = 0; col_num < num_cols; col_num = col_num + 1) {
                                                var col_name = stmt.getColumnName(col_num+1);
                                                row_as_json[col_name] = rs.getColumnValue(col_num + 1);
                                                }      
                                           lmd = Date.parse(row_as_json["last_modified"]);
                                           ddd = cd - lmd                                           
                                           if (ddd>0){                                                                                      
                                           qry = new RemoveFiles(db_name,schema_name,row_as_json["name"]);
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
      retention = parseInt(RETENTION)
      remove_stage_files(DB_NAME,SCHEMA_NAME,STAGE_NAME,retention,table_as_json)
      
      return table_as_json;
      ';
