/**
 * Copyright 2019 little-pan. A SQLite server based on the C/S architecture.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sqlite.server.sql;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.util.ConvertUtils;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

/** "SHOW CREATE TABLE [schema_name.]tbl_name [{FROM | IN} schema_name]" statement.
 * 
 * @author little-pan
 * @since 2019-12-01
 *
 */
public class ShowCreateTableStatement extends SQLStatement {
    
    protected String schemaName;
    protected String tableName;
    
    public ShowCreateTableStatement(String sql) {
        super(sql, "SHOW CREATE TABLE", true);
    }
    
    @Override
    public String getExecutableSQL() throws SQLException {
        final String sql, f;
        
        if (this.schemaName == null) {
            f = "select name 'Table', sql 'Create Table' from sqlite_master"
                    + " where type in('table', 'view') and name = '%s'";
            sql = format(f, this.tableName);
        } else {
            f = "select name 'Table', sql 'Create Table' from '%s'.sqlite_master"
                    + " where type in('table', 'view') and name = '%s'";
            sql = format(f, this.schemaName, this.tableName);
        }
        
        try (SQLParser parser = new SQLParser(sql, true)) {
            SQLStatement stmt = parser.next();
            if ("SELECT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e) {
            throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, e.getMessage());
        }
        
        throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, getSQL());
    }
    
    public String getSchemaName() {
        return schemaName;
    }
    
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
}
