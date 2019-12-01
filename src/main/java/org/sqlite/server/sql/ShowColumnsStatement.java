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
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;
import org.sqlite.util.ConvertUtils;

/** "SHOW [EXTENDED] {COLUMNS | FIELDS} {FROM | IN} [schema_name.]table_name [{FROM | IN} schema_name]" statement.
 * 
 * @author little-pan
 * @since 2019-11-30
 *
 */
public class ShowColumnsStatement extends SQLStatement {

    protected boolean extended;
    protected String schemaName;
    protected String tableName;
    
    public ShowColumnsStatement(String sql) {
        super(sql, "SHOW COLUMNS", true);
    }
    
    public boolean isExtended() {
        return extended;
    }
    
    public void setExtended(boolean extended) {
        this.extended = extended;
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
    
    @Override
    public String getExecutableSQL() throws SQLException {
        String sql, f;
        if (this.isExtended() && this.schemaName != null) {
            f = "pragma '%s'.table_xinfo('%s')";
            sql = format(f, this.schemaName, this.tableName);
        } else if (this.isExtended() && this.schemaName == null) {
            f = "pragma table_xinfo('%s')";
            sql = format(f,  this.tableName);
        } else if (!this.isExtended() && this.schemaName != null) {
            f = "pragma '%s'.table_info('%s')";
            sql = format(f, this.schemaName, this.tableName);
        } else {
            f = "pragma table_info('%s')";
            sql = format(f,  this.tableName);
        }
        
        // check SQL
        try (SQLParser parser = new SQLParser(sql, true)) {
            SQLStatement stmt = parser.next();
            if ("PRAGMA".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        }  catch (SQLParseException e) {
            throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, e.getMessage());
        }
        
        throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, getSQL());
    }
    
}
