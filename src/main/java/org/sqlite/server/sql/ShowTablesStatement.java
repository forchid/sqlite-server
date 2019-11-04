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

/** "SHOW TABLES [{FROM | IN} schema_name] [LIKE 'pattern']" statement.
 * @author little-pan
 * @since 2019-10-20
 *
 */
public class ShowTablesStatement extends SQLStatement {
    
    protected String schemaName;
    protected String pattern;
    
    public ShowTablesStatement(String sql) {
        super(sql, "SHOW TABLES", true);
    }
    
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
    
    @Override
    public String getExecutableSQL() throws SQLException {
        if (this.schemaName == null && this.pattern == null) {
            return "select name from sqlite_master where type in('table', 'view') order by name";
        }
        
        String sql, f;
        if (this.schemaName == null && this.pattern != null) {
            f = "select name from sqlite_master where type in('table', 'view') and name like '%s' order by name";
            sql = format(f, this.pattern);
        } else if (this.schemaName != null && this.pattern == null) {
            f = "select name from '%s'.sqlite_master where type in('table', 'view') order by name";
            sql = format(f, this.schemaName);
        } else {
            f = "select name from '%s'.sqlite_master where type in('table', 'view') and name like '%s' order by name";
            sql = format(f, this.schemaName, this.pattern);
        }
        
        // check SQL
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
    
}
