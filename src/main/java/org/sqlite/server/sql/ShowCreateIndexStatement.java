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

/** "SHOW CREATE INDEX [schema_name.]index_name [{FROM | IN} schema_name]" statement.
 * 
 * @author little-pan
 * @since 2019-12-01
 *
 */
public class ShowCreateIndexStatement extends SQLStatement {
    
    protected String schemaName;
    protected String indexName;
    
    public ShowCreateIndexStatement(String sql) {
        super(sql, "SHOW CREATE INDEX", true);
    }
    
    @Override
    public String getExecutableSQL() throws SQLException {
        final String sql, f;
        
        if (this.schemaName == null) {
            f = "select name 'Index', sql 'Create Index' from sqlite_master"
                    + " where type = 'index' and name = '%s'";
            sql = format(f, this.indexName);
        } else {
            f = "select name 'Index', sql 'Create Index' from '%s'.sqlite_master"
                    + " where type = 'index' and name = '%s'";
            sql = format(f, this.schemaName, this.indexName);
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
    
    public String getIndexName() {
        return indexName;
    }
    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
}
