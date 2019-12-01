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

/** "SHOW {INDEX | INDEXES} [{FROM | IN} [schema_name.]table_name [{FROM | IN} schema_name]] | [WHERE 'pattern'] ", or
 * "SHOW {INDEX | INDEXES} [EXTENDED] COLUMNS {FROM | IN} [schema_name.]index_name [{FROM | IN} schema_name]" 
 * statement.
 * 
 * @author little-pan
 * @since 2019-11-30
 *
 */
public class ShowIndexesStatement extends SQLStatement {
    
    protected String schemaName;
    protected String tableName;
    protected String pattern;
    
    protected String indexName;
    protected boolean indexColumns;
    protected boolean extended;
    
    public ShowIndexesStatement(String sql) {
        super(sql, "SHOW INDEXES", true);
    }
    
    @Override
    public String getExecutableSQL() throws SQLException {
        final String sql, f;
        
        if (this.indexColumns) {
            if (this.schemaName != null && this.indexName != null) {
                f = "pragma '%s'.index_%sinfo('%s')";
                sql = format(f, this.schemaName, extended? "x": "", this.indexName);
            } else if (this.schemaName != null && this.indexName == null) {
                throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, "No index name specified");
            } else if (this.schemaName == null && this.indexName != null) {
                f = "pragma index_%sinfo('%s')";
                sql = format(f, extended? "x": "", this.indexName);
            } else {
                throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, "No index name specified");
            }
        } else {
            if (this.schemaName != null && this.tableName != null) {
                f = "pragma '%s'.index_list('%s')";
                sql = format(f, this.schemaName, this.tableName);
            } else if (this.schemaName != null && this.tableName == null) {
                throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, "No table name specified");
            } else if (this.schemaName == null && this.tableName != null) {
                f = "pragma index_list('%s')";
                sql = format(f, this.tableName);
            } else {
                if (this.pattern == null) {
                    sql = "select name from sqlite_master where type = 'index' order by name asc";
                } else {
                    f   = "select name from sqlite_master where type = 'index' and name like '%s' order by name asc";
                    sql = format(f, this.pattern);
                }
            }
        }
        
        // check SQL
        try (SQLParser parser = new SQLParser(sql, true)) {
            SQLStatement stmt = parser.next();
            if (parser.hasNext()) {
                throw ConvertUtils.convertError(SQLiteErrorCode.SQLITE_ERROR, getSQL());
            }
            
            if (this.indexColumns || this.tableName != null) {
                if ("PRAGMA".equals(stmt.getCommand())) {
                    return stmt.getSQL();
                }
            } else {
                if ("SELECT".equals(stmt.getCommand())) {
                    return stmt.getSQL();
                }
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
    
    public boolean isIndexColumns() {
        return indexColumns;
    }
    
    public void setIndexColumns(boolean indexColumns) {
        this.indexColumns = indexColumns;
    }
    
    public boolean isExtended() {
        return extended;
    }
    
    public void setExtended(boolean extended) {
        this.extended = extended;
    }
    
    public String getPattern() {
        return pattern;
    }
    
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
}
