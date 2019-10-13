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
package org.sqlite.server.sql.meta;

import java.sql.SQLException;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.sql.ImplicitCommitException;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

import static org.sqlite.util.ConvertUtils.*;

import org.sqlite.util.StringUtils;

/** DROP {DATABASE | SCHEMA} [IF EXISTS] dbname, requires superuser privilege.
 * @author little-pan
 * @since 2019-09-22
 *
 */
public class DropDatabaseStatement extends MetaStatement {
    
    protected boolean quiet;
    protected String db;

    public DropDatabaseStatement(String sql) {
        super(sql, "DROP DATABASE");
    }

    public boolean isQuiet() {
        return quiet;
    }

    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = StringUtils.toLowerEnglish(db);
    }
    
    protected void postExecute(boolean resultSet) throws SQLException {
        super.postExecute(resultSet);
        
        if (!isQuiet() && getJdbcStatement().getUpdateCount() == 0) {
            throw convertError(SQLiteErrorCode.SQLITE_ERROR, "Database not exists");
        }
    }
    
    @Override
    public void preExecute(int maxRows) throws SQLException {
        super.preExecute(maxRows);
        
        SQLiteProcessor proc = getContext();
        proc.deleteDbFile(this);
    }
    
    @Override
    public void complete(boolean success) throws ImplicitCommitException, IllegalStateException {
        super.complete(success);
        
        if (success) {
            SQLiteProcessor proc = getContext();
            proc.getServer().flushCatalogs();
        }
    }
    
    @Override
    public String getMetaSQL(String metaSchema) throws SQLParseException {
        if (this.db == null || this.db.length() == 0) {
            throw new SQLParseException("No dbname specified");
        }
        
        String f = "delete from '%s'.catalog where db = '%s'";
        String sql = String.format(f, metaSchema, this.db);
        // check
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            if ("DELETE".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e) {}
        
        throw new SQLParseException(getSQL());
    }

}
