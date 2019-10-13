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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.SQLiteProcessor;
import org.sqlite.sql.ImplicitCommitException;
import org.sqlite.sql.SQLParseException;
import org.sqlite.sql.SQLParser;
import org.sqlite.sql.SQLStatement;

import static org.sqlite.util.ConvertUtils.*;

/** CREATE {DATABASE | SCHEMA} [IF NOT EXISTS] dbname [{LOCATION | DIRECTORY} 'data-dir']
 * 
 * @author little-pan
 * @since 2019-09-19
 *
 */
public class CreateDatabaseStatement extends MetaStatement {
    static final Logger log = LoggerFactory.getLogger(CreateDatabaseStatement.class);
    
    protected final Catalog catalog = new Catalog();
    private boolean quite; // true: if not exists
    private boolean created;

    public CreateDatabaseStatement(String sql) {
        super(sql, "CREATE DATABASE");
    }
    
    public Catalog getCatalog() {
        return this.catalog;
    }
    
    public boolean isQuite() {
        return quite;
    }

    public void setQuite(boolean quite) {
        this.quite = quite;
    }
    
    public void setDb(String db) {
        this.catalog.setDb(db);
    }
    
    public String getDb() {
        return this.catalog.getDb();
    }
    
    public String getDir() {
        return this.catalog.getDir();
    }
    
    public void setDir(String dir) {
        this.catalog.setDir(dir);
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
    public boolean executionException(SQLException e) {
        if (!isQuite()) {
            return false;
        }
        
        boolean duplicated = getContext().isUniqueViolated(e);
        if (duplicated && this.context.isTrace()) {
            this.context.trace(log, "Database '{}' existing", getDb());
        }
        return duplicated;
    }

    @Override
    public String getMetaSQL(String metaSchema) throws SQLException, SQLParseException {
        String db = getDb(), dir, sql;
        if (db == null) {
            throw new SQLParseException("No dbname specified");
        }
        if (!this.created) {
            SQLiteProcessor proc = getContext();
            if (proc.getMetaDbName().equals(getDb())) {
                SQLiteErrorCode error = SQLiteErrorCode.SQLITE_ERROR;
                String message = "Can't create a database named by meta db's name";
                throw convertError(error, message);
            }
            
            proc.createDbFile(this);
            this.created = true;
        }
        
        dir = getDir();
        if (dir == null) {
            sql = String.format("insert into '%s'.catalog(db, dir)values('%s', NULL)", 
                    metaSchema, db);
        } else {
            sql = String.format("insert into '%s'.catalog(db, dir)values('%s', '%s')", 
                    metaSchema, db, dir);
        }
        
        // check SQL
        try (SQLParser parser = new SQLParser(sql)) {
            SQLStatement stmt = parser.next();
            if ("INSERT".equals(stmt.getCommand()) && !parser.hasNext()) {
                return stmt.getSQL();
            }
        } catch (SQLParseException e){}
        
        throw new SQLParseException(getSQL());
    }
    
}
