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
package org.sqlite.server;

import static java.lang.String.*;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.sqlite.SQLiteConnection;
import org.sqlite.server.sql.local.LocalStatement;
import org.sqlite.server.util.SecurityUtils;

/** SQLite local(session level) memory database that non-blocking executes 
 * session level SQL statements such as "SET TRANSACTION", "SHOW processlist".
 * 
 * @author little-pan
 * @since 2019-10-13
 *
 */
public class SQLiteLocalDb {
    
    protected final SQLiteProcessor processor;
    protected final Map<LocalStatement, LocalStatement> localStmts;
    
    protected final String schemaName;
    private boolean attached;
    
    public SQLiteLocalDb(SQLiteProcessor processor) {
        this.processor = processor;
        this.schemaName = "local_"+ SecurityUtils.nextHexs(5);
        this.localStmts = new HashMap<>();
    }
    
    public String getSchemaName() {
        return this.schemaName;
    }
    
    public boolean isAttached() {
        return this.attached;
    }
    
    public SQLiteLocalDb attach() throws SQLException {
        if (!isAttached()) {
            SQLiteConnection conn = this.processor.getConnection();
            try (Statement s = conn.createStatement()) {
                String f = "attach database ':memory:' as '%s'";
                s.execute(format(f, this.schemaName));
                
                // "UPDATABLE" table is used to execute local "UPDATE" statement
                f = "create table '%s'.updatable(id integer primary key, value integer)";
                s.execute(format(f, this.schemaName));
                f = "insert into '%s'.updatable(id, value)values(1, 0)";
                s.execute(format(f, this.schemaName));
                
                this.attached = true;
            }
        }
        
        return this;
    }
    
    public boolean detach() throws SQLException {
        if (this.localStmts.size() > 0) {
            return false;
        }
        
        if (isAttached()) {
            SQLiteConnection conn = this.processor.getConnection();
            try (Statement s = conn.createStatement()) {
                String f = "detach database '%s'";
                s.execute(format(f, this.schemaName));
                this.attached = false;
            }
        }
        return true;
    }
    
    public void register(LocalStatement stmt) {
        this.localStmts.put(stmt, stmt);
    }
    
    public void deregister(LocalStatement stmt) {
        this.localStmts.remove(stmt);
    }

    @Override
    public String toString() {
        return getSchemaName();
    }
    
}
