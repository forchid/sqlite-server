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

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.sql.meta.User;
import org.sqlite.sql.SQLStatement;

/**The SQLite meta database operation statement.
 * 
 * @author little-pan
 * @since 2019-09-11
 *
 */
public abstract class MetaStatement extends SQLStatement {
    static final Logger log = LoggerFactory.getLogger(MetaStatement.class);
    
    public static final String PROTO_DEFAULT = "pg";
    public static final String AUTHM_DEFAULT = "md5";
    
    protected MetaStatement(String sql) {
        super(sql);
    }
    
    protected MetaStatement(String sql, String command) {
        super(sql, command);
    }
    
    protected MetaStatement(String sql, String command, boolean query) {
        super(sql, command, query);
    }
    
    @Override
    protected void checkPermission() throws SQLException {
        User user = getMetaUser();
        if (user.isSa() || !isNeedSa()) {
            return;
        }
        
        throw convertError(SQLiteErrorCode.SQLITE_PERM);
    }
    
    protected User getMetaUser() {
        SQLiteProcessor proc = getContext();
        return (proc.getUser());
    }
    
    protected SQLException convertError(SQLiteErrorCode error) {
        SQLiteProcessor proc = getContext();
        return proc.convertError(error);
    }
    
    @Override
    public void postResult() throws SQLException {
        if (this.context.isAutoCommit()) {
            SQLiteProcessor proc = getContext();
            proc.detachMetaDb();
        }
        
        super.postResult();
    }
    
    @Override
    public SQLiteProcessor getContext() {
        return (SQLiteProcessor)this.context;
    }
    
    @Override
    public String getExecutableSQL() throws SQLException {
        SQLiteProcessor proc = getContext();
        proc.attachMetaDb();
        String metaSQL = getMetaSQL(proc.getMetaSchema());
        this.context.trace(log, "meta SQL: {}", metaSQL);
        return metaSQL;
    }

    public abstract String getMetaSQL(String metaSchema);
    
    public boolean isNeedSa() {
        return true;
    }
    
}
