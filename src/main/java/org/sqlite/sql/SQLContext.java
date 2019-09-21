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
package org.sqlite.sql;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;

/** A context of SQL execution
 * 
 * @author little-pan
 * @since 2019-09-21
 *
 */
public abstract class SQLContext {
    
    protected SQLContext() {
        
    }
    
    public abstract Connection getConnection();
    
    public boolean isTrace() {
        return false;
    }
    
    public abstract void trace(Logger log, String message);
    
    public abstract void trace(Logger log, String format, Object ... args);
    
    public abstract  void traceError(Logger log, String message, Throwable cause);
    
    public abstract void traceError(Logger log, String tag, String message, Throwable cause);
    
    public boolean isAutoCommit() throws SQLException {
        return (getConnection().getAutoCommit());
    }
    
    protected abstract void prepareTransaction(TransactionStatement txSql);
    
    protected abstract void pushSavepoint(TransactionStatement txSql);
    
    protected abstract void resetAutoCommit();
    
    protected abstract void releaseTransaction(TransactionStatement txSql, boolean finished);
    
    protected abstract boolean hasPrivilege(SQLStatement s) throws SQLException;
    
    protected abstract void checkPermission(SQLStatement s) throws SQLException;
    
}
