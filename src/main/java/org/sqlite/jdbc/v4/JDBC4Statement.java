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
package org.sqlite.jdbc.v4;

import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc.v3.JDBC3Statement;

/**
 * @author little-pan
 * @since 2019-04-06
 *
 */
public class JDBC4Statement extends JDBC3Statement implements Statement {
    
    private boolean closed = false;
    boolean closeOnCompletion;

    /**
     * @param conn
     */
    public JDBC4Statement(SQLiteConnection conn) {
        super(conn);
    }
    
    // JDBC 4
    public <T> T unwrap(Class<T> iface) throws ClassCastException {
        return iface.cast(this);
    }

    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public void close() throws SQLException {
        super.close();
        closed = true; // isClosed() should only return true when close() happened
    }

    public boolean isClosed() {
        return closed;
    }

    public void closeOnCompletion() throws SQLException {
        if (closed) throw new SQLException("statement is closed");
        closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() throws SQLException {
        if (closed) throw new SQLException("statement is closed");
        return closeOnCompletion;
    }

    public void setPoolable(boolean poolable) throws SQLException {
        
    }

    public boolean isPoolable() throws SQLException {
        
        return false;
    }

}
