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
package org.sqlite.jdbc.v3;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteOpenMode;
import org.sqlite.core.DB;
import org.sqlite.core.RemoteDB;

/**<p>
 * The JDBC v3 connection.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-24
 *
 */
public abstract class JDBC3Connection extends SQLiteConnection {
    
    private final AtomicInteger savePoint = new AtomicInteger(0);
    private Map<String, Class<?>> typeMap;
    
    protected JDBC3Connection(DB db) {
        super(db);
    }

    /**
     * @param url
     * @param address
     * @param prop
     * @throws SQLException
     */
    protected JDBC3Connection(String url, String address, Properties prop) throws SQLException {
        this(open(url, address, prop));
    }
    
    /**
     * @see java.sql.Connection#getCatalog()
     */
    public String getCatalog() throws SQLException {
        checkOpen();
        return null;
    }

    /**
     * @see java.sql.Connection#setCatalog(java.lang.String)
     */
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
    }

    /**
     * @see java.sql.Connection#getHoldability()
     */
    public int getHoldability() throws SQLException {
        checkOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * @see java.sql.Connection#setHoldability(int)
     */
    public void setHoldability(int h) throws SQLException {
        checkOpen();
        if (h != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException("SQLite only supports CLOSE_CURSORS_AT_COMMIT");
        }
    }

    /**
     * @see java.sql.Connection#getTypeMap()
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        synchronized (this) {
            if (this.typeMap == null) {
                this.typeMap = new HashMap<String, Class<?>>();
            }

            return this.typeMap;
        }
    }

    /**
     * @see java.sql.Connection#setTypeMap(java.util.Map)
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        synchronized (this) {
            this.typeMap = map;
        }
    }

    /**
     * @see java.sql.Connection#isReadOnly()
     */
    public boolean isReadOnly() throws SQLException {
        return (getDatabase().getConfig().getOpenModeFlags() & SQLiteOpenMode.READONLY.flag) != 0;
    }

    /**
     * @see java.sql.Connection#setReadOnly(boolean)
     */
    public void setReadOnly(boolean ro)  throws SQLException  {
        // trying to change read-only flag
        if (ro != isReadOnly()) {
            throw new SQLException(
                    "Cannot change read-only flag after establishing a connection.");
        }
    }

    /**
     * @see java.sql.Connection#nativeSQL(java.lang.String)
     */
    public String nativeSQL(String sql) {
        return sql;
    }

    /**
     * @see java.sql.Connection#clearWarnings()
     */
    public void clearWarnings()
            throws SQLException
    {
    }

    /**
     * @see java.sql.Connection#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    /**
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * @see java.sql.Connection#createStatement(int, int)
     */
    public Statement createStatement(int rsType, int rsConcurr) throws SQLException {
        return createStatement(rsType, rsConcurr, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * @see java.sql.Connection#createStatement(int, int, int)
     */
    public abstract Statement createStatement(int rst, int rsc, int rsh)
            throws SQLException;

    /**
     * @see java.sql.Connection#prepareCall(java.lang.String)
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
     */
    public CallableStatement prepareCall(String sql, int rst, int rsc) throws SQLException {
        return prepareCall(sql, rst, rsc, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
     */
    public CallableStatement prepareCall(String sql, int rst, int rsc, int rsh) throws SQLException {
        throw new SQLException("SQLite does not support Stored Procedures");
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String, int)
     */
    public PreparedStatement prepareStatement(String sql, int autoC) throws SQLException {
        return prepareStatement(sql);
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
     */
    public PreparedStatement prepareStatement(String sql, int[] colInds) throws SQLException {
        return prepareStatement(sql);
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
     */
    public PreparedStatement prepareStatement(String sql, String[] colNames) throws SQLException {
        return prepareStatement(sql);
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
     */
    public PreparedStatement prepareStatement(String sql, int rst, int rsc) throws SQLException {
        return prepareStatement(sql, rst, rsc, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
     */
    public abstract PreparedStatement prepareStatement(String sql, int rst, int rsc, int rsh)
            throws SQLException;

    /**
     * @see java.sql.Connection#setSavepoint()
     */
    public Savepoint setSavepoint() throws SQLException  {
        checkOpen();
        if(getAutoCommit()) {
            // when a SAVEPOINT is the outermost savepoint and not
            // with a BEGIN...COMMIT then the behavior is the same
            // as BEGIN DEFERRED TRANSACTION
            // http://www.sqlite.org/lang_savepoint.html
            getConnectionConfig().setAutoCommit(false);
        }
        Savepoint sp = new JDBC3Savepoint(savePoint.incrementAndGet());
        getDatabase().exec(String.format("SAVEPOINT %s", sp.getSavepointName()), false);
        return sp;
    }

    /**
     * @see java.sql.Connection#setSavepoint(java.lang.String)
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        if(getAutoCommit()) {
            // when a SAVEPOINT is the outermost savepoint and not
            // with a BEGIN...COMMIT then the behavior is the same
            // as BEGIN DEFERRED TRANSACTION
            // http://www.sqlite.org/lang_savepoint.html
            getConnectionConfig().setAutoCommit(false);
        }
        Savepoint sp = new JDBC3Savepoint(savePoint.incrementAndGet(), name);
        getDatabase().exec(String.format("SAVEPOINT %s", sp.getSavepointName()), false);
        return sp;
    }

    /**
     * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        if (getAutoCommit()) {
            throw new SQLException("database in auto-commit mode");
        }
        getDatabase().exec(String.format("RELEASE SAVEPOINT %s", savepoint.getSavepointName()), false);
    }

    /**
     * @see java.sql.Connection#rollback(java.sql.Savepoint)
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        if (getAutoCommit()) {
            throw new SQLException("database in auto-commit mode");
        }
        getDatabase().exec(String.format("ROLLBACK TO SAVEPOINT %s", 
                savepoint.getSavepointName()), getAutoCommit());
    }

    // UNUSED FUNCTIONS /////////////////////////////////////////////

    public Struct createStruct(String t, Object[] attr) throws SQLException {
        throw new SQLException("unsupported by SQLite");
    }

    /**
     * @param url
     * @param address [domain[:port]]/fileName[?k1=v1&k2=v2...]
     * @param props
     * @return The DB instance
     */
    static DB open(String url, String address, Properties props) throws SQLException {
        final Properties newProps = new Properties();
        newProps.putAll(props);
        
        final String fileName = extractPragmasFromFilename(url, address, props);
        final SQLiteConfig config = new SQLiteConfig(newProps);
        
        final DB db = new RemoteDB(url, fileName, config);
        db.open(fileName, config.getOpenModeFlags());
        return db;
    }

}
