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

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.Properties;

import org.sqlite.core.DB;
import org.sqlite.jdbc.v3.JDBC3Connection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;
import org.sqlite.jdbc4.JDBC4Statement;

/**<p>
 * The JDBC v4 connection.
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-24
 *
 */
public class JDBC4Connection extends JDBC3Connection {

    /**
     * @param db
     */
    public JDBC4Connection(DB db) {
        super(db);
    }
    
    public JDBC4Connection(String url, String address, Properties props) throws SQLException {
        super(url, address, props);
    }
    
    @Override
    public Statement createStatement(int rst, int rsc, int rsh) throws SQLException {
        checkOpen();
        checkCursor(rst, rsc, rsh);

        return new JDBC4Statement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int rst, int rsc, int rsh) 
            throws SQLException {
        checkOpen();
        checkCursor(rst, rsc, rsh);

        return new JDBC4PreparedStatement(this, sql);
    }

    //JDBC 4
    /**
     * @see java.sql.Connection#isClosed()
     */
    @Override
    public boolean isClosed() throws SQLException {
        return super.isClosed();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws ClassCastException {
        // caller should invoke isWrapperFor prior to unwrap
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }
        Statement statement = createStatement();
        try {
            return statement.execute("select 1");
        } finally {
            statement.close();
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // ignore
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // ignore
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
}
