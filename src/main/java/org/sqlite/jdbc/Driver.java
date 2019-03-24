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
package org.sqlite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.sqlite.jdbc.v4.JDBC4Connection;
import org.sqlite.util.IoUtils;

/**<p>
 * The SQLite server JDBC driver. The JDBC url pattern: <br/>
 * jdbc:sqlites://[domain[:port]]/fileName[?k1=v1&k2=v2...]
 * </p>
 * 
 * @author little-pan
 * @since 2019-03-24
 *
 */
public class Driver implements java.sql.Driver {
    
    public static final String PREFIX = "jdbc:sqlites:";
    
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            // ignore
        }
    }
    
    public Driver() {
        
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return (url != null && url.startsWith(PREFIX));
    }

    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if(!acceptsURL(url)) {
            return null;
        }
        
        url = url.trim();
        return new JDBC4Connection(url, extractAddress(url), props);
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 27;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException {
        return (new DriverPropertyInfo[0]);
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    static String extractAddress(String url) {
        return url.substring(PREFIX.length() + 2/*//*/);
    }
    
    public static void main(String args[]) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlites://localhost/test.db");
        try {
            
        } finally {
            IoUtils.close(conn);
        }
    }
    
}
