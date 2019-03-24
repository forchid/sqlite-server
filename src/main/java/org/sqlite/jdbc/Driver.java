/**
 * A SQLite server based on the C/S architecture, little-pan (c) 2019
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

/**<p>
 * The SQLite server JDBC driver.
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
        
        return new JDBC4Connection(url, props);
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

}
