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

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.server.util.IoUtils;

/**
 * @author little-pan
 * @since 2019-09-05
 *
 */
public abstract class TestDbBase extends TestBase {
    protected static String url = "jdbc:postgresql://localhost:"+SQLiteServer.PORT_DEFAULT+"/test.db";
    protected static String user = "root";
    protected static String password = "123456";
    
    protected static SQLiteServer server;
    static {
        String dataDir= getDataDir();
        
        deleteDataDir(new File(dataDir));
        String[] initArgs = {"initdb", "-D", dataDir, "-p", password};
        server = SQLiteServer.create(initArgs);
        server.initdb(initArgs);
        IoUtils.close(server);
        
        String[] bootArgs = {"boot", "-D", dataDir};
        server = SQLiteServer.create(bootArgs);
        server.bootAsync(bootArgs);
    }
    
    protected static String getUrl(int port, String path) {
        return ("jdbc:postgresql://localhost:"+port+"/"+path);
    }
    
    protected Connection getConnection(String url, String user, String password) 
            throws SQLException {
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected Connection getConnection(int port, String path, String user, String password) 
            throws SQLException {
        String url = getUrl(port, path);
        return (DriverManager.getConnection(url, user, password));
    }
    
    protected Connection getConnection() throws SQLException {
        return (getConnection(url, user, password));
    }
    
    protected static String getDataDir() {
        return getDataDir(null);
    }
    
    protected static String getDataDir(String dataDir) {
        if (dataDir == null || dataDir.length() == 0) {
            dataDir = "sqlite3Test";
        }
        return ("data"+File.separator+dataDir);
    }
    
    protected static void deleteDataDir(String dataDir) {
        File target = new File(dataDir);
        deleteDataDir(target);
    }
    
    protected static void deleteDataDir(File dataDir) {
        assertTrue(!dataDir.exists() || dataDir.isDirectory());
        
        dataDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                File f = new File(dir, name);
                if (f.isFile()) {
                    boolean ok = f.delete();
                    // wait for sqlite3 closing DB
                    for (int i = 0;!ok && i < 5; ++i) {
                        try {
                            Thread.sleep(50L);
                            ok = f.delete();
                        } catch (InterruptedException e) {}
                    }
                    assertTrue(ok);
                    return ok;
                }
                
                return false;
            }
        });
        
        assertTrue(!dataDir.exists() || dataDir.delete());
    }

    protected void connectionTest(Connection conn, String sql, String result)
            throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (stmt.execute(sql)) {
                try (ResultSet rs = stmt.getResultSet()) {
                    if (result == null) {
                        assertTrue(!rs.next());
                    } else {
                        assertTrue(rs.next());
                        assertTrue(result.equals(rs.getString(1)));
                    }
                }
            } else {
                assertTrue(result.equals(stmt.getUpdateCount()+""));
            }
        }
    }
    
}
