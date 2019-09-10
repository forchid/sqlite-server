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

import java.sql.Connection;
import java.sql.SQLException;

import org.sqlite.util.IoUtils;

/**SQLite server test case.
 * 
 * @author little-pan
 * @since 2019-09-10
 *
 */
public class TestSQLiteServer extends TestDbBase {
    
    public static void main(String[] args) throws SQLException {
        new TestSQLiteServer().test();
    }

    @Override
    public void test() throws SQLException {
        initdbTest();
    }

    private void initdbTest() throws SQLException {
        SQLiteServer server = null;
        String dataDir = getDataDir("initdbTest");
        String user = "sa", password = "", db = "test";
        String sql = "select 1";
        String authMethod;
        int port = 3273;
        String[] args;
        Connection conn;
        
        // test trust
        deleteDataDir(dataDir);
        authMethod = "trust";
        args = new String[]{"initdb", "-D", dataDir, "-A", authMethod, "-U", user};
        server = SQLiteServer.create(args);
        server.initdb(args);
        assertTrue(server.isInited());
        try {
            server.initdb(args);
            fail("initdb again");
        } catch (IllegalStateException e){
            // OK
        }
        IoUtils.close(server);
        args = new String[]{"boot", "-D", dataDir, "-P", port+""};
        server = SQLiteServer.create(args);
        server.bootAsync(args);
        conn = getConnection(port, db, user, password);
        connectionTest(conn, sql, "1");
        assertTrue(server.isOpen());
        assertTrue(!server.isStopped());
        IoUtils.close(conn);
        IoUtils.close(server);
        assertTrue(!server.isOpen());
        assertTrue(server.isStopped());
        
        // test password
        deleteDataDir(dataDir);
        authMethod = "password";
        password = "abc123";
        args = new String[]{"initdb", "-D", dataDir, "-A", authMethod, "-U", user};
        server = SQLiteServer.create(args);
        try {
            server.initdb(args);
            fail("No password for auth method " + authMethod);
        } catch (IllegalArgumentException e){
            // OK
            IoUtils.close(server);
        }
        args = new String[]{"initdb", "-D", dataDir, "-A", authMethod, "-U", user, "-p", password};
        server = SQLiteServer.create(args);
        server.initdb(args);
        assertTrue(server.isInited());
        try {
            server.initdb(args);
            fail("initdb again");
        } catch (IllegalStateException e){
            // OK
            IoUtils.close(server);
        }
        args = new String[]{"boot", "-D", dataDir, "-P", port+""};
        server = SQLiteServer.create(args);
        server.bootAsync(args);
        try {
            conn = getConnection(port, db, user, password+"X");
            fail("password is incorrect");
        } catch (SQLException e) {
            // OK
        }
        conn = getConnection(port, db, user, password);
        connectionTest(conn, sql, "1");
        assertTrue(server.isOpen());
        assertTrue(!server.isStopped());
        IoUtils.close(conn);
        IoUtils.close(server);
        assertTrue(!server.isOpen());
        assertTrue(server.isStopped());
        try {
            server.bootAsync(args);
            fail("Server has been closed");
        } catch (IllegalStateException e){
            // OK
            IoUtils.close(server);
        }
        
        // test md5
        deleteDataDir(dataDir);
        authMethod = "md5";
        password = "aaa111";
        args = new String[]{"initdb", "-D", dataDir, "-A", authMethod, "-U", user};
        server = SQLiteServer.create(args);
        try {
            server.initdb(args);
            fail("No password for auth method " + authMethod);
        } catch (IllegalArgumentException e){
            // OK
            IoUtils.close(server);
        }
        args = new String[]{"initdb", "-D", dataDir, "-A", authMethod, "-U", user, "-p", password};
        server = SQLiteServer.create(args);
        server.initdb(args);
        assertTrue(server.isInited());
        try {
            server.initdb(args);
            fail("initdb again");
        } catch (IllegalStateException e){
            // OK
            IoUtils.close(server);
        }
        args = new String[]{"boot", "-D", dataDir, "-P", port+""};
        server = SQLiteServer.create(args);
        args = new String[]{"-D", dataDir, "-P", port+""};
        server.bootAsync(args);
        try {
            conn = getConnection(port, db, user, password+"X");
            fail("password is incorrect");
        } catch (SQLException e) {
            // OK
        }
        try {
            conn = getConnection(port, db, user, "");
            fail("password is incorrect");
        } catch (SQLException e) {
            // OK
        }
        conn = getConnection(port, db, user, password);
        connectionTest(conn, sql, "1");
        assertTrue(server.isOpen());
        assertTrue(!server.isStopped());
        IoUtils.close(conn);
        IoUtils.close(server);
        assertTrue(!server.isOpen());
        assertTrue(server.isStopped());
    }
    
}
