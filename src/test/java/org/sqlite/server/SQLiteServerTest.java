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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.TestDbBase;
import org.sqlite.server.util.IoUtils;

/**SQLite server test case.
 * 
 * @author little-pan
 * @since 2019-09-10
 *
 */
public class SQLiteServerTest extends TestDbBase {
    
    public static void main(String[] args) throws SQLException {
        new SQLiteServerTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        initdbTest();
        
        maxAllowedPacketTest(1);
        maxAllowedPacketTest(2);
        maxAllowedPacketTest(10);
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
        args = new String[]{"-D", dataDir, "-A", authMethod, "-U", user, "-d", db};
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
        args = new String[]{"-D", dataDir, "-P", port+""};
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
        args = new String[]{"-D", dataDir, "-A", authMethod, "-U", user, "-d", db};
        server = SQLiteServer.create(args);
        try {
            server.initdb(args);
            fail("No password for auth method " + authMethod);
        } catch (IllegalArgumentException e){
            // OK
            IoUtils.close(server);
        }
        args = new String[]{"-D", dataDir, "-A", authMethod, "-U", user, "-p", password, "-d", db};
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
        args = new String[]{"-D", dataDir, "-P", port+"", "-d", db};
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
        args = new String[]{"-D", dataDir, "-A", authMethod, "-U", user, "-d", db};
        server = SQLiteServer.create(args);
        try {
            server.initdb(args);
            fail("No password for auth method " + authMethod);
        } catch (IllegalArgumentException e){
            // OK
            IoUtils.close(server);
        }
        args = new String[]{"-D", dataDir, "-A", authMethod, "-U", user, "-p", password, "-d", db};
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
        args = new String[]{"-D", dataDir, "-P", port+""};
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
    
    private void maxAllowedPacketTest(int times) throws SQLException {
        for (int i = 0; i < times; ++i) {
            doMaxAllowedPacketTest();
        }
    }
    
    private void doMaxAllowedPacketTest() throws SQLException {
        final long maxPacket = this.currentEnv.getMaxAllowedPacket();
        
        try (Connection conn = getConnection()) {
            PreparedStatement ps;
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("drop table if exists test_blob");
            stmt.executeUpdate("create table test_blob(id integer primary key, value blob)");
            stmt.close();
            
            ps = conn.prepareStatement("insert into test_blob(value)values(?)");
            for (int size: new int[]{
                    1<<10, 2<<10,  4<<10,  16<< 10, 64<<10, 256<<10, 
                    1<<20, 10<<20, 16<<20, 20<<20 }) {
                try {
                    byte[] blob = new byte[size];
                    ps.setBytes(1, blob);
                    ps.executeUpdate();
                    ps.clearParameters();
                } catch (SQLException e) {
                    final String sqlState = e.getSQLState();
                    if (maxPacket <= 0L || size < maxPacket || (!sqlState.startsWith("08"))) {
                        throw e;
                    }
                }
            }
            ps.close();
        }
    }
    
}
