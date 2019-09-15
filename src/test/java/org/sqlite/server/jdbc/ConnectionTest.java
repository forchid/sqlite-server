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
package org.sqlite.server.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.sqlite.SQLiteErrorCode;
import org.sqlite.server.TestDbBase;
import org.sqlite.util.IoUtils;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public class ConnectionTest extends TestDbBase {
    
    public static void main(String[] args) throws SQLException {
        try {
            new ConnectionTest().test();
        } finally {
            IoUtils.close(server);
        }
    }
    
    public void test() throws SQLException {
        int maxConns = getMaxConns() * getWorkCount();
        
        singleConnTest();
        nestedConnTxTest();
        
        multiConnsTest(maxConns-50, maxConns, false, 1);
        multiConnsTest(maxConns,    maxConns, false, 10);
        multiConnsTest(maxConns*2,  maxConns, false, 5);
        
        sleep(1000L);
        multiConnsTest(maxConns-50, maxConns, true, 2);
        multiConnsTest(maxConns,    maxConns, true, 10);
        multiConnsTest(maxConns*2,  maxConns, true, 5);
    }
    
    private void singleConnTest() throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue(conn != null);
            connectionTest(conn, "select 1", "1");
        }
    }
    
    private void multiConnsTest(int n, int maxConns, boolean multiThread, int iterates) 
            throws SQLException {
        for (int i = 0; i < iterates; ++i) {
            doMultiConnsTest(n, maxConns, multiThread, i);
            if (multiThread) sleep(1000L);
        }
    }
    
    private void nestedConnTxTest() throws SQLException {
        String dataDir = getDataDir(), dbFile = getDbDefault();
        String url = "jdbc:sqlite:"+new File(dataDir, dbFile);
        try (Connection conn = getConnection(url, "", "")) {
            Statement stmt = conn.createStatement();
            ResultSet rs;
            int n, b = 1000000, d = 1000, id = 1;
            String sql;
            
            sql = "create table if not exists accounts(id integer primary key, balance decimal)";
            stmt.executeUpdate(sql);
            sql = String.format("insert into accounts(id, balance)values(%d, %d)", id, b);
            n = stmt.executeUpdate(sql);
            assertTrue(1 == n);
            
            // tx#1
            conn.setAutoCommit(false);
            sql = String.format("update accounts set balance = balance + %d where id = %d", d, id);
            n = stmt.executeUpdate(sql);
            assertTrue(1 == n);
            // check update result
            sql = String.format("select balance from accounts where id = %d", id);
            rs = stmt.executeQuery(sql);
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == b + d);
            rs.close();
            
            try (Connection aConn = getConnection(url, "", "")) {
                assertTrue(aConn != conn);
                Statement aStmt;
                // check the first insertion result
                aStmt = aConn.createStatement();
                sql = String.format("select balance from accounts where id = %d", id);
                rs = aStmt.executeQuery(sql);
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == b);
                rs.close();
                
                // tx#2
                aConn.setAutoCommit(false);
                // check isolation
                sql = String.format("select balance from accounts where id = %d", id);
                rs = aStmt.executeQuery(sql);
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == b);
                // concurrent update
                try {
                    sql = String.format("update accounts set balance = balance + %d where id = %d", d, id);
                    n = aStmt.executeUpdate(sql);
                    assertTrue(1 == n);
                    fail("Concurrent update should be blocked and busy");
                } catch (SQLException e) {
                    if (e.getErrorCode() == SQLiteErrorCode.SQLITE_BUSY.code) {
                        // OK
                        aConn.rollback();
                    } else {
                        throw e;
                    }
                }
            }
            
            // commit and check result
            conn.commit();
            sql = String.format("select balance from accounts where id = %d", id);
            rs = stmt.executeQuery(sql);
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == b + d);
            rs.close();
        }
    }
    
    private void doMultiConnsTest(final int n, final int maxConns, final boolean multiThread, final int at) 
            throws SQLException {
        final AtomicReference<SQLException> exRef = new AtomicReference<>();
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger failedCount = new AtomicInteger(0);
        final CountDownLatch closeLatch = new CountDownLatch(multiThread? n: 1);
        ExecutorService executor = Executors.newFixedThreadPool(multiThread? n: 1);
        
        // test it
        for (int i = 0; i < (multiThread? n: 1); ++i) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    Connection[] conns = null;
                    try {
                        conns = new Connection[multiThread? 1: n];
                        for (int i = 0; i < conns.length; ++i) {
                            try {
                                Connection conn = getConnection();
                                assertTrue(conn != null);
                                connectionTest(conn, "select 1+1;", "2");
                                conns[i] = conn;
                                if (successCount.incrementAndGet() > maxConns) {
                                    exRef.set(new SQLException("Exceeds max connections limit"));
                                    break;
                                }
                            } catch (SQLException e) {
                                if (failedCount.incrementAndGet() > (n - maxConns)) {
                                    exRef.set(e);
                                }
                                
                                if (multiThread) break;
                            }
                        } 
                    } finally {
                        closeLatch.countDown();
                        try {
                            closeLatch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    
                    for (int i = 0; i < conns.length; ++i) {
                        if ((multiThread && conns[i] != null) 
                                || (!multiThread && i < maxConns)) {
                            try {
                                assertTrue(conns[i] != null);
                                conns[i].close();
                            } catch (SQLException e) {
                                exRef.set(e);
                            }
                        } else {
                            assertTrue(conns[i] == null);
                        }
                    }
                }
            });
        }
        
        // wait and check result
        try {
            closeLatch.await();
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                throw new RuntimeException("Connection timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        SQLException e = exRef.get();
        if (e != null) {
            String f = "Test failure at iterator-%s(ok %s, fail %s, ttl %s)";
            throw new AssertionError(String.format(f, at, successCount, failedCount, n), e);
        }
        assertTrue(successCount.get() <= maxConns);
        assertTrue(failedCount.get() + successCount.get() == n);
    }

}
