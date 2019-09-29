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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.sqlite.TestDbBase;
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
    
    @Override
    protected void doTest() throws SQLException {
        int maxConns = getMaxConns() * getWorkCount();
        
        singleConnTest();
        
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
