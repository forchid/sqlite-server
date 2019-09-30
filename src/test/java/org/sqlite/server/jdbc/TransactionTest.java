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

import org.sqlite.SQLiteErrorCode;
import org.sqlite.TestDbBase;

/** Transaction test case.
 * 
 * @author little-pan
 * @since 2019-09-26
 *
 */
public class TransactionTest extends TestDbBase {
    
    public static void main(String args[]) throws SQLException {
        new TransactionTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        initTableAccounts();
        nestedConnTxTest();
        readOnlyTxTest();
        
        rrTxTest(100, 10);
        rrTxTest(100, 50);
        rrTxTest(10, 100);
        rrTxTest(100, 100);
        rrTxTest(100, 150);
        
        rwTxTest(100, 10);
        rwTxTest(10, 100);
        rwTxTest(100, 100);
        
        wwTxTest(100, 10);
        wwTxTest(100, 100);
    }
    
    private void nestedConnTxTest() throws SQLException {
        String dataDir = getDataDir(), dbFile = getDbDefault();
        String url = "jdbc:sqlite:"+new File(dataDir, dbFile);
        try (Connection conn = getConnection(url, "", "")) {
            Statement stmt = conn.createStatement();
            ResultSet rs;
            int n, b = 1000000, d = 1000, id = 1;
            String sql;
            sql = String.format("insert into accounts(id, name, balance)values(%d, 'Peter', %d)", id, b);
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
            stmt.execute("delete from accounts");
            conn.commit();
        }
    }
    
    private void readOnlyTxTest() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("begin read only");
            connectionTest(conn, "select 1", "1");
            
            try {
                stmt.executeUpdate("insert into accounts(name, balance)values('Peter', 100000)");
                fail("Can't insert in a read only transaction");
            } catch (SQLException e) {
                if (!"25000".equals(e.getSQLState())) throw e;
            }
            
            try {
                stmt.executeUpdate("update accounts set balance=balance+1000 where id=1");
                fail("Can't update in a read only transaction");
            } catch (SQLException e) {
                if (!"25000".equals(e.getSQLState())) throw e;
            }
            
            try {
                stmt.executeUpdate("DELETE from accounts where id=1");
                fail("Can't delete in a read only transaction");
            } catch (SQLException e) {
                if (!"25000".equals(e.getSQLState())) throw e;
            }
            
            stmt.execute("rollback");
        }
    }
    
    private void rrTxTest(int times, int cons) throws SQLException {
        try (Connection conn = getConnection(true)) {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select count(*) from accounts");
            assertTrue (rs.next());
            assertTrue (rs.getInt(1) == 0);
            int n = s.executeUpdate("insert into accounts(id, name, balance)values(1, 'Ken', 15000)");
            assertTrue(n == 1);
            for (int i = 0; i < times; ++i) {
                Transaction[] readers = new Reader[cons];
                for (int j = 0; j < cons; ++j) {
                    readers[j] = new Reader(this);
                }
                for (int j = 0; j < cons; ++j) {
                    readers[j].start();
                }
                try {
                    for (int j = 0; j < cons; ++j) {
                        Transaction reader = readers[j];
                        reader.join();
                        assertTrue(reader.isOk());
                    }
                } catch (InterruptedException e) {
                    fail("Interrupted");
                }
            }
            s.execute("delete from accounts");
        }
    }
    
    private void rwTxTest(int times, int cons) throws SQLException {
        try (Connection conn = getConnection(true)) {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select count(*) from accounts");
            assertTrue (rs.next());
            assertTrue (rs.getInt(1) == 0);
            int n = s.executeUpdate("insert into accounts(id, name, balance)values(1, 'Ken', 15000)");
            assertTrue(n == 1);
            for (int i = 0; i < times; ++i) {
                Transaction[] txList = new Transaction[cons];
                for (int j = 0; j < cons; ++j) {
                    if (j % 3 == 0) {
                        txList[j] = new Writer(this);
                    } else {
                        txList[j] = new Reader(this);
                    }
                }
                for (int j = 0; j < cons; ++j) {
                    txList[j].start();
                }
                try {
                    for (int j = 0; j < cons; ++j) {
                        Transaction tx = txList[j];
                        tx.join();
                        assertTrue(tx.isOk());
                    }
                } catch (InterruptedException e) {
                    fail("Interrupted");
                }
            }
            s.execute("delete from accounts");
        }
    }
    
    private void wwTxTest(int times, int cons) throws SQLException {
        try (Connection conn = getConnection(true)) {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select count(*) from accounts");
            assertTrue (rs.next());
            assertTrue (rs.getInt(1) == 0);
            int n = s.executeUpdate("insert into accounts(id, name, balance)values(1, 'Ken', 15000)");
            assertTrue(n == 1);
            
            for (int i = 0; i < times; ++i) {
                Transaction[] txList = new Transaction[cons];
                for (int j = 0; j < cons; ++j) {
                    txList[j] = new Writer(this);
                }
                for (int j = 0; j < cons; ++j) {
                    txList[j].start();
                }
                try {
                    for (int j = 0; j < cons; ++j) {
                        Transaction tx = txList[j];
                        tx.join();
                        assertTrue(tx.isOk());
                    }
                } catch (InterruptedException e) {
                    fail("Interrupted");
                }
            }
            s.execute("delete from accounts");
        }
    }
    
    static abstract class Transaction extends Thread {
        
        protected final TransactionTest test;
        protected volatile boolean ok;
        
        Transaction(TransactionTest test) {
            this.test = test;
        }
        
        public boolean isOk() {
            return this.ok;
        }
        
    }
    
    static class Reader extends Transaction {
        
        Reader(TransactionTest test) {
            super(test);
            super.setDaemon(true);
            super.setName("r-tx");
        }
        
        @Override
        public void run() {
            try (Connection c = test.getConnection(true)){
                Statement s = c.createStatement();
                c.setAutoCommit(true);
                s.execute("begin read only");
                ResultSet rs = s.executeQuery("select id, name, balance from accounts where id = 1");
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == 1);
                
                rs = s.executeQuery("select count(*) from accounts");
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == 1);
                rs.close();
                
                s.execute("commit");
                this.ok = true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    static class Writer extends Transaction {
        
        Writer(TransactionTest test) {
            super(test);
            super.setDaemon(true);
            super.setName("w-tx");
        }
        
        @Override
        public void run() {
            try (Connection c = test.getConnection(true)){
                c.setAutoCommit(false);
                Statement s = c.createStatement();
                ResultSet rs;
                
                rs = s.executeQuery("select count(*) from accounts");
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == 1);
                rs.close();
                
                rs = s.executeQuery("select id, name, balance from accounts where id = 1");
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == 1);
                rs.close();
                
                int n = s.executeUpdate("insert into accounts(name, balance)values('Johnson', 200000)");
                assertTrue(n == 1);
                rs = s.executeQuery("select last_insert_rowid()");
                assertTrue(rs.next());
                int id = rs.getInt(1);
                rs.close();
                
                n = s.executeUpdate("update accounts set balance=balance+1000 where id = " + id);
                assertTrue(n == 1);
                
                n = s.executeUpdate("delete from accounts where id = " + id + " and balance = 201000");
                assertTrue(n == 1);
                
                c.commit();
                this.ok = true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
}
