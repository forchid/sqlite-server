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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.sqlite.TestDbBase;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public class StatementTest extends TestDbBase {
    
    public static void main(String args[]) throws SQLException {
        new StatementTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        alterUserTest();
        attachTest();
        createTableTest();
        createUserTest();
        databaseDDLTest();
        dropUserTest();
        
        insertTest();
        
        insertReturningTest(false, true);
        insertReturningTest(true, true);
        insertReturningTest(true, false);
        
        insertReturningTest(1);
        insertReturningTest(10);
        insertReturningTest(100);
        insertReturningTest(150);
        
        nestedBlockCommentTest();
        pragmaTest();
        simpleScalarQueryTest();
    }
    
    private void simpleScalarQueryTest() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            assertTrue(stmt.execute("select 1;"));
            
            ResultSet rs = stmt.executeQuery("select 1;");
            rs.next();
            assertTrue(1 == rs.getInt(1));
            rs.close();
        }
    }
    
    private void createTableTest() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            int n = stmt.executeUpdate("create table if not exists accounts("
                    + "id integer primary key, "
                    + "name varchar(50) not null,"
                    + "balance decimal(12,1) not null)");
            assertTrue(0 == n);
            stmt.close();
        }
    }
    
    private void attachTest() throws SQLException {
        doAttachTest(null);
        doAttachTest(getExtraDir());
    }
    
    private void doAttachTest(String dataDir) throws SQLException {
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            int n;
            try {
                s.executeUpdate("attach database 'sqlite3.meta' as m");
                fail("Can't attach meta db");
            } catch (SQLException e) {
                // OK
            }
            
            // init
            n = s.executeUpdate("create user test@localhost identified by '123'");
            assertTrue(1 == n);
            n = s.executeUpdate("grant select, insert, create on '"+getDbDefault()+"' to test@localhost");
            assertTrue(1 == n);
            s.executeUpdate("drop database if exists 'attach.db'");
            if (dataDir == null) {
                n = s.executeUpdate("create database 'attach.db'");
            } else {
                n = s.executeUpdate("create database 'attach.db' location '" + dataDir +"'");
            }
            assertTrue(1 == n);
            n = s.executeUpdate("grant attach on 'attach.db' to test@localhost");
            assertTrue(1 == n);
            // do test
            try (Connection c = getConnection("test", "123")) {
                Statement sa = c.createStatement();
                connectionTest(c, "select 1", "1");
                if (dataDir == null) {
                    n = sa.executeUpdate("attach database 'attach.db' as a");
                } else {
                    n = sa.executeUpdate("attach database '"+dataDir+File.separator+"attach.db' as a");
                }
                assertTrue(0 == n);
                sa.executeUpdate("create table a.test(id int primary key, name varchar(20))");
                n = sa.executeUpdate("insert into a.test(name)values('ABC')");
                assertTrue(1 == n);
                ResultSet rs = sa.executeQuery("select count(*) from a.test");
                assertTrue(rs.next());
                assertTrue(1 == rs.getInt(1));
                rs = sa.executeQuery("select name from a.test where name='ABC'");
                assertTrue(rs.next());
                assertTrue("ABC".equals(rs.getString(1)));
                n = sa.executeUpdate("detach database a");
                assertTrue(1 == n);
            }
            // cleanup
            n = s.executeUpdate("revoke attach on 'attach.db' from test@localhost");
            assertTrue(1 == n);
            n = s.executeUpdate("drop database 'attach.db'");
            assertTrue(1 == n);
            n = s.executeUpdate("revoke all on '"+getDbDefault()+"' from test@localhost");
            assertTrue(1 == n);
            n = s.executeUpdate("drop user test@localhost");
        }
    }
    
    private void alterUserTest() throws SQLException {
        doAlterUserTest(true, false, false);
        try {
            try (Connection c = getConnection("test02", "0123")) {}
            fail("tx hasn't been committed");
        } catch (SQLException e) {
            // OK
        }
        
        doAlterUserTest(true, true, false);
        try (Connection c = getConnection("test02", "0123")) {}
        
        doAlterUserTest(false, false, true);
        try (Connection c = getConnection("test02", "0123")) {}
        
        // test outer statement of transaction
        try (Connection conn = getConnection()) {
            int n;
            conn.setAutoCommit(false);
            // Simple statement
            Statement outOfTxStmt = conn.createStatement();
            n = outOfTxStmt.executeUpdate("create user 'test-1'@localhost identified by '123'");
            assertTrue(1 == n);
            conn.commit();
            conn.setAutoCommit(true);
            n = outOfTxStmt.executeUpdate("alter user 'test-1'@localhost identified by '0123'");
            assertTrue(1 == n);
            
            // Prepared statement
            conn.setAutoCommit(false);
            PreparedStatement ps = 
                    conn.prepareStatement("alter user 'test-1'@localhost identified by '1123'");
            n = ps.executeUpdate();
            assertTrue(1 == n);
            conn.commit();
            n = ps.executeUpdate();
            assertTrue(1 == n);
            conn.setAutoCommit(true);
            n = ps.executeUpdate();
            assertTrue(1 == n);
        }
    }
    
    private void databaseDDLTest() throws SQLException {
        try (Connection conn = getConnection("test")) {
            fail("'test' database not exists");
        } catch (SQLException e) {
            if (!"08001".equals(e.getSQLState())) {
                throw e;
            }
        }
        
        // test-1: Database in default data directory
        // create database test
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            int n = s.executeUpdate("drop schema if exists test");
            assertTrue(0 == n);
            n =s.executeUpdate("create database test");
            assertTrue(1 == n);
            n =s.executeUpdate("create database if not exists test");
            assertTrue(0 == n);
        }
        // connect to the new database test and try to drop
        try (Connection conn = getConnection("test")) {
            Statement s = conn.createStatement();
            try {
                s.executeUpdate("drop database test");
                fail("Database 'test' in use");
            } catch (SQLException e) {
                // OK
            }
        }
        // drop database test
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            int n = s.executeUpdate("drop schema test");
            assertTrue(1 == n);
            n = s.executeUpdate("drop database if exists test");
            assertTrue(0 == n);
        }
        
        // test-2: Database in dbDDLTest data directory
        // create database test
        String dataDir = getExtraDir();
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            int n = s.executeUpdate("drop schema if exists test");
            assertTrue(0 == n);
            n =s.executeUpdate("create database test location '" +dataDir+"'");
            assertTrue(1 == n);
            n =s.executeUpdate("create database if not exists test directory '" +dataDir+"'");
            assertTrue(0 == n);
        }
        // connect to the new database test and try to drop
        try (Connection conn = getConnection("test")) {
            Statement s = conn.createStatement();
            try {
                s.executeUpdate("drop database test");
                fail("Database 'test' in use");
            } catch (SQLException e) {
                // OK
            }
        }
        // drop database test
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            int n = s.executeUpdate("drop schema test");
            assertTrue(1 == n);
            n = s.executeUpdate("drop database if exists test");
            assertTrue(0 == n);
        }
        
        // test-3: try to create a database named with meta db's name
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            s.executeUpdate("create database 'sqlite3.meta'");
            fail("Can't create a database named by meta db's name");
        } catch (SQLException e) {
            if (!"42000".equals(e.getSQLState())) {
                throw e;
            }
        }
        try (Connection conn = getConnection()) {
            Statement s = conn.createStatement();
            s.executeUpdate("create database 'SQLite3.META'");
            fail("Can't create a database named by meta db's name");
        } catch (SQLException e) {
            if (!"42000".equals(e.getSQLState())) {
                throw e;
            }
        }
    }
    
    private void dropUserTest() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            int n;
            
            n = stmt.executeUpdate("create user test10@localhost identified by '123'");
            assertTrue(1 == n);
            n = stmt.executeUpdate("drop user test10@localhost;");
            assertTrue(1 == n);
            
            n = stmt.executeUpdate("create user test11@localhost identified by '123' superuser");
            assertTrue(1 == n);
            try (Connection c = getConnection("test11", "123")){}
            n = stmt.executeUpdate("drop user test11@localhost;");
            assertTrue(1 == n);
            try (Connection c = getConnection("test11", "123")){
                fail("The user 'test11' has been dropped");
            } catch (SQLException e) {
                if (!"28000".equals(e.getSQLState())) throw e;
            }
        }
    }
    
    private void doAlterUserTest(boolean useTx, boolean commitTx, boolean created) throws SQLException {
        try (Connection conn = getConnection()) {
            if (useTx) conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            int n;
            try {
                n = stmt.executeUpdate("create user test0@localhost identified by '123'");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            n = stmt.executeUpdate("alter user test0@localhost identified by '0123'");
            assertTrue(1 == n);
            try {
                try (Connection c = getConnection("test0", "0123")) {
                    fail("User test@localhost hasn't been granted or tx not committed");
                }
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test01@'127.0.0.1' identified by '123'");
                assertTrue(1 == n);
                n = stmt.executeUpdate("alter user test01@'127.0.0.1' identified by '0123'");
                assertTrue(1 == n);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            
            try {
                n = stmt.executeUpdate("create user test02@'localhost' identified by '123' nosuperuser");
                assertTrue(1 == n);
                n = stmt.executeUpdate("alter user test02@'localhost' identified by '0123' superuser");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            
            try {
                try (Connection c = getConnection("test02", "0123")) {}
                if (useTx) fail("tx hasn't been committed");
            } catch (SQLException e) {
                // OK
                if (!useTx) throw e;
            }
            try {
                n = stmt.executeUpdate("create user test03@'localhost' identified by '0123' nosuperuser");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                try (Connection c = getConnection("test03", "0123")) {
                    fail("User test03@'localhost' hasn't been granted");
                }
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test04@'localhost' identified by '123' nosuperuser "
                        + "identified with pg password");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                try (Connection c = getConnection("test04", "0123")) {
                    fail("User test04@'localhost' hasn't been granted");
                }
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test05@'localhost' identified by '123' nosuperuser");
                assertTrue(1 == n);
                n = stmt.executeUpdate("alter user test05@'localhost' identified by '0123' superuser "
                        + "identified with pg trust");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                try (Connection c = getConnection("test05", "")) {
                    if (useTx) fail("User test5@'localhost' hasn't been granted");
                }
            } catch (SQLException e) {
                // OK
                if (!useTx) throw e;
            }
            
            if (useTx && commitTx) {
                conn.commit();
            }
        }
    }
    
    private void createUserTest() throws SQLException {
        doCreateUserTest(true, false, false);
        try {
            try (Connection c = getConnection("test2", "123")) {}
            fail("tx hasn't been committed");
        } catch (SQLException e) {
            // OK
        }
        
        doCreateUserTest(true, true, false);
        try (Connection c = getConnection("test2", "123")) {}
        
        doCreateUserTest(false, false, true);
        try (Connection c = getConnection("test2", "123")) {}
    }
    
    private void doCreateUserTest(boolean useTx, boolean commitTx, boolean created) throws SQLException {
        try (Connection conn = getConnection()) {
            if (useTx) conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            int n;
            try {
                n = stmt.executeUpdate("create user test@localhost identified by '123'");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                n = stmt.executeUpdate("create user test@localhost identified by '123'");
                assertTrue(1 == n);
                fail("User test@localhost existing yet");
            } catch (SQLException e) {
                // OK
            }
            try {
                getConnection("test", "123");
                fail("User test@localhost hasn't been granted");
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test1@localhost identified by '123'");
                assertTrue(1 == n);
                n = stmt.executeUpdate("create user test1@'127.0.0.1' identified by '123'");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            
            try {
                n = stmt.executeUpdate("create user test2@'localhost' identified by '123' superuser");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            
            try {
                try (Connection c = getConnection("test2", "123")) {}
                if (useTx) fail("tx hasn't been committed");
            } catch (SQLException e) {
                // OK
                if (!useTx) throw e;
            }
            try {
                n = stmt.executeUpdate("create user test3@'localhost' identified by '123' nosuperuser");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                getConnection("test3", "123");
                fail("User test3@'localhost' hasn't been granted");
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test4@'localhost' identified by '123' nosuperuser "
                        + "identified with pg password");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                getConnection("test4", "123");
                fail("User test4@'localhost' hasn't been granted");
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test5@'localhost' identified by '123' nosuperuser "
                        + "identified with pg trust");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                getConnection("test5", "123");
                fail("User test5@'localhost' hasn't been granted");
            } catch (SQLException e) {
                // OK
            }
            
            try {
                n = stmt.executeUpdate("create user test6@'localhost' identified by '123' superuser "
                        + "identified with pg trust");
                assertTrue(1 == n);
                assertTrue(!created);
            } catch (SQLException e) {
                if (!created) throw e;
            }
            try {
                try (Connection c = getConnection("test6", "")){};
                if (useTx) fail("tx not commited: User test6@'localhost'");
            } catch (SQLException e) {
                if (!useTx || created) throw e;
            }
            
            if (useTx && commitTx) {
                conn.commit();
            }
        }
    }
    
    private void nestedBlockCommentTest() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            
            assertFalse(stmt.execute("/*/**/*/"));
            assertFalse(stmt.execute("/*b/**/*/"));
            assertFalse(stmt.execute("/*b/*b*/*/"));
            assertFalse(stmt.execute("/*b/*b*/b*/"));
            assertFalse(stmt.execute("/*select 1;/*select 2;*/select 3*/"));
            assertFalse(stmt.execute("/*select 1;/*select 2;*/select 3;*/"));
            assertFalse(stmt.execute("/*select 1;/*select 2;*/select 3;*/ "));
            assertFalse(stmt.execute("/*select 1;/*select 2;*/select 3;*/ -- c"));
            
            ResultSet rs;
            assertTrue(stmt.execute("/*select 1;/*select 2;*/outmost block;*/ select 1"));
            rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertTrue(1 == rs.getInt(1));
            
            assertTrue(stmt.execute("/*select 1;/*select 2;*/select 3;*/ select 1"));
            rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertTrue(1 == rs.getInt(1));
            
            assertTrue(stmt.execute("/*select 1;/*/*select 4;*/select 2;*/ /*select 5;*/select 3;*/ select 1"));
            rs = stmt.getResultSet();
            assertTrue(rs.next());
            assertTrue(1 == rs.getInt(1));
            
            stmt.close();
        }
    }
    
    private void pragmaTest() throws SQLException {
        doPragmaTest("pragma user_version;", true, "0");
        doPragmaTest("pragma user_version = 1;", false, null);
    }
    
    private void doPragmaTest(String sql, boolean resultSet, String result) throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue((resultSet && result != null) || (!resultSet && result == null));
            Statement stmt = conn.createStatement();
            boolean res = stmt.execute(sql);
            assertTrue(res == resultSet);
            if (res) {
                ResultSet rs = stmt.getResultSet();
                assertTrue(rs.next());
                assertTrue(rs.getString(1).equals(result));
            } else {
                assertTrue(result == null && stmt.getUpdateCount() == 0);
            }
            
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSetMetaData meta = ps.getMetaData();
            if (result == null) {
                assertTrue(meta == null);
            } else {
                assertTrue(meta != null);
            }
        }
    }
    
    private void insertTest() throws SQLException {
        try (Connection conn = getConnection()) {
            initTableAccounts(conn);
            Statement s = conn.createStatement();
            ResultSet rs;
            int n, id = 0;
            
            conn.setAutoCommit(false);
            n = s.executeUpdate("insert into accounts(name, balance)values('Kite', 20000)");
            assertTrue(1 == n);
            rs = s.executeQuery("select last_insert_rowid()");
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == ++id);
            rs.close();
            
            n = s.executeUpdate("insert into accounts(name, balance)values('Tom', 25000), ('John son', 22000)");
            assertTrue(2 == n);
            id += 2;
            rs = s.executeQuery("select last_insert_rowid()");
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == id);
            assertTrue(!rs.next());
            rs.close();
            
            conn.rollback();
        }
    }
    
    private void insertReturningTest(boolean tx, boolean commit) throws SQLException {
        try (Connection conn = getConnection()) {
            initTableAccounts(conn);
            doInsertReturningTest(conn, tx, commit);
        }
    }
    
    private void doInsertReturningTest(Connection conn, boolean tx, boolean commit) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs;
        int n, id;
        boolean resultSet;
        
        conn.setAutoCommit(!tx);
        
        try (ResultSet r = s.executeQuery("select count(*) from accounts")) {
            assertTrue(r.next());
            assertTrue(r.getInt(1) == 0);
            assertTrue(!r.next());
        }
        
        resultSet = s.execute("insert into accounts(name, balance)values('James', 21000)", 
                Statement.RETURN_GENERATED_KEYS);
        assertTrue(!resultSet);
        n = s.getUpdateCount();
        assertTrue(1 == n);
        rs = s.getGeneratedKeys();
        assertTrue(rs.next());
        id = rs.getInt(1);
        assertTrue(id == 1);
        assertTrue(!rs.next());
        rs.close();
        
        resultSet = s.execute("insert into accounts(id, name, balance) select null, 'James', 21000", 
                Statement.RETURN_GENERATED_KEYS);
        assertTrue(!resultSet);
        n = s.getUpdateCount();
        assertTrue(1 == n);
        rs = s.getGeneratedKeys();
        assertTrue(rs.next());
        id = rs.getInt(1);
        assertTrue(id == 2);
        assertTrue(!rs.next());
        rs.close();
        
        resultSet = s.execute("insert into accounts(name, balance) values('Tom', 25000), ('John son', 22000)", 
                Statement.RETURN_GENERATED_KEYS);
        assertTrue(!resultSet);
        n = s.getUpdateCount();
        assertTrue(2 == n);
        rs = s.getGeneratedKeys();
        assertTrue(rs.next());
        id = rs.getInt(1);
        assertTrue(id == 3);
        assertTrue(rs.next());
        id = rs.getInt(1);
        assertTrue(id == 4);
        assertTrue(!rs.next());
        rs.close();
        
        resultSet = s.execute("insert into accounts(id, name, balance) select 5, 'James', 21000", 
                Statement.RETURN_GENERATED_KEYS);
        assertTrue(!resultSet);
        n = s.getUpdateCount();
        assertTrue(1 == n);
        rs = s.getGeneratedKeys();
        assertTrue(rs.next());
        id = rs.getInt(1);
        assertTrue(id == 5);
        assertTrue(!rs.next());
        rs.close();
        
        if (tx) {
            if (commit) {
                conn.commit();
                conn.setAutoCommit(true);
                try (ResultSet r = s.executeQuery("select count(*) from accounts")) {
                    assertTrue(r.next());
                    assertTrue(r.getInt(1) == id);
                    assertTrue(!r.next());
                }
            } else {
                conn.rollback();
                conn.setAutoCommit(true);
                try (ResultSet r = s.executeQuery("select count(*) from accounts")) {
                    assertTrue(r.next());
                    assertTrue(r.getInt(1) == 0);
                    assertTrue(!r.next());
                }
            }
        } else {
            conn.setAutoCommit(true);
            try (ResultSet r = s.executeQuery("select count(*) from accounts")) {
                assertTrue(r.next());
                assertTrue(r.getInt(1) == id);
                assertTrue(!r.next());
            }
        }
    }

    private void insertReturningTest(final int cons) throws SQLException {
        Thread [] tests = new Thread[cons];
        final AtomicInteger successes = new AtomicInteger();
        try (Connection conn = getConnection(true)) {
            initTableAccounts(conn);
        }
        for (int i = 0; i < cons; ++i) {
            tests [i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try (Connection conn = getConnection(true)) {
                        doInsertReturningTest(conn, true, false);
                        successes.incrementAndGet();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, "test-" + i);
            tests [i].start();
        }
        
        try {
            for (int i = 0; i < cons; ++i) {
                tests [i].join(3000L);
            }
            assertTrue(successes.get() == cons);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
}
