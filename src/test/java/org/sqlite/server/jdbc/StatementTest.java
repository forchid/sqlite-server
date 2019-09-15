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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.server.TestDbBase;

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
    public void test() throws SQLException {
        alterUserTest();
        createTableTest();
        createUserTest();
        nestedBlockCommentTest();
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

}
