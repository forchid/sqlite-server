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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.server.TestDbBase;

/**
 * @author little-pan
 * @since 2019-08-31
 *
 */
public class TestStatement extends TestDbBase {
    
    public static void main(String args[]) throws SQLException {
        new TestStatement().test();
    }

    @Override
    public void test() throws SQLException {
        simpleScalarQueryTest();
        createTableTest();
        nestedBlockCommentTest();
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
