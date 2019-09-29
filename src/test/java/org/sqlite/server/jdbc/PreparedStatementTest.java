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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.sqlite.TestDbBase;

/** JDBC prepared statement test cases
 * 
 * @author little-pan
 * @since 2019-09-29
 *
 */
public class PreparedStatementTest extends TestDbBase {
    
    public static void main(String args[]) throws SQLException {
        new PreparedStatementTest().test();
    }

    @Override
    protected void doTest() throws SQLException {
        batchTest(false);
        batchTest(true);
    }
    
    private void batchTest(boolean tx) throws SQLException {
        //try (Connection conn = getConnection(5432, "postgres", "postgres", "123456")) {
        try (Connection conn = getConnection()) {
            PreparedStatement ps;
            Statement s;
            ResultSet rs;
            String sql;
            int i = 0;
            
            initTableAccounts(conn);
            conn.setAutoCommit(!tx);
            
            // Simple batch test
            sql = "insert into accounts(name, balance)values(?, ?)";
            ps = conn.prepareStatement(sql);
            ps.setString(++i, "Tom");
            ps.setBigDecimal(++i, new BigDecimal(30000));
            ps.addBatch();
            i = 0;
            ps.setString(++i, "Ben");
            ps.setBigDecimal(++i, new BigDecimal(25000));
            ps.addBatch();
            ps.executeBatch();
            // check
            s = conn.createStatement();
            rs = s.executeQuery("select count(*) from accounts");
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == 2);
            assertTrue(!rs.next());
            rs.close();
            rs = s.executeQuery("select max(id) from accounts");
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) == 2);
            assertTrue(!rs.next());
            rs.close();
            
            // Test batch atomic
            sql = "insert into accounts(id, name, balance)values(?, ?, ?)";
            ps = conn.prepareStatement(sql);
            i = 0;
            ps.setNull(++i, Types.BIGINT);
            ps.setString(++i, "James");
            ps.setBigDecimal(++i, new BigDecimal(50000));
            ps.addBatch();
            i = 0;
            ps.setLong(++i, 3);
            ps.setString(++i, "Kite");
            ps.setBigDecimal(++i, new BigDecimal(15000));
            ps.addBatch();
            try {
                ps.executeBatch();
                fail("Duplicate primary key 3");
            } catch (SQLException e) {
                // check: one batch should be atomic
                rs = s.executeQuery("select count(*) from accounts");
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == (tx? 3: 2));
                assertTrue(!rs.next());
                rs.close();
                rs = s.executeQuery("select max(id) from accounts");
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) == (tx? 3: 2));
                assertTrue(!rs.next());
                rs.close();
                if (tx) {
                    conn.rollback();
                }
            }
            
            s.close();
            ps.close();
        }
    }

}
